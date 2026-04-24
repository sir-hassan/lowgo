package kv

import (
	"bytes"
	"sync"

	"github.com/sir-hassan/lowgo/pkg/blockfs"
)

type BPlusTreeStore struct {
	file                 blockfs.File
	blockSize            int64
	blockBytes           int
	chunkPayloadCapacity int
	maxLeafEntries       int
	maxInternalEntries   int

	mu    sync.RWMutex
	super bplusSuperblock
}

type bplusSplit struct {
	keyLen     uint32
	keyRef     int64
	rightBlock int64
}

func OpenBPT(path string, opts Options) (*BPlusTreeStore, error) {
	opts, err := opts.normalized()
	if err != nil {
		return nil, err
	}

	file, err := blockfs.Open(path, blockfs.Options{
		BlockSize: opts.BlockSize,
		Perm:      opts.Perm,
	})
	if err != nil {
		return nil, err
	}

	store := &BPlusTreeStore{
		file:      file,
		blockSize: file.Size(),
	}
	store.blockBytes = int(store.blockSize)
	if store.blockSize <= chunkHeaderSize {
		_ = file.Close()
		return nil, ErrBlockSizeTooSmall
	}
	store.chunkPayloadCapacity = store.blockBytes - chunkHeaderSize
	store.maxLeafEntries = (store.blockBytes - bplusNodeHeaderSize) / bplusLeafEntrySize
	store.maxInternalEntries = (store.blockBytes - bplusNodeHeaderSize) / bplusInternalEntrySize
	if store.maxLeafEntries < 2 || store.maxInternalEntries < 2 {
		_ = file.Close()
		return nil, ErrBlockSizeTooSmall
	}

	if err := store.open(); err != nil {
		_ = file.Close()
		return nil, err
	}

	return store, nil
}

func (s *BPlusTreeStore) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, found, err := s.lookupLeafEntryLocked(s.super.rootBlock, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}

	return s.loadBlobLocked(entry.valueRef, entry.valueLen)
}

func (s *BPlusTreeStore) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	split, err := s.insertLocked(s.super.rootBlock, key, value)
	if err != nil {
		return err
	}
	if split == nil {
		return nil
	}

	newRootBlock, err := s.allocateBlockLocked()
	if err != nil {
		return err
	}
	root := bplusNode{
		kind:       bplusNodeTypeInternal,
		firstChild: s.super.rootBlock,
		nextLeaf:   -1,
		internalEntries: []bplusInternalEntry{{
			keyLen: split.keyLen,
			keyRef: split.keyRef,
			child:  split.rightBlock,
		}},
	}
	if err := s.writeNodeLocked(newRootBlock, root); err != nil {
		return err
	}
	s.super.rootBlock = newRootBlock

	return s.writeSuperblockLocked()
}

func (s *BPlusTreeStore) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.deleteLocked(s.super.rootBlock, key)

	return err
}

func (s *BPlusTreeStore) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrEmptyKey
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, found, err := s.lookupLeafEntryLocked(s.super.rootBlock, key)
	if err != nil {
		return false, err
	}

	return found, nil
}

func (s *BPlusTreeStore) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.file.Sync()
}

func (s *BPlusTreeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.file.Close()
}

func (s *BPlusTreeStore) open() error {
	buf, err := s.readBlockLocked(0)
	if err != nil {
		return err
	}

	if isZeroBlock(buf) {
		s.super = bplusSuperblock{
			rootBlock: 1,
			nextBlock: 2,
		}
		if err := s.writeSuperblockLocked(); err != nil {
			return err
		}

		return s.writeNodeLocked(s.super.rootBlock, bplusNode{
			kind:       bplusNodeTypeLeaf,
			firstChild: -1,
			nextLeaf:   -1,
		})
	}

	meta, err := decodeBPlusSuperblock(buf)
	if err != nil {
		return err
	}
	s.super = meta

	_, err = s.readNodeLocked(s.super.rootBlock)

	return err
}

func (s *BPlusTreeStore) lookupLeafEntryLocked(block int64, key []byte) (bplusLeafEntry, bool, error) {
	node, err := s.readNodeLocked(block)
	if err != nil {
		return bplusLeafEntry{}, false, err
	}

	if node.kind == bplusNodeTypeLeaf {
		index, found, err := s.findLeafEntryIndexLocked(node, key)
		if err != nil {
			return bplusLeafEntry{}, false, err
		}
		if !found {
			return bplusLeafEntry{}, false, nil
		}

		return node.leafEntries[index], true, nil
	}

	childIndex, err := s.findChildIndexLocked(node, key)
	if err != nil {
		return bplusLeafEntry{}, false, err
	}

	return s.lookupLeafEntryLocked(node.childAt(childIndex), key)
}

func (s *BPlusTreeStore) insertLocked(block int64, key []byte, value []byte) (*bplusSplit, error) {
	node, err := s.readNodeLocked(block)
	if err != nil {
		return nil, err
	}

	if node.kind == bplusNodeTypeLeaf {
		return s.insertIntoLeafLocked(block, node, key, value)
	}

	childIndex, err := s.findChildIndexLocked(node, key)
	if err != nil {
		return nil, err
	}
	split, err := s.insertLocked(node.childAt(childIndex), key, value)
	if err != nil || split == nil {
		return split, err
	}

	node.internalEntries = insertInternalEntry(node.internalEntries, childIndex, bplusInternalEntry{
		keyLen: split.keyLen,
		keyRef: split.keyRef,
		child:  split.rightBlock,
	})
	if len(node.internalEntries) <= s.maxInternalEntries {
		return nil, s.writeNodeLocked(block, node)
	}

	return s.splitInternalNodeLocked(block, node)
}

func (s *BPlusTreeStore) insertIntoLeafLocked(block int64, node bplusNode, key []byte, value []byte) (*bplusSplit, error) {
	index, found, err := s.findLeafEntryIndexLocked(node, key)
	if err != nil {
		return nil, err
	}

	valueRef, err := s.storeBlobLocked(value)
	if err != nil {
		return nil, err
	}

	if found {
		node.leafEntries[index].valueLen = uint64(len(value))
		node.leafEntries[index].valueRef = valueRef

		return nil, s.writeNodeLocked(block, node)
	}

	keyRef, err := s.storeBlobLocked(key)
	if err != nil {
		return nil, err
	}
	node.leafEntries = insertLeafEntry(node.leafEntries, index, bplusLeafEntry{
		keyLen:   uint32(len(key)),
		keyRef:   keyRef,
		valueLen: uint64(len(value)),
		valueRef: valueRef,
	})
	if len(node.leafEntries) <= s.maxLeafEntries {
		return nil, s.writeNodeLocked(block, node)
	}

	return s.splitLeafNodeLocked(block, node)
}

func (s *BPlusTreeStore) splitLeafNodeLocked(block int64, node bplusNode) (*bplusSplit, error) {
	mid := len(node.leafEntries) / 2
	rightEntries := append([]bplusLeafEntry(nil), node.leafEntries[mid:]...)
	node.leafEntries = append([]bplusLeafEntry(nil), node.leafEntries[:mid]...)

	rightBlock, err := s.allocateBlockLocked()
	if err != nil {
		return nil, err
	}
	right := bplusNode{
		kind:        bplusNodeTypeLeaf,
		firstChild:  -1,
		nextLeaf:    node.nextLeaf,
		leafEntries: rightEntries,
	}
	node.nextLeaf = rightBlock

	if err := s.writeNodeLocked(block, node); err != nil {
		return nil, err
	}
	if err := s.writeNodeLocked(rightBlock, right); err != nil {
		return nil, err
	}

	return &bplusSplit{
		keyLen:     rightEntries[0].keyLen,
		keyRef:     rightEntries[0].keyRef,
		rightBlock: rightBlock,
	}, nil
}

func (s *BPlusTreeStore) splitInternalNodeLocked(block int64, node bplusNode) (*bplusSplit, error) {
	mid := len(node.internalEntries) / 2
	promoted := node.internalEntries[mid]

	rightBlock, err := s.allocateBlockLocked()
	if err != nil {
		return nil, err
	}
	right := bplusNode{
		kind:            bplusNodeTypeInternal,
		firstChild:      promoted.child,
		nextLeaf:        -1,
		internalEntries: append([]bplusInternalEntry(nil), node.internalEntries[mid+1:]...),
	}
	node.internalEntries = append([]bplusInternalEntry(nil), node.internalEntries[:mid]...)

	if err := s.writeNodeLocked(block, node); err != nil {
		return nil, err
	}
	if err := s.writeNodeLocked(rightBlock, right); err != nil {
		return nil, err
	}

	return &bplusSplit{
		keyLen:     promoted.keyLen,
		keyRef:     promoted.keyRef,
		rightBlock: rightBlock,
	}, nil
}

func (s *BPlusTreeStore) deleteLocked(block int64, key []byte) (bool, error) {
	node, err := s.readNodeLocked(block)
	if err != nil {
		return false, err
	}

	if node.kind == bplusNodeTypeLeaf {
		index, found, err := s.findLeafEntryIndexLocked(node, key)
		if err != nil || !found {
			return false, err
		}
		node.leafEntries = append(node.leafEntries[:index], node.leafEntries[index+1:]...)

		return true, s.writeNodeLocked(block, node)
	}

	childIndex, err := s.findChildIndexLocked(node, key)
	if err != nil {
		return false, err
	}

	return s.deleteLocked(node.childAt(childIndex), key)
}

func (s *BPlusTreeStore) findLeafEntryIndexLocked(node bplusNode, key []byte) (int, bool, error) {
	for i, entry := range node.leafEntries {
		cmp, err := s.compareKeyToBlobLocked(key, entry.keyRef, entry.keyLen)
		if err != nil {
			return 0, false, err
		}
		if cmp == 0 {
			return i, true, nil
		}
		if cmp < 0 {
			return i, false, nil
		}
	}

	return len(node.leafEntries), false, nil
}

func (s *BPlusTreeStore) findChildIndexLocked(node bplusNode, key []byte) (int, error) {
	for i, entry := range node.internalEntries {
		cmp, err := s.compareKeyToBlobLocked(key, entry.keyRef, entry.keyLen)
		if err != nil {
			return 0, err
		}
		if cmp < 0 {
			return i, nil
		}
	}

	return len(node.internalEntries), nil
}

func (s *BPlusTreeStore) compareKeyToBlobLocked(key []byte, ref int64, keyLen uint32) (int, error) {
	other, err := s.loadBlobLocked(ref, uint64(keyLen))
	if err != nil {
		return 0, err
	}

	return bytes.Compare(key, other), nil
}

func (s *BPlusTreeStore) storeBlobLocked(data []byte) (int64, error) {
	if len(data) == 0 {
		return -1, nil
	}

	chunkCount := ceilDiv(int64(len(data)), int64(s.chunkPayloadCapacity))
	start, err := s.allocateBlocksLocked(chunkCount)
	if err != nil {
		return 0, err
	}

	offset := 0
	for i := range chunkCount {
		block := make([]byte, s.blockBytes)
		used := minInt(len(data)-offset, s.chunkPayloadCapacity)
		next := int64(-1)
		if i+1 < chunkCount {
			next = start + i + 1
		}
		if err := encodeChunk(chunk{
			next: next,
			used: uint32(used),
		}, block); err != nil {
			return 0, err
		}
		copy(block[chunkHeaderSize:chunkHeaderSize+used], data[offset:offset+used])
		offset += used
		if err := s.file.Write(start+i, block); err != nil {
			return 0, err
		}
	}

	return start, nil
}

func (s *BPlusTreeStore) loadBlobLocked(ref int64, length uint64) ([]byte, error) {
	if length == 0 {
		if ref >= 0 {
			return nil, ErrCorrupt
		}

		return []byte{}, nil
	}
	if ref < 0 {
		return nil, ErrCorrupt
	}

	if length > uint64(^uint(0)>>1) {
		return nil, ErrCorrupt
	}
	data := make([]byte, int(length))
	offset := 0
	for ref >= 0 {
		block, err := s.readBlockLocked(ref)
		if err != nil {
			return nil, err
		}
		ch, err := decodeChunk(block)
		if err != nil {
			return nil, err
		}
		if int(ch.used) > s.chunkPayloadCapacity {
			return nil, ErrCorrupt
		}
		if offset+int(ch.used) > len(data) {
			return nil, ErrCorrupt
		}

		copy(data[offset:offset+int(ch.used)], block[chunkHeaderSize:chunkHeaderSize+int(ch.used)])
		offset += int(ch.used)
		ref = ch.next
	}
	if offset != len(data) {
		return nil, ErrCorrupt
	}

	return data, nil
}

func (s *BPlusTreeStore) allocateBlockLocked() (int64, error) {
	return s.allocateBlocksLocked(1)
}

func (s *BPlusTreeStore) allocateBlocksLocked(count int64) (int64, error) {
	if count <= 0 || s.super.nextBlock > (1<<63-1)-count {
		return 0, ErrCorrupt
	}

	start := s.super.nextBlock
	s.super.nextBlock += count
	if err := s.writeSuperblockLocked(); err != nil {
		s.super.nextBlock = start
		return 0, err
	}

	return start, nil
}

func (s *BPlusTreeStore) readNodeLocked(block int64) (bplusNode, error) {
	raw, err := s.readBlockLocked(block)
	if err != nil {
		return bplusNode{}, err
	}

	return decodeBPlusNode(raw)
}

func (s *BPlusTreeStore) writeNodeLocked(block int64, node bplusNode) error {
	raw := make([]byte, s.blockBytes)
	if err := encodeBPlusNode(node, raw); err != nil {
		return err
	}

	return s.file.Write(block, raw)
}

func (s *BPlusTreeStore) writeSuperblockLocked() error {
	raw := make([]byte, s.blockBytes)
	if err := encodeBPlusSuperblock(s.super, raw); err != nil {
		return err
	}

	return s.file.Write(0, raw)
}

func (s *BPlusTreeStore) readBlockLocked(index int64) ([]byte, error) {
	block := make([]byte, s.blockBytes)
	if err := s.file.Read(index, block); err != nil {
		return nil, err
	}

	return block, nil
}

func (n bplusNode) childAt(index int) int64 {
	if index == 0 {
		return n.firstChild
	}

	return n.internalEntries[index-1].child
}

func insertLeafEntry(entries []bplusLeafEntry, index int, entry bplusLeafEntry) []bplusLeafEntry {
	entries = append(entries, bplusLeafEntry{})
	copy(entries[index+1:], entries[index:])
	entries[index] = entry

	return entries
}

func insertInternalEntry(entries []bplusInternalEntry, index int, entry bplusInternalEntry) []bplusInternalEntry {
	entries = append(entries, bplusInternalEntry{})
	copy(entries[index+1:], entries[index:])
	entries[index] = entry

	return entries
}
