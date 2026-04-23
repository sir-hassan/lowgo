package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/sir-hassan/lowgo/pkg/blockfs"
)

var kvChecksumTable = crc32.MakeTable(crc32.Castagnoli)

type LinkedListStore struct {
	file                  blockfs.File
	blockSize             int64
	blockBytes            int
	chunkPayloadCapacity  int
	bucketEntriesPerBlock int64

	mu    sync.RWMutex
	super superblock
}

func OpenLinkedList(path string, opts Options) (*LinkedListStore, error) {
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

	store := &LinkedListStore{
		file:      file,
		blockSize: file.Size(),
	}
	store.blockBytes = int(store.blockSize)
	if store.blockSize < minBlockSize || store.blockSize <= chunkHeaderSize {
		_ = file.Close()
		return nil, ErrBlockSizeTooSmall
	}
	store.chunkPayloadCapacity = store.blockBytes - chunkHeaderSize
	store.bucketEntriesPerBlock = store.blockSize / 8

	if err := store.open(opts.BucketCount); err != nil {
		_ = file.Close()
		return nil, err
	}

	return store, nil
}

func (s *LinkedListStore) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	value, found, deleted, err := s.lookupLocked(key, true)
	if err != nil {
		return nil, err
	}
	if !found || deleted {
		return nil, ErrNotFound
	}

	return value, nil
}

func (s *LinkedListStore) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendRecordLocked(key, value, 0)
}

func (s *LinkedListStore) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, found, deleted, err := s.lookupLocked(key, false)
	if err != nil {
		return err
	}
	if !found || deleted {
		return nil
	}

	return s.appendRecordLocked(key, nil, flagDeleted)
}

func (s *LinkedListStore) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrEmptyKey
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, found, deleted, err := s.lookupLocked(key, false)
	if err != nil {
		return false, err
	}

	return found && !deleted, nil
}

func (s *LinkedListStore) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.file.Sync()
}

func (s *LinkedListStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.file.Close()
}

func (s *LinkedListStore) open(configuredBucketCount int64) error {
	buf, err := s.readBlock(0)
	if err != nil {
		return err
	}

	if isZeroBlock(buf) {
		bucketCount := configuredBucketCount
		if bucketCount == 0 {
			bucketCount = defaultBucketCount
		}

		meta := superblock{
			bucketCount:       bucketCount,
			bucketTableBlocks: ceilDiv(bucketCount*8, s.blockSize),
			dataStart:         1 + ceilDiv(bucketCount*8, s.blockSize),
			nextBlock:         1 + ceilDiv(bucketCount*8, s.blockSize),
		}
		if err := validateLayout(meta, s.blockSize); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		s.super = meta

		return s.writeSuperblockLocked()
	}

	meta, err := decodeSuperblock(buf)
	if err != nil {
		return err
	}
	if err := validateLayout(meta, s.blockSize); err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if configuredBucketCount > 0 && configuredBucketCount != meta.bucketCount {
		return ErrBucketCountMismatch
	}

	s.super = meta

	return nil
}

func (s *LinkedListStore) lookupLocked(key []byte, wantValue bool) ([]byte, bool, bool, error) {
	head, err := s.readBucketHeadLocked(s.bucketForKey(key))
	if err != nil {
		return nil, false, false, err
	}

	hash := hashKey(key)
	for head >= 0 {
		rec, err := s.readRecordLocked(head)
		if err != nil {
			return nil, false, false, err
		}
		if rec.keyHash == hash && rec.keyLen == uint32(len(key)) {
			payload, err := s.readPayloadLocked(rec)
			if err != nil {
				return nil, false, false, err
			}
			if bytes.Equal(payload[:len(key)], key) {
				if rec.flags&flagDeleted != 0 {
					return nil, true, true, nil
				}
				if !wantValue {
					return nil, true, false, nil
				}

				return append([]byte(nil), payload[len(key):]...), true, false, nil
			}
		}

		head = rec.nextRecord
	}

	return nil, false, false, nil
}

func (s *LinkedListStore) appendRecordLocked(key []byte, value []byte, flags byte) error {
	payloadSize := int64(len(key)) + int64(len(value))
	chunkCount := ceilDiv(payloadSize, int64(s.chunkPayloadCapacity))
	start, err := s.allocateBlocksLocked(1 + chunkCount)
	if err != nil {
		return err
	}

	recordBlock := start
	payloadHead := start + 1
	if err := s.writePayloadChainLocked(payloadHead, chunkCount, key, value); err != nil {
		return err
	}

	bucket := s.bucketForKey(key)
	head, err := s.readBucketHeadLocked(bucket)
	if err != nil {
		return err
	}

	rec := record{
		flags:       flags,
		nextRecord:  head,
		payloadHead: payloadHead,
		keyHash:     hashKey(key),
		keyLen:      uint32(len(key)),
		valueLen:    uint64(len(value)),
		checksum:    checksumPayload(key, value),
	}
	if err := s.writeRecordLocked(recordBlock, rec); err != nil {
		return err
	}

	return s.writeBucketHeadLocked(bucket, recordBlock)
}

func (s *LinkedListStore) readPayloadLocked(rec record) ([]byte, error) {
	total, err := payloadLen(rec.keyLen, rec.valueLen)
	if err != nil {
		return nil, err
	}

	payload := make([]byte, total)
	offset := 0
	ref := rec.payloadHead
	for ref >= 0 {
		block, err := s.readBlock(ref)
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
		if offset+int(ch.used) > len(payload) {
			return nil, ErrCorrupt
		}

		copy(payload[offset:offset+int(ch.used)], block[chunkHeaderSize:chunkHeaderSize+int(ch.used)])
		offset += int(ch.used)
		ref = ch.next
	}

	if offset != len(payload) {
		return nil, ErrCorrupt
	}
	keyLen := int(rec.keyLen)
	if checksumPayload(payload[:keyLen], payload[keyLen:]) != rec.checksum {
		return nil, ErrCorrupt
	}

	return payload, nil
}

func (s *LinkedListStore) writePayloadChainLocked(start int64, chunkCount int64, key []byte, value []byte) error {
	keyOffset := 0
	valueOffset := 0
	remaining := len(key) + len(value)

	for i := int64(0); i < chunkCount; i++ {
		block := make([]byte, s.blockBytes)
		used := minInt(remaining, s.chunkPayloadCapacity)

		next := int64(-1)
		if i+1 < chunkCount {
			next = start + i + 1
		}
		if err := encodeChunk(chunk{
			next: next,
			used: uint32(used),
		}, block); err != nil {
			return err
		}

		payload := block[chunkHeaderSize : chunkHeaderSize+used]
		written := copy(payload, key[keyOffset:])
		keyOffset += written
		copy(payload[written:], value[valueOffset:])
		valueOffset += used - written
		remaining -= used

		if err := s.file.Write(start+i, block); err != nil {
			return err
		}
	}

	return nil
}

func (s *LinkedListStore) readRecordLocked(index int64) (record, error) {
	block, err := s.readBlock(index)
	if err != nil {
		return record{}, err
	}

	return decodeRecord(block)
}

func (s *LinkedListStore) writeRecordLocked(index int64, rec record) error {
	block := make([]byte, s.blockBytes)
	if err := encodeRecord(rec, block); err != nil {
		return err
	}

	return s.file.Write(index, block)
}

func (s *LinkedListStore) readBucketHeadLocked(bucket int64) (int64, error) {
	blockIndex, offset := s.bucketSlot(bucket)
	block, err := s.readBlock(blockIndex)
	if err != nil {
		return 0, err
	}

	return decodeBlockRef(binary.LittleEndian.Uint64(block[offset : offset+8]))
}

func (s *LinkedListStore) writeBucketHeadLocked(bucket int64, head int64) error {
	blockIndex, offset := s.bucketSlot(bucket)
	block, err := s.readBlock(blockIndex)
	if err != nil {
		return err
	}

	ref, err := encodeBlockRef(head)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(block[offset:offset+8], ref)

	return s.file.Write(blockIndex, block)
}

func (s *LinkedListStore) bucketSlot(bucket int64) (int64, int) {
	return 1 + bucket/s.bucketEntriesPerBlock, int((bucket % s.bucketEntriesPerBlock) * 8)
}

func (s *LinkedListStore) bucketForKey(key []byte) int64 {
	return int64(hashKey(key) % uint64(s.super.bucketCount))
}

func (s *LinkedListStore) allocateBlocksLocked(count int64) (int64, error) {
	if count <= 0 {
		return 0, ErrCorrupt
	}
	if s.super.nextBlock > (1<<63-1)-count {
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

func (s *LinkedListStore) writeSuperblockLocked() error {
	block := make([]byte, s.blockBytes)
	if err := encodeSuperblock(s.super, block); err != nil {
		return err
	}

	return s.file.Write(0, block)
}

func (s *LinkedListStore) readBlock(index int64) ([]byte, error) {
	block := make([]byte, s.blockBytes)
	if err := s.file.Read(index, block); err != nil {
		return nil, err
	}

	return block, nil
}

func hashKey(key []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for _, b := range key {
		hash ^= uint64(b)
		hash *= prime64
	}

	return hash
}

func checksumPayload(key []byte, value []byte) uint32 {
	hash := crc32.New(kvChecksumTable)
	_, _ = hash.Write(key)
	_, _ = hash.Write(value)

	return hash.Sum32()
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}

	return b
}
