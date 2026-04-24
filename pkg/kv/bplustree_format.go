package kv

import "encoding/binary"

const (
	bplusFormatVersion = 1

	bplusSuperblockSize    = 64
	bplusNodeHeaderSize    = 32
	bplusLeafEntrySize     = 32
	bplusInternalEntrySize = 24

	bplusNodeTypeLeaf     = 1
	bplusNodeTypeInternal = 2
)

var (
	bplusSuperblockMagic = [8]byte{'B', 'K', 'V', 'B', 'P', 'T', 0x01, 0x00}
	bplusNodeMagic       = [8]byte{'B', 'K', 'V', 'B', 'N', 'D', 0x01, 0x00}
)

type bplusSuperblock struct {
	rootBlock int64
	nextBlock int64
}

type bplusLeafEntry struct {
	keyLen   uint32
	keyRef   int64
	valueLen uint64
	valueRef int64
}

type bplusInternalEntry struct {
	keyLen uint32
	keyRef int64
	child  int64
}

type bplusNode struct {
	kind            byte
	firstChild      int64
	nextLeaf        int64
	leafEntries     []bplusLeafEntry
	internalEntries []bplusInternalEntry
}

func encodeBPlusSuperblock(meta bplusSuperblock, dst []byte) error {
	if len(dst) < bplusSuperblockSize || meta.rootBlock < 0 || meta.nextBlock <= meta.rootBlock {
		return ErrCorrupt
	}

	clear(dst)
	copy(dst[:len(bplusSuperblockMagic)], bplusSuperblockMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], bplusFormatVersion)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(meta.rootBlock))
	binary.LittleEndian.PutUint64(dst[24:32], uint64(meta.nextBlock))

	return nil
}

func decodeBPlusSuperblock(src []byte) (bplusSuperblock, error) {
	if len(src) < bplusSuperblockSize {
		return bplusSuperblock{}, ErrCorrupt
	}
	if string(src[:len(bplusSuperblockMagic)]) != string(bplusSuperblockMagic[:]) {
		return bplusSuperblock{}, ErrCorrupt
	}
	if version := binary.LittleEndian.Uint16(src[8:10]); version != bplusFormatVersion {
		return bplusSuperblock{}, ErrCorrupt
	}

	meta := bplusSuperblock{
		rootBlock: int64(binary.LittleEndian.Uint64(src[16:24])),
		nextBlock: int64(binary.LittleEndian.Uint64(src[24:32])),
	}
	if meta.rootBlock < 0 || meta.nextBlock <= meta.rootBlock {
		return bplusSuperblock{}, ErrCorrupt
	}

	return meta, nil
}

func encodeBPlusNode(node bplusNode, dst []byte) error {
	if len(dst) < bplusNodeHeaderSize {
		return ErrCorrupt
	}

	clear(dst)
	copy(dst[:len(bplusNodeMagic)], bplusNodeMagic[:])
	dst[8] = node.kind

	switch node.kind {
	case bplusNodeTypeLeaf:
		if len(node.leafEntries) > int((len(dst)-bplusNodeHeaderSize)/bplusLeafEntrySize) {
			return ErrCorrupt
		}
		nextLeaf, err := encodeBlockRef(node.nextLeaf)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(dst[12:16], uint32(len(node.leafEntries)))
		binary.LittleEndian.PutUint64(dst[24:32], nextLeaf)

		offset := bplusNodeHeaderSize
		for _, entry := range node.leafEntries {
			keyRef, err := encodeBlockRef(entry.keyRef)
			if err != nil {
				return err
			}
			valueRef, err := encodeBlockRef(entry.valueRef)
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint32(dst[offset:offset+4], entry.keyLen)
			binary.LittleEndian.PutUint64(dst[offset+8:offset+16], keyRef)
			binary.LittleEndian.PutUint64(dst[offset+16:offset+24], entry.valueLen)
			binary.LittleEndian.PutUint64(dst[offset+24:offset+32], valueRef)
			offset += bplusLeafEntrySize
		}
	case bplusNodeTypeInternal:
		if len(node.internalEntries) > int((len(dst)-bplusNodeHeaderSize)/bplusInternalEntrySize) || node.firstChild < 0 {
			return ErrCorrupt
		}
		firstChild, err := encodeBlockRef(node.firstChild)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(dst[12:16], uint32(len(node.internalEntries)))
		binary.LittleEndian.PutUint64(dst[16:24], firstChild)

		offset := bplusNodeHeaderSize
		for _, entry := range node.internalEntries {
			keyRef, err := encodeBlockRef(entry.keyRef)
			if err != nil {
				return err
			}
			child, err := encodeBlockRef(entry.child)
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint32(dst[offset:offset+4], entry.keyLen)
			binary.LittleEndian.PutUint64(dst[offset+8:offset+16], keyRef)
			binary.LittleEndian.PutUint64(dst[offset+16:offset+24], child)
			offset += bplusInternalEntrySize
		}
	default:
		return ErrCorrupt
	}

	return nil
}

func decodeBPlusNode(src []byte) (bplusNode, error) {
	if len(src) < bplusNodeHeaderSize {
		return bplusNode{}, ErrCorrupt
	}
	if string(src[:len(bplusNodeMagic)]) != string(bplusNodeMagic[:]) {
		return bplusNode{}, ErrCorrupt
	}

	node := bplusNode{
		kind: src[8],
	}
	count := int(binary.LittleEndian.Uint32(src[12:16]))

	switch node.kind {
	case bplusNodeTypeLeaf:
		if count > (len(src)-bplusNodeHeaderSize)/bplusLeafEntrySize {
			return bplusNode{}, ErrCorrupt
		}
		nextLeaf, err := decodeBlockRef(binary.LittleEndian.Uint64(src[24:32]))
		if err != nil {
			return bplusNode{}, err
		}
		node.nextLeaf = nextLeaf
		node.firstChild = -1
		node.leafEntries = make([]bplusLeafEntry, count)

		offset := bplusNodeHeaderSize
		for i := range count {
			keyRef, err := decodeBlockRef(binary.LittleEndian.Uint64(src[offset+8 : offset+16]))
			if err != nil {
				return bplusNode{}, err
			}
			valueRef, err := decodeBlockRef(binary.LittleEndian.Uint64(src[offset+24 : offset+32]))
			if err != nil {
				return bplusNode{}, err
			}
			entry := bplusLeafEntry{
				keyLen:   binary.LittleEndian.Uint32(src[offset : offset+4]),
				keyRef:   keyRef,
				valueLen: binary.LittleEndian.Uint64(src[offset+16 : offset+24]),
				valueRef: valueRef,
			}
			if entry.keyLen == 0 || entry.keyRef < 0 {
				return bplusNode{}, ErrCorrupt
			}
			if entry.valueLen == 0 && entry.valueRef >= 0 {
				return bplusNode{}, ErrCorrupt
			}
			if entry.valueLen > 0 && entry.valueRef < 0 {
				return bplusNode{}, ErrCorrupt
			}
			node.leafEntries[i] = entry
			offset += bplusLeafEntrySize
		}
	case bplusNodeTypeInternal:
		if count > (len(src)-bplusNodeHeaderSize)/bplusInternalEntrySize {
			return bplusNode{}, ErrCorrupt
		}
		firstChild, err := decodeBlockRef(binary.LittleEndian.Uint64(src[16:24]))
		if err != nil {
			return bplusNode{}, err
		}
		if firstChild < 0 {
			return bplusNode{}, ErrCorrupt
		}
		node.firstChild = firstChild
		node.nextLeaf = -1
		node.internalEntries = make([]bplusInternalEntry, count)

		offset := bplusNodeHeaderSize
		for i := range count {
			keyRef, err := decodeBlockRef(binary.LittleEndian.Uint64(src[offset+8 : offset+16]))
			if err != nil {
				return bplusNode{}, err
			}
			child, err := decodeBlockRef(binary.LittleEndian.Uint64(src[offset+16 : offset+24]))
			if err != nil {
				return bplusNode{}, err
			}
			entry := bplusInternalEntry{
				keyLen: binary.LittleEndian.Uint32(src[offset : offset+4]),
				keyRef: keyRef,
				child:  child,
			}
			if entry.keyLen == 0 || entry.keyRef < 0 || entry.child < 0 {
				return bplusNode{}, ErrCorrupt
			}
			node.internalEntries[i] = entry
			offset += bplusInternalEntrySize
		}
	default:
		return bplusNode{}, ErrCorrupt
	}

	return node, nil
}
