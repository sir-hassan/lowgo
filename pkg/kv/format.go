package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	defaultBucketCount = 256
	formatVersion      = 1

	superblockSize  = 64
	recordSize      = 64
	chunkHeaderSize = 24
	minBlockSize    = recordSize

	flagDeleted = 1 << 0
)

var (
	superblockMagic = [8]byte{'B', 'K', 'V', 'S', 'T', 'R', 0x01, 0x00}
	recordMagic     = [8]byte{'B', 'K', 'V', 'R', 'E', 'C', 0x01, 0x00}
	chunkMagic      = [8]byte{'B', 'K', 'V', 'C', 'H', 'N', 0x01, 0x00}
)

type superblock struct {
	bucketCount       int64
	bucketTableBlocks int64
	dataStart         int64
	nextBlock         int64
}

type record struct {
	flags       byte
	nextRecord  int64
	payloadHead int64
	keyHash     uint64
	keyLen      uint32
	valueLen    uint64
	checksum    uint32
}

type chunk struct {
	next int64
	used uint32
}

func encodeSuperblock(meta superblock, dst []byte) error {
	if len(dst) < superblockSize {
		return ErrCorrupt
	}
	if meta.bucketCount <= 0 || meta.bucketTableBlocks <= 0 || meta.dataStart <= 0 || meta.nextBlock < meta.dataStart {
		return ErrCorrupt
	}

	clear(dst)
	copy(dst[:len(superblockMagic)], superblockMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], formatVersion)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(meta.bucketCount))
	binary.LittleEndian.PutUint64(dst[24:32], uint64(meta.bucketTableBlocks))
	binary.LittleEndian.PutUint64(dst[32:40], uint64(meta.dataStart))
	binary.LittleEndian.PutUint64(dst[40:48], uint64(meta.nextBlock))

	return nil
}

func decodeSuperblock(src []byte) (superblock, error) {
	if len(src) < superblockSize {
		return superblock{}, ErrCorrupt
	}
	if string(src[:len(superblockMagic)]) != string(superblockMagic[:]) {
		return superblock{}, ErrCorrupt
	}
	if version := binary.LittleEndian.Uint16(src[8:10]); version != formatVersion {
		return superblock{}, ErrCorrupt
	}

	meta := superblock{
		bucketCount:       int64(binary.LittleEndian.Uint64(src[16:24])),
		bucketTableBlocks: int64(binary.LittleEndian.Uint64(src[24:32])),
		dataStart:         int64(binary.LittleEndian.Uint64(src[32:40])),
		nextBlock:         int64(binary.LittleEndian.Uint64(src[40:48])),
	}
	if meta.bucketCount <= 0 || meta.bucketTableBlocks <= 0 || meta.dataStart <= 0 || meta.nextBlock < meta.dataStart {
		return superblock{}, ErrCorrupt
	}

	return meta, nil
}

func encodeRecord(rec record, dst []byte) error {
	if len(dst) < recordSize {
		return ErrCorrupt
	}
	if rec.payloadHead < 0 || rec.keyLen == 0 {
		return ErrCorrupt
	}

	nextRecord, err := encodeBlockRef(rec.nextRecord)
	if err != nil {
		return err
	}
	payloadHead, err := encodeBlockRef(rec.payloadHead)
	if err != nil {
		return err
	}

	clear(dst)
	copy(dst[:len(recordMagic)], recordMagic[:])
	dst[8] = rec.flags
	binary.LittleEndian.PutUint64(dst[16:24], nextRecord)
	binary.LittleEndian.PutUint64(dst[24:32], payloadHead)
	binary.LittleEndian.PutUint64(dst[32:40], rec.keyHash)
	binary.LittleEndian.PutUint32(dst[40:44], rec.keyLen)
	binary.LittleEndian.PutUint64(dst[48:56], rec.valueLen)
	binary.LittleEndian.PutUint32(dst[56:60], rec.checksum)

	return nil
}

func decodeRecord(src []byte) (record, error) {
	if len(src) < recordSize {
		return record{}, ErrCorrupt
	}
	if string(src[:len(recordMagic)]) != string(recordMagic[:]) {
		return record{}, ErrCorrupt
	}

	nextRecord, err := decodeBlockRef(binary.LittleEndian.Uint64(src[16:24]))
	if err != nil {
		return record{}, err
	}
	payloadHead, err := decodeBlockRef(binary.LittleEndian.Uint64(src[24:32]))
	if err != nil {
		return record{}, err
	}

	rec := record{
		flags:       src[8],
		nextRecord:  nextRecord,
		payloadHead: payloadHead,
		keyHash:     binary.LittleEndian.Uint64(src[32:40]),
		keyLen:      binary.LittleEndian.Uint32(src[40:44]),
		valueLen:    binary.LittleEndian.Uint64(src[48:56]),
		checksum:    binary.LittleEndian.Uint32(src[56:60]),
	}
	if rec.payloadHead < 0 || rec.keyLen == 0 {
		return record{}, ErrCorrupt
	}

	return rec, nil
}

func encodeChunk(ch chunk, dst []byte) error {
	if len(dst) < chunkHeaderSize {
		return ErrCorrupt
	}
	if ch.used == 0 {
		return ErrCorrupt
	}

	next, err := encodeBlockRef(ch.next)
	if err != nil {
		return err
	}

	clear(dst)
	copy(dst[:len(chunkMagic)], chunkMagic[:])
	binary.LittleEndian.PutUint64(dst[8:16], next)
	binary.LittleEndian.PutUint32(dst[16:20], ch.used)

	return nil
}

func decodeChunk(src []byte) (chunk, error) {
	if len(src) < chunkHeaderSize {
		return chunk{}, ErrCorrupt
	}
	if string(src[:len(chunkMagic)]) != string(chunkMagic[:]) {
		return chunk{}, ErrCorrupt
	}

	next, err := decodeBlockRef(binary.LittleEndian.Uint64(src[8:16]))
	if err != nil {
		return chunk{}, err
	}

	ch := chunk{
		next: next,
		used: binary.LittleEndian.Uint32(src[16:20]),
	}
	if ch.used == 0 {
		return chunk{}, ErrCorrupt
	}

	return ch, nil
}

func encodeBlockRef(index int64) (uint64, error) {
	if index < 0 {
		return 0, nil
	}
	if index == math.MaxInt64 {
		return 0, fmt.Errorf("kv: block reference overflow: %w", ErrCorrupt)
	}

	return uint64(index) + 1, nil
}

func decodeBlockRef(ref uint64) (int64, error) {
	if ref == 0 {
		return -1, nil
	}
	if ref-1 > math.MaxInt64 {
		return 0, fmt.Errorf("kv: block reference overflow: %w", ErrCorrupt)
	}

	return int64(ref - 1), nil
}

func isZeroBlock(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}

	return true
}

func ceilDiv(n int64, d int64) int64 {
	return (n + d - 1) / d
}

func payloadLen(keyLen uint32, valueLen uint64) (int64, error) {
	total := uint64(keyLen) + valueLen
	if total < uint64(keyLen) || total > math.MaxInt64 {
		return 0, fmt.Errorf("kv: payload length overflow: %w", ErrCorrupt)
	}

	return int64(total), nil
}

func validateLayout(meta superblock, blockSize int64) error {
	expectedTableBlocks := ceilDiv(meta.bucketCount*8, blockSize)
	if expectedTableBlocks != meta.bucketTableBlocks {
		return errors.New("kv: invalid bucket table layout")
	}
	if meta.dataStart != 1+meta.bucketTableBlocks {
		return errors.New("kv: invalid data start")
	}

	return nil
}
