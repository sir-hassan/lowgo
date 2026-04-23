package blockfs

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

var headerMagic = [8]byte{'B', 'L', 'K', 'F', 'S', 0x02, 0x00, 0x00}

const (
	headerSize       = 24
	headerRegionSize = 4 * 1024
)

type header struct {
	blockSize int64
	nextIndex int64
}

func encodeHeader(blockSize int64, nextIndex int64, dst []byte) error {
	if int64(len(dst)) < headerSize {
		return ErrShortBlock
	}
	if blockSize <= 0 || nextIndex < 0 {
		return ErrCorruptHeader
	}

	clear(dst)
	copy(dst[:len(headerMagic)], headerMagic[:])
	binary.LittleEndian.PutUint64(dst[8:16], uint64(blockSize))
	binary.LittleEndian.PutUint64(dst[16:24], uint64(nextIndex))

	return nil
}

func decodeHeader(src []byte) (header, error) {
	if len(src) < headerSize {
		return header{}, ErrCorruptHeader
	}
	if string(src[:len(headerMagic)]) != string(headerMagic[:]) {
		return header{}, ErrCorruptHeader
	}

	blockSize := int64(binary.LittleEndian.Uint64(src[8:16]))
	nextIndex := int64(binary.LittleEndian.Uint64(src[16:24]))
	if blockSize <= 0 || nextIndex < 0 {
		return header{}, ErrCorruptHeader
	}

	return header{
		blockSize: blockSize,
		nextIndex: nextIndex,
	}, nil
}

func openHeader(file *os.File, configuredBlockSize int64) (header, error) {
	info, err := file.Stat()
	if err != nil {
		return header{}, err
	}

	if info.Size() == 0 {
		meta := header{
			blockSize: configuredBlockSize,
			nextIndex: 0,
		}
		if err := writeHeader(file, meta); err != nil {
			return header{}, err
		}

		return meta, nil
	}

	raw := make([]byte, headerRegionSize)
	if _, err := file.ReadAt(raw, 0); err != nil && !errors.Is(err, io.EOF) {
		return header{}, err
	}

	meta, err := decodeHeader(raw)
	if err != nil {
		return header{}, err
	}
	if meta.blockSize != configuredBlockSize {
		return header{}, ErrBlockSizeMismatch
	}

	return meta, nil
}

func writeHeader(file *os.File, meta header) error {
	buf := make([]byte, headerRegionSize)
	if err := encodeHeader(meta.blockSize, meta.nextIndex, buf); err != nil {
		return err
	}

	written := 0
	for written < len(buf) {
		n, err := file.WriteAt(buf[written:], int64(written))
		written += n
		if err != nil {
			return err
		}
	}

	return nil
}
