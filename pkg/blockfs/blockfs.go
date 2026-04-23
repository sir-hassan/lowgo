package blockfs

import (
	"errors"
	"fmt"
	"io/fs"
)

var (
	ErrClosed            = errors.New("blockfs: file is closed")
	ErrInvalidBlockSize  = errors.New("blockfs: invalid block size")
	ErrInvalidBlockIndex = errors.New("blockfs: invalid block index")
	ErrShortBlock        = errors.New("blockfs: buffer size must match block size")
	ErrCorruptHeader     = errors.New("blockfs: corrupt header")
	ErrBlockSizeMismatch = errors.New("blockfs: block size does not match file header")
)

type File interface {
	Size() int64
	Read(index int64, dst []byte) error
	Write(index int64, data []byte) error
	Sync() error
	Close() error
}

type Options struct {
	BlockSize int64
	Perm      fs.FileMode
}

func (o Options) normalized() (Options, error) {
	if o.BlockSize < 0 {
		return Options{}, ErrInvalidBlockSize
	}
	if o.BlockSize == 0 {
		o.BlockSize = 4 * 1024
	}
	if o.Perm == 0 {
		o.Perm = 0o600
	}

	return o, nil
}

func blockOffset(index int64, blockSize int64) (int64, error) {
	if index < 0 {
		return 0, ErrInvalidBlockIndex
	}
	if index > (1<<63-1-headerRegionSize)/blockSize {
		return 0, fmt.Errorf("blockfs: block offset overflow: %w", ErrInvalidBlockIndex)
	}

	return headerRegionSize + index*blockSize, nil
}
