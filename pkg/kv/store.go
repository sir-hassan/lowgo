package kv

import (
	"errors"
	"io/fs"
)

var (
	ErrEmptyKey            = errors.New("kv: empty key")
	ErrNotFound            = errors.New("kv: key not found")
	ErrCorrupt             = errors.New("kv: corrupt store data")
	ErrInvalidBucketCount  = errors.New("kv: invalid bucket count")
	ErrBucketCountMismatch = errors.New("kv: bucket count does not match file metadata")
	ErrBlockSizeTooSmall   = errors.New("kv: block size too small for store")
	ErrInvalidBlockSize    = errors.New("kv: invalid block size")
)

type Store interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Delete(key []byte) error
	Has(key []byte) (bool, error)
	Sync() error
	Close() error
}

type Options struct {
	BlockSize   int64
	Perm        fs.FileMode
	BucketCount int64
}

func Open(path string, opts Options) (Store, error) {
	return OpenLinkedList(path, opts)
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
	if o.BucketCount < 0 {
		return Options{}, ErrInvalidBucketCount
	}

	return o, nil
}
