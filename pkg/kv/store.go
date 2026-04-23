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
	ErrInvalidType         = errors.New("kv: invalid database type")
)

type Store interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Delete(key []byte) error
	Has(key []byte) (bool, error)
	Sync() error
	Close() error
}

type Type string

const (
	TypeLinkedList Type = "linkedlist"
)

type Options struct {
	BlockSize   int64
	Perm        fs.FileMode
	BucketCount int64
	Type        Type
}

func Open(path string, opts Options) (Store, error) {
	opts, err := opts.normalized()
	if err != nil {
		return nil, err
	}

	switch opts.Type {
	case TypeLinkedList:
		return OpenLinkedList(path, opts)
	default:
		return nil, ErrInvalidType
	}
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
	if o.Type == "" {
		o.Type = TypeLinkedList
	}
	if o.Type != TypeLinkedList {
		return Options{}, ErrInvalidType
	}

	return o, nil
}
