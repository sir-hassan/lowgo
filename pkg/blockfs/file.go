package blockfs

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
)

const (
	fileStateUninitialized int32 = iota
	fileStateOpen
	fileStateClosed
)

type blockFile struct {
	file      *os.File
	blockSize int64
	state     atomic.Int32
}

func Open(path string, opts Options) (File, error) {
	opts, err := opts.normalized()
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, opts.Perm)
	if err != nil {
		return nil, err
	}

	bf := &blockFile{
		file:      f,
		blockSize: opts.BlockSize,
	}
	bf.state.Store(fileStateOpen)

	return bf, nil
}

func (f *blockFile) Size() int64 {
	return f.blockSize
}

func (f *blockFile) Read(index int64) ([]byte, error) {
	buf := make([]byte, f.blockSize)
	if err := f.readBlockInto(index, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

func (f *blockFile) readBlockInto(index int64, dst []byte) error {
	offset, err := blockOffset(index, f.blockSize)
	if err != nil {
		return err
	}
	if int64(len(dst)) != f.blockSize {
		return ErrShortBlock
	}

	file, err := f.readFile()
	if err != nil {
		return err
	}

	clear(dst)
	n, err := file.ReadAt(dst, offset)
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) && n >= 0 {
		return nil
	}

	return err
}

func (f *blockFile) Write(index int64, data []byte) error {
	offset, err := blockOffset(index, f.blockSize)
	if err != nil {
		return err
	}
	if int64(len(data)) != f.blockSize {
		return ErrShortBlock
	}

	file, err := f.readFile()
	if err != nil {
		return err
	}

	written := 0
	for written < len(data) {
		n, err := file.WriteAt(data[written:], offset+int64(written))
		written += n
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *blockFile) Sync() error {
	file, err := f.readFile()
	if err != nil {
		return err
	}

	return file.Sync()
}

func (f *blockFile) Close() error {
	if !f.state.CompareAndSwap(fileStateOpen, fileStateClosed) {
		return ErrClosed
	}

	return f.file.Close()
}

func (f *blockFile) readFile() (*os.File, error) {
	if f.state.Load() != fileStateOpen {
		return nil, ErrClosed
	}

	return f.file, nil
}
