package blockfs

import (
	"sync"
	"sync/atomic"
)

const (
	cacheStateUninitialized int32 = iota
	cacheStateOpen
	cacheStateClosed
)

type cachedFile struct {
	cacheMu sync.RWMutex
	file    File
	state   atomic.Int32
	blocks  map[int64][]byte
	dirty   []int64
}

// Cache wraps an existing block file with an in-memory block cache.
// Reads are served from memory after the first access. Writes update only the
// cache and become durable when Sync flushes dirty blocks to the wrapped file.
func Cache(file File) File {
	f := &cachedFile{
		file:   file,
		blocks: make(map[int64][]byte),
	}
	f.state.Store(cacheStateOpen)

	return f
}

// OpenCached opens a file-backed block store and wraps it with an in-memory
// cache for faster repeated reads.
func OpenCached(path string, opts Options) (File, error) {
	file, err := Open(path, opts)
	if err != nil {
		return nil, err
	}

	return Cache(file), nil
}

func (f *cachedFile) Size() int64 {
	return f.file.Size()
}

func (f *cachedFile) Read(index int64, dst []byte) error {
	if _, err := blockOffset(index, f.file.Size()); err != nil {
		return err
	}
	if int64(len(dst)) != f.file.Size() {
		return ErrShortBlock
	}
	if err := f.ensureOpen(); err != nil {
		return err
	}

	f.cacheMu.RLock()
	if block, ok := f.blocks[index]; ok {
		f.cacheMu.RUnlock()
		copy(dst, block)

		return nil
	}
	f.cacheMu.RUnlock()

	block := make([]byte, f.file.Size())
	if err := f.file.Read(index, block); err != nil {
		return err
	}

	f.cacheMu.Lock()
	if cached, ok := f.blocks[index]; ok {
		f.cacheMu.Unlock()
		copy(dst, cached)

		return nil
	}
	f.blocks[index] = block
	f.cacheMu.Unlock()

	copy(dst, block)

	return nil
}

func (f *cachedFile) Write(index int64, data []byte) error {
	blockSize := f.file.Size()
	if _, err := blockOffset(index, blockSize); err != nil {
		return err
	}
	if int64(len(data)) != blockSize {
		return ErrShortBlock
	}
	if err := f.ensureOpen(); err != nil {
		return err
	}

	f.cacheMu.Lock()
	f.blocks[index] = cloneBlock(data)
	f.dirty = append(f.dirty, index)
	f.cacheMu.Unlock()

	return nil
}

func (f *cachedFile) Sync() error {
	if err := f.ensureOpen(); err != nil {
		return err
	}

	pending := f.pendingDirtyIndexes()
	for _, index := range pending {
		block, ok := f.cachedBlock(index)
		if !ok {
			continue
		}
		if err := f.file.Write(index, block); err != nil {
			return err
		}
	}
	if err := f.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (f *cachedFile) Close() error {
	if !f.state.CompareAndSwap(cacheStateOpen, cacheStateClosed) {
		return ErrClosed
	}

	return f.file.Close()
}

func (f *cachedFile) pendingDirtyIndexes() []int64 {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()

	pending := append([]int64(nil), f.dirty...)
	f.dirty = nil

	return pending
}

func (f *cachedFile) cachedBlock(index int64) ([]byte, bool) {
	f.cacheMu.RLock()
	defer f.cacheMu.RUnlock()

	block, ok := f.blocks[index]
	if !ok {
		return nil, false
	}

	return cloneBlock(block), true
}

func cloneBlock(data []byte) []byte {
	cloned := make([]byte, len(data))
	copy(cloned, data)

	return cloned
}

func (f *cachedFile) ensureOpen() error {
	if f.state.Load() != cacheStateOpen {
		return ErrClosed
	}

	return nil
}
