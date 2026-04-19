package blockfs

import "sync"

type cachedFile struct {
	stateMu sync.Mutex
	cacheMu sync.RWMutex
	file    File
	closed  bool
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

func (f *cachedFile) BlockSize() int64 {
	return f.file.BlockSize()
}

func (f *cachedFile) ReadBlock(index int64) ([]byte, error) {
	if _, err := blockOffset(index, f.file.BlockSize()); err != nil {
		return nil, err
	}
	if err := f.ensureOpen(); err != nil {
		return nil, err
	}

	f.cacheMu.RLock()
	if block, ok := f.blocks[index]; ok {
		f.cacheMu.RUnlock()
		return cloneBlock(block), nil
	}
	f.cacheMu.RUnlock()

	block, err := f.file.ReadBlock(index)
	if err != nil {
		return nil, err
	}
	block = cloneBlock(block)

	f.cacheMu.Lock()
	if cached, ok := f.blocks[index]; ok {
		f.cacheMu.Unlock()
		return cloneBlock(cached), nil
	}
	f.blocks[index] = block
	f.cacheMu.Unlock()

	return cloneBlock(block), nil
}

func (f *cachedFile) WriteBlock(index int64, data []byte) error {
	blockSize := f.file.BlockSize()
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
		if err := f.file.WriteBlock(index, block); err != nil {
			return err
		}
	}
	if err := f.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (f *cachedFile) Close() error {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()

	if f.closed {
		return ErrClosed
	}
	f.closed = true

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
	f.stateMu.Lock()
	defer f.stateMu.Unlock()

	if f.closed {
		return ErrClosed
	}

	return nil
}
