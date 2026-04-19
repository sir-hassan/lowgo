package blockfs_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sir-hassan/lowgo/pkg/blockfs"
)

type fakeFile struct {
	blockSize  int64
	readBlocks map[int64][]byte
	writes     map[int64][]byte
	readCount  int
	syncCount  int
	closeCount int
	closed     bool
}

func newFakeFile(blockSize int64) *fakeFile {
	return &fakeFile{
		blockSize:  blockSize,
		readBlocks: make(map[int64][]byte),
		writes:     make(map[int64][]byte),
	}
}

func (f *fakeFile) Size() int64 {
	return f.blockSize
}

func (f *fakeFile) Read(index int64) ([]byte, error) {
	if f.closed {
		return nil, blockfs.ErrClosed
	}
	f.readCount++
	if block, ok := f.readBlocks[index]; ok {
		out := make([]byte, len(block))
		copy(out, block)
		return out, nil
	}

	return make([]byte, f.blockSize), nil
}

func (f *fakeFile) Write(index int64, data []byte) error {
	if f.closed {
		return blockfs.ErrClosed
	}
	out := make([]byte, len(data))
	copy(out, data)
	f.writes[index] = out
	f.readBlocks[index] = out
	return nil
}

func (f *fakeFile) Sync() error {
	if f.closed {
		return blockfs.ErrClosed
	}
	f.syncCount++
	return nil
}

func (f *fakeFile) Close() error {
	if f.closed {
		return blockfs.ErrClosed
	}
	f.closed = true
	f.closeCount++
	return nil
}

func TestCacheServesRepeatedReadsFromMemory(t *testing.T) {
	t.Parallel()

	inner := newFakeFile(8)
	inner.readBlocks[3] = []byte("abcdefgh")

	cached := blockfs.Cache(inner)

	got1, err := cached.Read(3)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	got1[0] = 'z'

	got2, err := cached.Read(3)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if inner.readCount != 1 {
		t.Fatalf("expected one inner read, got %d", inner.readCount)
	}
	if !bytes.Equal(got2, []byte("abcdefgh")) {
		t.Fatalf("expected cached block contents to remain stable, got %q", got2)
	}
}

func TestCacheWritesStayInMemoryUntilSync(t *testing.T) {
	t.Parallel()

	inner := newFakeFile(8)
	cached := blockfs.Cache(inner)

	payload := []byte("12345678")
	if err := cached.Write(5, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	payload[0] = 'x'

	if inner.readCount != 0 {
		t.Fatalf("expected no inner reads before cached read, got %d", inner.readCount)
	}
	if _, ok := inner.writes[5]; ok {
		t.Fatalf("expected no inner writes before sync, got %q", inner.writes[5])
	}

	got, err := cached.Read(5)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if inner.readCount != 0 {
		t.Fatalf("expected cached read to avoid inner file, got %d inner reads", inner.readCount)
	}
	if !bytes.Equal(got, []byte("12345678")) {
		t.Fatalf("expected cached block, got %q", got)
	}

	if err := cached.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !bytes.Equal(inner.writes[5], []byte("12345678")) {
		t.Fatalf("expected sync to flush payload, got %q", inner.writes[5])
	}
}

func TestCacheSyncFlushesLatestValueForRepeatedDirtyIndex(t *testing.T) {
	t.Parallel()

	inner := newFakeFile(8)
	cached := blockfs.Cache(inner)

	if err := cached.Write(2, []byte("11111111")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := cached.Write(2, []byte("22222222")); err != nil {
		t.Fatalf("second write: %v", err)
	}
	if err := cached.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !bytes.Equal(inner.writes[2], []byte("22222222")) {
		t.Fatalf("expected latest value to be flushed, got %q", inner.writes[2])
	}
}

func TestOpenCachedPersistsThroughWrappedFile(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/data.bin"
	writer, err := blockfs.OpenCached(path, blockfs.Options{BlockSize: 8})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}

	if err := writer.Write(1, []byte("persisted")); !errors.Is(err, blockfs.ErrShortBlock) {
		if err == nil {
			t.Fatal("expected short block error for oversized payload")
		}
	}

	block := []byte("block-01")
	if err := writer.Write(1, block); err != nil {
		t.Fatalf("write block: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader, err := blockfs.Open(path, blockfs.Options{BlockSize: 8})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() {
		_ = reader.Close()
	})

	got, err := reader.Read(1)
	if err != nil {
		t.Fatalf("read persisted block: %v", err)
	}
	if !bytes.Equal(got, block) {
		t.Fatalf("expected %q, got %q", block, got)
	}
}

func TestCacheDelegatesSyncAndClose(t *testing.T) {
	t.Parallel()

	inner := newFakeFile(8)
	cached := blockfs.Cache(inner)

	if err := cached.Write(0, []byte("12345678")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cached.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if inner.syncCount != 1 {
		t.Fatalf("expected one sync delegation, got %d", inner.syncCount)
	}
	if !bytes.Equal(inner.writes[0], []byte("12345678")) {
		t.Fatalf("expected sync to flush dirty block, got %q", inner.writes[0])
	}

	if err := cached.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if inner.closeCount != 1 {
		t.Fatalf("expected one close delegation, got %d", inner.closeCount)
	}

	if _, err := cached.Read(0); !errors.Is(err, blockfs.ErrClosed) {
		t.Fatalf("expected ErrClosed after close, got %v", err)
	}
}
