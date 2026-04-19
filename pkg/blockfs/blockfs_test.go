package blockfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/sir-hassan/lowgo/pkg/blockfs"
)

// TestOpenRejectsInvalidBlockSize verifies that opening a block file without a
// positive block size fails with ErrInvalidBlockSize.
func TestOpenRejectsInvalidBlockSize(t *testing.T) {
	t.Parallel()

	_, err := blockfs.Open(filepath.Join(t.TempDir(), "data.bin"), blockfs.Options{})
	if !errors.Is(err, blockfs.ErrInvalidBlockSize) {
		t.Fatalf("expected ErrInvalidBlockSize, got %v", err)
	}
}

// TestReadWriteBlockRoundTrip verifies that a written block can be synced and
// read back unchanged from the same index.
func TestReadWriteBlockRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	block := make([]byte, bf.Size())
	for i := range block {
		block[i] = byte(i % 251)
	}

	if err := bf.Write(2, block); err != nil {
		t.Fatalf("write block: %v", err)
	}
	if err := bf.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	got, err := bf.Read(2)
	if err != nil {
		t.Fatalf("read block: %v", err)
	}
	if string(got) != string(block) {
		t.Fatal("block contents mismatch")
	}
}

// TestReadSparseBlockReturnsZeroFilledData verifies that reading an unwritten
// block returns a zero-filled buffer.
func TestReadSparseBlockReturnsZeroFilledData(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	got, err := bf.Read(8)
	if err != nil {
		t.Fatalf("read sparse block: %v", err)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("expected zero-filled block at byte %d, got %d", i, b)
		}
	}
}

// TestWriteRejectsWrongSizedBuffer verifies that writes fail when the provided
// buffer size does not match the configured block size.
func TestWriteRejectsWrongSizedBuffer(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	err = bf.Write(0, make([]byte, 1024))
	if !errors.Is(err, blockfs.ErrShortBlock) {
		t.Fatalf("expected ErrShortBlock, got %v", err)
	}
}

// TestReadBlockZeroFillsRemainderOfPartialBlock verifies that reading a
// partially present block leaves the unread tail zero-filled.
func TestReadBlockZeroFillsRemainderOfPartialBlock(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 8})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	got, err := bf.Read(0)
	if err != nil {
		t.Fatalf("read block: %v", err)
	}

	want := []byte{'h', 'e', 'l', 'l', 'o', 0, 0, 0}
	if !bytes.Equal(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

// TestReadRejectsInvalidBlockIndex verifies that reads reject negative and
// overflowing block indexes.
func TestReadRejectsInvalidBlockIndex(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	for _, index := range []int64{-1, math.MaxInt64/4096 + 1} {
		if _, err := bf.Read(index); !errors.Is(err, blockfs.ErrInvalidBlock) {
			t.Fatalf("expected ErrInvalidBlock for index %d, got %v", index, err)
		}
	}
}

// TestWriteRejectsInvalidBlockIndex verifies that writes reject negative and
// overflowing block indexes.
func TestWriteRejectsInvalidBlockIndex(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	block := make([]byte, bf.Size())
	for _, index := range []int64{-1, math.MaxInt64/4096 + 1} {
		if err := bf.Write(index, block); !errors.Is(err, blockfs.ErrInvalidBlock) {
			t.Fatalf("expected ErrInvalidBlock for index %d, got %v", index, err)
		}
	}
}

// TestCloseMakesFurtherOperationsFail verifies that closing a block file is
// idempotent only once and that later operations fail with ErrClosed.
func TestCloseMakesFurtherOperationsFail(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := bf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := bf.Close(); !errors.Is(err, blockfs.ErrClosed) {
		t.Fatalf("expected ErrClosed on second close, got %v", err)
	}
	if _, err := bf.Read(0); !errors.Is(err, blockfs.ErrClosed) {
		t.Fatalf("expected ErrClosed on read after close, got %v", err)
	}
	if err := bf.Sync(); !errors.Is(err, blockfs.ErrClosed) {
		t.Fatalf("expected ErrClosed on sync after close, got %v", err)
	}
}

// TestSyncPersistsDataAcrossReopen verifies that synced block data remains
// available after closing and reopening the file.
func TestSyncPersistsDataAcrossReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")

	writer, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}

	payload := make([]byte, writer.Size())
	copy(payload, []byte("persistent block"))

	if err := writer.Write(1, payload); err != nil {
		t.Fatalf("write block: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 2*writer.Size() {
		t.Fatalf("expected file size %d, got %d", 2*writer.Size(), info.Size())
	}

	reader, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
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
	if string(got) != string(payload) {
		t.Fatal("persisted block contents mismatch")
	}
}

// TestWriteAndReadBackOneHundredBlocks verifies sequential round-trip behavior
// across 100 written blocks and checks the resulting file size.
func TestWriteAndReadBackOneHundredBlocks(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	const totalBlocks = 100

	wantBlocks := make([][]byte, totalBlocks)
	for i := 0; i < totalBlocks; i++ {
		block := make([]byte, bf.Size())
		prefix := []byte(fmt.Sprintf("block-%03d", i))
		copy(block, prefix)
		for j := len(prefix); j < len(block); j++ {
			block[j] = byte((i + j) % 251)
		}

		if err := bf.Write(int64(i), block); err != nil {
			t.Fatalf("write block %d: %v", i, err)
		}
		wantBlocks[i] = block
	}

	if err := bf.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != int64(totalBlocks)*bf.Size() {
		t.Fatalf("expected file size %d, got %d", int64(totalBlocks)*bf.Size(), info.Size())
	}

	for i, want := range wantBlocks {
		got, err := bf.Read(int64(i))
		if err != nil {
			t.Fatalf("read block %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("block %d contents mismatch", i)
		}
	}
}

// TestReadNonExistentBlocksAfterWritingOneHundredBlocksReturnsZeroFilledData
// verifies that block indexes beyond the written range still read back as
// zero-filled data.
func TestReadNonExistentBlocksAfterWritingOneHundredBlocksReturnsZeroFilledData(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	const totalBlocks = 100

	for i := 0; i < totalBlocks; i++ {
		block := make([]byte, bf.Size())
		block[0] = byte(i)
		if err := bf.Write(int64(i), block); err != nil {
			t.Fatalf("write block %d: %v", i, err)
		}
	}

	if err := bf.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	for _, index := range []int64{100, 150, 999} {
		got, err := bf.Read(index)
		if err != nil {
			t.Fatalf("read non-existent block %d: %v", index, err)
		}
		for i, b := range got {
			if b != 0 {
				t.Fatalf("expected zero-filled data for block %d at byte %d, got %d", index, i, b)
			}
		}
	}
}

// TestOpenUsesDefaultPermissionsWhenUnset verifies that opening a new file
// without explicit permissions applies the default mode.
func TestOpenUsesDefaultPermissionsWhenUnset(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("expected default permissions 0600, got %04o", got)
	}
}

// TestBasicWriteRead verifies a simple multi-block write/read sequence using a
// small block size and predictable string payloads.
func TestBasicWriteRead(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 8})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	const totalBlocks = 10

	for i := 0; i < totalBlocks; i++ {
		str := strings.Repeat(strconv.Itoa(i), int(bf.Size()))
		if err := bf.Write(int64(i), []byte(str)); err != nil {
			t.Fatalf("write block %d: %v", i, err)
		}
	}

	if err := bf.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	got := ""
	for i := 0; i < totalBlocks; i++ {
		st, err := bf.Read(int64(i))
		if err != nil {
			t.Fatalf("read block %d: %v", i, err)
		}
		got += string(st)
	}
	want := "00000000111111112222222233333333444444445555555566666666777777778888888899999999"
	if got != want {
		t.Fatalf("expected file content %s, got %s", want, got)
	}
}

// BenchmarkWriteTenThousandOneKilobyteBlocks measures sequential writes of
// 10,000 blocks with 1 KiB payloads, including the final sync.
func BenchmarkWriteTenThousandOneKilobyteBlocks(b *testing.B) {
	const (
		totalBlocks = 10_000
		blockSize   = 1024
	)

	payloads := benchmarkBlockPayloads(totalBlocks, blockSize)
	b.SetBytes(int64(totalBlocks * blockSize))
	b.ReportAllocs()

	for b.Loop() {
		path := filepath.Join(b.TempDir(), "data.bin")
		bf, err := blockfs.Open(path, blockfs.Options{BlockSize: blockSize})
		if err != nil {
			b.Fatalf("open: %v", err)
		}

		for i, payload := range payloads {
			if err := bf.Write(int64(i), payload); err != nil {
				_ = bf.Close()
				b.Fatalf("write block %d: %v", i, err)
			}
		}
		if err := bf.Sync(); err != nil {
			_ = bf.Close()
			b.Fatalf("sync: %v", err)
		}
		if err := bf.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
}

// BenchmarkReadTenThousandOneKilobyteBlocks measures sequential reads of
// 10,000 pre-seeded 1 KiB blocks and validates the returned contents.
func BenchmarkReadTenThousandOneKilobyteBlocks(b *testing.B) {
	const (
		totalBlocks = 10_000
		blockSize   = 1024
	)

	payloads := benchmarkBlockPayloads(totalBlocks, blockSize)
	b.SetBytes(int64(totalBlocks * blockSize))
	b.ReportAllocs()

	for b.Loop() {
		path := filepath.Join(b.TempDir(), "data.bin")
		writer, err := blockfs.Open(path, blockfs.Options{BlockSize: blockSize})
		if err != nil {
			b.Fatalf("open writer: %v", err)
		}
		for i, payload := range payloads {
			if err := writer.Write(int64(i), payload); err != nil {
				_ = writer.Close()
				b.Fatalf("seed block %d: %v", i, err)
			}
		}
		if err := writer.Sync(); err != nil {
			_ = writer.Close()
			b.Fatalf("seed sync: %v", err)
		}
		if err := writer.Close(); err != nil {
			b.Fatalf("close writer: %v", err)
		}

		reader, err := blockfs.Open(path, blockfs.Options{BlockSize: blockSize})
		if err != nil {
			b.Fatalf("open reader: %v", err)
		}

		for i, want := range payloads {
			got, err := reader.Read(int64(i))
			if err != nil {
				_ = reader.Close()
				b.Fatalf("read block %d: %v", i, err)
			}
			if !bytes.Equal(got, want) {
				_ = reader.Close()
				b.Fatalf("block %d contents mismatch", i)
			}
		}
		if err := reader.Close(); err != nil {
			b.Fatalf("close reader: %v", err)
		}
	}
}

// benchmarkBlockPayloads builds deterministic benchmark payloads so the read
// and write benchmarks operate on the same block contents every run.
func benchmarkBlockPayloads(totalBlocks int, blockSize int64) [][]byte {
	payloads := make([][]byte, totalBlocks)
	for i := 0; i < totalBlocks; i++ {
		block := make([]byte, blockSize)
		prefix := []byte(fmt.Sprintf("block-%05d", i))
		copy(block, prefix)
		for j := len(prefix); j < len(block); j++ {
			block[j] = byte((i + j) % 251)
		}
		payloads[i] = block
	}

	return payloads
}
