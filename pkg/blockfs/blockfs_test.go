package blockfs_test

import (
	"bytes"
	"encoding/binary"
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

func writeTestHeader(t *testing.T, path string, blockSize int64, nextIndex int64) {
	t.Helper()

	buf := make([]byte, 4*1024)
	copy(buf[:8], []byte{'B', 'L', 'K', 'F', 'S', 0x02, 0x00, 0x00})
	binary.LittleEndian.PutUint64(buf[8:16], uint64(blockSize))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(nextIndex))
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("write header: %v", err)
	}
}

// TestOpenUsesDefaultBlockSize verifies that opening a block file without an
// explicit block size uses the default 4 KiB block size.
func TestOpenUsesDefaultBlockSize(t *testing.T) {
	t.Parallel()

	bf, err := blockfs.Open(filepath.Join(t.TempDir(), "data.bin"), blockfs.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	if bf.Size() != 4*1024 {
		t.Fatalf("expected default block size 4096, got %d", bf.Size())
	}
}

// TestOpenRejectsNegativeBlockSize verifies that opening a block file with a
// negative block size fails with ErrInvalidBlockSize.
func TestOpenRejectsNegativeBlockSize(t *testing.T) {
	t.Parallel()

	_, err := blockfs.Open(filepath.Join(t.TempDir(), "data.bin"), blockfs.Options{BlockSize: -1})
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

	got := make([]byte, bf.Size())
	if err := bf.Read(2, got); err != nil {
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

	got := make([]byte, bf.Size())
	if err := bf.Read(8, got); err != nil {
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
	writeTestHeader(t, path, 8, 1)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open raw file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.WriteAt([]byte("hello"), 4*1024); err != nil {
		t.Fatalf("write partial block: %v", err)
	}

	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 8})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	got := make([]byte, bf.Size())
	if err := bf.Read(0, got); err != nil {
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
		if err := bf.Read(index, make([]byte, bf.Size())); !errors.Is(err, blockfs.ErrInvalidBlockIndex) {
			t.Fatalf("expected ErrInvalidBlockIndex for index %d, got %v", index, err)
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
		if err := bf.Write(index, block); !errors.Is(err, blockfs.ErrInvalidBlockIndex) {
			t.Fatalf("expected ErrInvalidBlockIndex for index %d, got %v", index, err)
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
	if err := bf.Read(0, make([]byte, bf.Size())); !errors.Is(err, blockfs.ErrClosed) {
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
	if info.Size() != 4*1024+2*writer.Size() {
		t.Fatalf("expected file size %d, got %d", 4*1024+2*writer.Size(), info.Size())
	}

	reader, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() {
		_ = reader.Close()
	})

	got := make([]byte, reader.Size())
	if err := reader.Read(1, got); err != nil {
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
	if info.Size() != 4*1024+int64(totalBlocks)*bf.Size() {
		t.Fatalf("expected file size %d, got %d", 4*1024+int64(totalBlocks)*bf.Size(), info.Size())
	}

	for i, want := range wantBlocks {
		got := make([]byte, bf.Size())
		if err := bf.Read(int64(i), got); err != nil {
			t.Fatalf("read block %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("block %d contents mismatch", i)
		}
	}
}

// TestWriteAndRead verifies repeated round-trip
// behavior by writing and reading 10 blocks across 100 iterations.
func TestWriteAndRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.bin")
	bf, err := blockfs.Open(path, blockfs.Options{BlockSize: 256})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = bf.Close()
	})

	const (
		totalBlocks = 10
		iterations  = 100
	)

	block := make([]byte, bf.Size())
	readBuf := make([]byte, bf.Size())
	allocs := testing.AllocsPerRun(5, func() {
		for iter := 0; iter < iterations; iter++ {
			for i := 0; i < totalBlocks; i++ {
				block[0] = byte(iter)
				block[1] = byte(i)
				if err := bf.Write(int64(i), block); err != nil {
					t.Fatalf("iteration %d write block %d: %v", iter, i, err)
				}
				if err := bf.Read(int64(i), readBuf); err != nil {
					t.Fatalf("iteration %d read block %d: %v", iter, i, err)
				}
				if readBuf[0] != block[0] || readBuf[1] != block[1] {
					t.Fatalf("iteration %d read block %d: expected [%d %d], got [%d %d]", iter, i, block[0], block[1], readBuf[0], readBuf[1])
				}
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("expected 0, got %d", int(allocs))
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
		got := make([]byte, bf.Size())
		if err := bf.Read(index, got); err != nil {
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

func TestOpenRejectsMismatchedBlockSizeFromHeader(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.bin")
	writer, err := blockfs.Open(path, blockfs.Options{BlockSize: 4096})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	_, err = blockfs.Open(path, blockfs.Options{BlockSize: 1024})
	if !errors.Is(err, blockfs.ErrBlockSizeMismatch) {
		t.Fatalf("expected ErrBlockSizeMismatch, got %v", err)
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
		st := make([]byte, bf.Size())
		if err := bf.Read(int64(i), st); err != nil {
			t.Fatalf("read block %d: %v", i, err)
		}
		got += string(st)
	}
	want := "00000000111111112222222233333333444444445555555566666666777777778888888899999999"
	if got != want {
		t.Fatalf("expected file content %s, got %s", want, got)
	}
}
