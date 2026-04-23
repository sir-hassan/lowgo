package blockfs_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sir-hassan/lowgo/pkg/blockfs"
)

func BenchmarkBlockFSReadWrite(b *testing.B) {
	const (
		totalBlocks = 10_000
		blockSize   = 1024
	)

	payloads := benchmarkBlockPayloads(totalBlocks, blockSize)
	totalBytes := int64(totalBlocks * blockSize)

	b.Run("write", func(b *testing.B) {
		b.SetBytes(totalBytes)
		b.ReportAllocs()

		for b.Loop() {
			path := filepath.Join(b.TempDir(), "data.bin")
			file, err := blockfs.Open(path, blockfs.Options{BlockSize: blockSize})
			if err != nil {
				b.Fatalf("open: %v", err)
			}

			for i, payload := range payloads {
				if err := file.Write(int64(i), payload); err != nil {
					_ = file.Close()
					b.Fatalf("write block %d: %v", i, err)
				}
			}
			if err := file.Sync(); err != nil {
				_ = file.Close()
				b.Fatalf("sync: %v", err)
			}
			if err := file.Close(); err != nil {
				b.Fatalf("close: %v", err)
			}
		}
	})

	b.Run("read", func(b *testing.B) {
		b.SetBytes(totalBytes)
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

			got := make([]byte, blockSize)
			for i, want := range payloads {
				if err := reader.Read(int64(i), got); err != nil {
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
	})
}

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
