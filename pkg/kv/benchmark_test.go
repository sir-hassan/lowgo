package kv_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sir-hassan/lowgo/pkg/kv"
)

func BenchmarkStore(b *testing.B) {
	for _, storeType := range []kv.Type{kv.TypeLL, kv.TypeBPT} {
		for _, valueSize := range []int{64, 4096} {
			b.Run(fmt.Sprintf("%s/set/value_%d", storeType, valueSize), func(b *testing.B) {
				const totalKeys = 2_000

				keys, values := benchmarkPayloads(totalKeys, valueSize)
				b.SetBytes(int64(totalKeys * valueSize))
				b.ReportAllocs()

				for b.Loop() {
					path := filepath.Join(b.TempDir(), "data.kv")
					store, err := kv.Open(path, kv.Options{
						BlockSize:   1024,
						BucketCount: 256,
						Type:        storeType,
					})
					if err != nil {
						b.Fatalf("open kv: %v", err)
					}

					for i := range keys {
						if err := store.Set(keys[i], values[i]); err != nil {
							_ = store.Close()
							b.Fatalf("set key %d: %v", i, err)
						}
					}
					if err := store.Sync(); err != nil {
						_ = store.Close()
						b.Fatalf("sync: %v", err)
					}
					if err := store.Close(); err != nil {
						b.Fatalf("close: %v", err)
					}
				}
			})

			b.Run(fmt.Sprintf("%s/get/value_%d", storeType, valueSize), func(b *testing.B) {
				const totalKeys = 2_000

				keys, values := benchmarkPayloads(totalKeys, valueSize)
				path := filepath.Join(b.TempDir(), "data.kv")

				writer, err := kv.Open(path, kv.Options{
					BlockSize:   1024,
					BucketCount: 256,
					Type:        storeType,
				})
				if err != nil {
					b.Fatalf("open kv writer: %v", err)
				}
				for i := range keys {
					if err := writer.Set(keys[i], values[i]); err != nil {
						_ = writer.Close()
						b.Fatalf("seed key %d: %v", i, err)
					}
				}
				if err := writer.Sync(); err != nil {
					_ = writer.Close()
					b.Fatalf("seed sync: %v", err)
				}
				if err := writer.Close(); err != nil {
					b.Fatalf("close kv writer: %v", err)
				}

				reader, err := kv.Open(path, kv.Options{
					BlockSize: 1024,
					Type:      storeType,
				})
				if err != nil {
					b.Fatalf("open kv reader: %v", err)
				}
				defer func() {
					_ = reader.Close()
				}()

				b.SetBytes(int64(totalKeys * valueSize))
				b.ReportAllocs()

				for b.Loop() {
					for i := range keys {
						got, err := reader.Get(keys[i])
						if err != nil {
							b.Fatalf("get key %d: %v", i, err)
						}
						if !bytes.Equal(got, values[i]) {
							b.Fatalf("key %d contents mismatch", i)
						}
					}
				}
			})
		}
	}
}

func benchmarkPayloads(totalKeys int, valueSize int) ([][]byte, [][]byte) {
	keys := make([][]byte, totalKeys)
	values := make([][]byte, totalKeys)

	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := make([]byte, valueSize)
		prefix := []byte(fmt.Sprintf("value-%05d", i))
		copy(value, prefix)
		for j := len(prefix); j < len(value); j++ {
			value[j] = byte((i + j) % 251)
		}

		keys[i] = key
		values[i] = value
	}

	return keys, values
}
