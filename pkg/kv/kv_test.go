package kv_test

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sir-hassan/lowgo/pkg/blockfs"
	"github.com/sir-hassan/lowgo/pkg/kv"
)

func TestRoundTripVariableValueAcrossBlocks(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.kv")
	store, err := kv.Open(path, kv.Options{
		BlockSize:   128,
		BucketCount: 8,
		Type:        kv.TypeLinkedList,
	})
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}

	key := []byte("alpha")
	value := bytes.Repeat([]byte("payload-"), 40)

	if err := store.Set(key, value); err != nil {
		_ = store.Close()
		t.Fatalf("set: %v", err)
	}
	if err := store.Sync(); err != nil {
		_ = store.Close()
		t.Fatalf("sync: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader, err := kv.Open(path, kv.Options{
		BlockSize: 128,
		Type:      kv.TypeLinkedList,
	})
	if err != nil {
		t.Fatalf("reopen kv: %v", err)
	}
	t.Cleanup(func() {
		_ = reader.Close()
	})

	got, err := reader.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatal("value mismatch after reopen")
	}
}

func TestLinkedListCollisionsDeleteAndReinsert(t *testing.T) {
	t.Parallel()

	store, err := kv.OpenLinkedList(filepath.Join(t.TempDir(), "data.kv"), kv.Options{
		BlockSize:   128,
		BucketCount: 1,
	})
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	keyA := []byte("alpha")
	keyB := []byte("beta")

	if err := store.Set(keyA, []byte("v1")); err != nil {
		t.Fatalf("set alpha v1: %v", err)
	}
	if err := store.Set(keyB, []byte("v2")); err != nil {
		t.Fatalf("set beta v2: %v", err)
	}
	if err := store.Set(keyA, []byte("v3")); err != nil {
		t.Fatalf("set alpha v3: %v", err)
	}

	gotA, err := store.Get(keyA)
	if err != nil {
		t.Fatalf("get alpha: %v", err)
	}
	if string(gotA) != "v3" {
		t.Fatalf("expected latest alpha value, got %q", gotA)
	}

	if err := store.Delete(keyB); err != nil {
		t.Fatalf("delete beta: %v", err)
	}
	if _, err := store.Get(keyB); !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	if err := store.Set(keyB, []byte("v4")); err != nil {
		t.Fatalf("set beta v4: %v", err)
	}
	gotB, err := store.Get(keyB)
	if err != nil {
		t.Fatalf("get beta: %v", err)
	}
	if string(gotB) != "v4" {
		t.Fatalf("expected beta value v4, got %q", gotB)
	}
}

func TestHasAndDeleteMissingKey(t *testing.T) {
	t.Parallel()

	store, err := kv.Open(filepath.Join(t.TempDir(), "data.kv"), kv.Options{
		BlockSize:   128,
		BucketCount: 4,
		Type:        kv.TypeLinkedList,
	})
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Delete([]byte("missing")); err != nil {
		t.Fatalf("delete missing key: %v", err)
	}

	has, err := store.Has([]byte("missing"))
	if err != nil {
		t.Fatalf("has missing key: %v", err)
	}
	if has {
		t.Fatal("expected missing key to be absent")
	}
}

func TestRejectsEmptyKey(t *testing.T) {
	t.Parallel()

	store, err := kv.Open(filepath.Join(t.TempDir(), "data.kv"), kv.Options{
		BlockSize:   128,
		BucketCount: 4,
		Type:        kv.TypeLinkedList,
	})
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Set(nil, []byte("value")); !errors.Is(err, kv.ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey from set, got %v", err)
	}
	if _, err := store.Get(nil); !errors.Is(err, kv.ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey from get, got %v", err)
	}
	if err := store.Delete(nil); !errors.Is(err, kv.ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey from delete, got %v", err)
	}
}

func TestRejectsBucketCountMismatch(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.kv")
	store, err := kv.Open(path, kv.Options{
		BlockSize:   128,
		BucketCount: 4,
		Type:        kv.TypeLinkedList,
	})
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close kv: %v", err)
	}

	_, err = kv.Open(path, kv.Options{
		BlockSize:   128,
		BucketCount: 8,
		Type:        kv.TypeLinkedList,
	})
	if !errors.Is(err, kv.ErrBucketCountMismatch) {
		t.Fatalf("expected ErrBucketCountMismatch, got %v", err)
	}
}

func TestRejectsCorruptSuperblock(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data.kv")
	file, err := blockfs.Open(path, blockfs.Options{BlockSize: 128})
	if err != nil {
		t.Fatalf("open raw block file: %v", err)
	}
	block := make([]byte, file.Size())
	copy(block, []byte("not-a-kv-superblock"))
	if err := file.Write(0, block); err != nil {
		_ = file.Close()
		t.Fatalf("write corrupt block: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close raw block file: %v", err)
	}

	_, err = kv.Open(path, kv.Options{
		BlockSize: 128,
		Type:      kv.TypeLinkedList,
	})
	if !errors.Is(err, kv.ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestRejectsInvalidType(t *testing.T) {
	t.Parallel()

	_, err := kv.Open(filepath.Join(t.TempDir(), "data.kv"), kv.Options{
		BlockSize: 128,
		Type:      kv.Type("unknown"),
	})
	if !errors.Is(err, kv.ErrInvalidType) {
		t.Fatalf("expected ErrInvalidType, got %v", err)
	}
}

func TestBPlusTreeRoundTripAcrossSplits(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "tree.kv")
	store, err := kv.Open(path, kv.Options{
		BlockSize: 128,
		Type:      kv.TypeBPlusTree,
	})
	if err != nil {
		t.Fatalf("open b+ tree: %v", err)
	}

	want := make(map[string][]byte)
	for i := 0; i < 48; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := bytes.Repeat([]byte{byte(i)}, 96+i%17)
		want[string(key)] = append([]byte(nil), value...)
		if err := store.Set(key, value); err != nil {
			_ = store.Close()
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := store.Sync(); err != nil {
		_ = store.Close()
		t.Fatalf("sync: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader, err := kv.Open(path, kv.Options{
		BlockSize: 128,
		Type:      kv.TypeBPlusTree,
	})
	if err != nil {
		t.Fatalf("reopen b+ tree: %v", err)
	}
	t.Cleanup(func() {
		_ = reader.Close()
	})

	for key, value := range want {
		got, err := reader.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("value mismatch for %q", key)
		}
	}
}

func TestBPlusTreeDeleteAndUpdate(t *testing.T) {
	t.Parallel()

	store, err := kv.Open(filepath.Join(t.TempDir(), "tree.kv"), kv.Options{
		BlockSize: 128,
		Type:      kv.TypeBPlusTree,
	})
	if err != nil {
		t.Fatalf("open b+ tree: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	key := []byte("alpha")
	if err := store.Set(key, []byte("v1")); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	if err := store.Set(key, []byte("v2")); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("get updated value: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("expected updated value, got %q", got)
	}

	if err := store.Delete(key); err != nil {
		t.Fatalf("delete key: %v", err)
	}
	if _, err := store.Get(key); !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestBPlusTreeRejectsCorruptSuperblock(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "tree.kv")
	file, err := blockfs.Open(path, blockfs.Options{BlockSize: 128})
	if err != nil {
		t.Fatalf("open raw block file: %v", err)
	}
	block := make([]byte, file.Size())
	copy(block, []byte("not-a-bplus-superblock"))
	if err := file.Write(0, block); err != nil {
		_ = file.Close()
		t.Fatalf("write corrupt block: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close raw block file: %v", err)
	}

	_, err = kv.Open(path, kv.Options{
		BlockSize: 128,
		Type:      kv.TypeBPlusTree,
	})
	if !errors.Is(err, kv.ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}
