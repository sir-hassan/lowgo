# lowgo

![lowgo logo](assets/lowgo-logo.svg)

`lowgo` is a small collection of Go packages for low-level storage and message
routing primitives.

The repo is intentionally package-oriented. Each package is meant to be usable
on its own, with explicit APIs and minimal framework code.

## Packages

### `pkg/blockfs`

Fixed-size block I/O over regular files.

The on-disk format reserves a fixed 4 KiB header region for metadata. That
header stores the configured block size and the next logical block index, and
leaves the remaining bytes reserved for future format changes. User data begins
immediately after that 4 KiB header region.

What it provides:
- Open a file as a block-addressable store.
- Read and write blocks by index.
- Sync writes to disk.

Main APIs:
- `blockfs.Open(path, opts)`

Example:

```go
package main

import (
	"log"

	"github.com/sir-hassan/lowgo/pkg/blockfs"
)

func main() {
	f, err := blockfs.Open("data.bin", blockfs.Options{BlockSize: 4 * 1024})
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	block := make([]byte, f.Size())
	copy(block, []byte("hello"))

	if err := f.Write(0, block); err != nil {
		log.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		log.Fatal(err)
	}

	got := make([]byte, f.Size())
	if err := f.Read(0, got); err != nil {
		log.Fatal(err)
	}

	_ = got
}
```

### `pkg/barid`

An in-memory frame router built around source and drain pipes.

What it provides:
- Register a set of source pipes and drain pipes.
- Route frames by `Frame.Code`.
- Drop frames whose code has no matching drain.
- Run synchronously with `Run()` or asynchronously with `Start()` and `Wait()`.

Main APIs:
- `barid.New(sources, drains, frameBufferSize)`
- `(*barid.Router).Run()`
- `(*barid.Router).Start()`
- `(*barid.Router).Wait()`

Example:

```go
package main

import "github.com/sir-hassan/lowgo/pkg/barid"

func main() {
	source := make(chan barid.Frame, 1)
	drain := make(chan barid.Frame, 1)

	router, err := barid.New(
		[]barid.Pipe{{Code: 0xAA, Channel: source}},
		[]barid.Pipe{{Code: 0xAA, Channel: drain}},
		0,
	)
	if err != nil {
		panic(err)
	}

	router.Start()
	source <- barid.Frame{Code: 0xAA, Bytes: []byte("hello")}
	close(source)
	router.Wait()
}
```

### `pkg/kv`

Persistent key/value storage built on top of `pkg/blockfs`.

What it provides:
- A package-level `Store` interface for persistent KV backends.
- `Open(path, opts)` as a type-dispatching constructor.
- `OpenLinkedList(path, opts)` as the linked-list-backed implementation.
- Variable-length values encoded across chained payload blocks.

Main APIs:
- `kv.Open(path, opts)`
- `kv.OpenLinkedList(path, opts)`
- `kv.Store`

Example:

```go
package main

import (
	"log"

	"github.com/sir-hassan/lowgo/pkg/kv"
)

func main() {
	store, err := kv.Open("data.kv", kv.Options{
		BlockSize:   4 * 1024,
		BucketCount: 256,
		Type:        kv.TypeLinkedList,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	if err := store.Set([]byte("name"), []byte("lowgo")); err != nil {
		log.Fatal(err)
	}

	value, err := store.Get([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}

	_ = value
}
```

## Repository Notes

- The module path is `github.com/sir-hassan/lowgo`.
- Packages live under `pkg/`.
- The repo logo lives at `assets/lowgo-logo.svg`.
- There is no top-level application in this repo; it is a package library.
