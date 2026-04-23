# lowgo

![lowgo logo](assets/lowgo-logo.svg)

`lowgo` is a small collection of Go packages for low-level storage and message
routing primitives.

The repo is intentionally package-oriented. Each package is meant to be usable
on its own, with explicit APIs and minimal framework code.

## Packages

### `pkg/blockfs`

Fixed-size block I/O over regular files.

What it provides:
- Open a file as a block-addressable store.
- Read and write blocks by index.
- Sync writes to disk.
- Optionally wrap the file with an in-memory cache.

Main APIs:
- `blockfs.Open(path, opts)`
- `blockfs.OpenCached(path, opts)`
- `blockfs.Cache(file)`

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

	got, err := f.Read(0)
	if err != nil {
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

## Repository Notes

- The module path is `github.com/sir-hassan/lowgo`.
- Packages live under `pkg/`.
- The repo logo lives at `assets/lowgo-logo.svg`.
- There is no top-level application in this repo; it is a package library.
