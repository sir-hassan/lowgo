# lowgo

`lowgo` is a Go library collection for lower-level systems constructs.

The project is meant to hold small, focused packages that expose primitives
useful when building storage engines, filesystems, caches, and other
infrastructure code. The emphasis is on simple APIs, explicit behavior, and
low-level building blocks rather than full application frameworks.

`pkg/blockfs` is one package inside `lowgo`. It provides fixed-size block reads
and writes over regular files.

## Packages

- `pkg/blockfs`: fixed-size block file access with an optional in-memory cache

## blockfs example

```go
package main

import (
	"log"

	"github.com/sir-hassan/lowgo/pkg/blockfs"
)

func main() {
	bf, err := blockfs.Open("data.bin", blockfs.Options{BlockSize: 4 * 1024})
	if err != nil {
		log.Fatal(err)
	}
	defer bf.Close()

	block := make([]byte, bf.BlockSize())
	copy(block, []byte("hello"))

	if err := bf.WriteBlock(0, block); err != nil {
		log.Fatal(err)
	}
	if err := bf.Sync(); err != nil {
		log.Fatal(err)
	}

	got, err := bf.ReadBlock(0)
	if err != nil {
		log.Fatal(err)
	}

	_ = got
}
```

For repeated reads of the same blocks, wrap the file-backed implementation with
the in-memory cache:

```go
bf, err := blockfs.OpenCached("data.bin", blockfs.Options{BlockSize: 4 * 1024})
if err != nil {
	log.Fatal(err)
}
defer bf.Close()
```
