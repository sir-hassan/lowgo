# lowgo

`pkg/blockfs` provides fixed-size block reads and writes over regular files.

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
