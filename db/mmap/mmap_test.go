package mmap

import (
	"bucketdb/tests"
	"fmt"
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	file, _ := os.OpenFile("./test.wal", os.O_RDWR | os.O_CREATE, 0644)
	defer os.Remove("./test.wal")

	size := int64(1024 * 1024 * 1)
	file.Truncate(size)

	mmap, err := Open(file, int(size), 0)
	if err != nil {
		fmt.Println(err)
	}

	mmap.Resize(1_000)
	tests.Assert(t, len(mmap.data), 1_000)
}