package mmap

import (
	"bytedb/tests"
	"fmt"
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	file, _ := os.OpenFile("./test.wal", os.O_RDWR|os.O_CREATE, 0644)
	defer os.Remove("./test.wal")

	size := int64(1024 * 1024 * 1)
	file.Truncate(size)

	mmap, err := Open(file, int(size), 1, 0)
	if err != nil {
		fmt.Println(err)
	}

	mmap.Resize(1_000)
	tests.Assert(t, len(mmap.data), 1_000)
}

func TestWriteRead(t *testing.T) {
	file, _ := os.OpenFile("./test.wal", os.O_RDWR|os.O_CREATE, 0644)
	defer os.Remove("./test.wal")

	size := int64(1024 * 1024 * 1)
	file.Truncate(size)

	mmap, err := Open(file, int(size), 1, 0)
	if err != nil {
		fmt.Println(err)
	}

	data1 := []byte("Hello Wal ❤️")
	mmap.Write(data1)

	data2 := []byte("Hello Wal 😱")
	mmap.Write(data2)

	res1, _ := mmap.Read(0, 1)
	res2, _ := mmap.Read(0, 1)

	tests.AssertEqual(t, string(data1), string(res1))
	tests.AssertEqual(t, string(data2), string(res2))
}
