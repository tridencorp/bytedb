package db

import (
	"bucketdb/tests"
	"flag"
	"fmt"
	"os"
	"testing"
)

// TODO: Move this to test setup
var num = flag.Int64("num", 100_000, "number of iterations")

func TestIndexPrealloc(t *testing.T) {
	flag.Parse()

	i, _ := OpenIndex(".index.idx", *num)
	defer os.Remove(".index.idx")

	prealloc := int64(2080000) // keys + collisions
	tests.AssertEqual(t, prealloc, i.file.Size())
}

func TestIndexSetGet(t *testing.T) {
	flag.Parse()

	idx, _ := OpenIndex(".index.idx", *num)
	defer os.Remove(".index.idx")

	for i := 0; i < int(*num); i++ {
		key := fmt.Sprintf("key_%d", i)
		off := &Offset{Start: 0, Size: 10}
		err := idx.Set([]byte(key), off)
		tests.Assert(t, nil, err)
	}

	for i := 0; i < int(*num); i++ {
		key := fmt.Sprintf("key_%d", i)
		val, _ := idx.Get([]byte(key))
		tests.Assert(t, key, string(val))
	}
}
