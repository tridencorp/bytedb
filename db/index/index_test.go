package index

import (
	"bucketdb/tests"
	"flag"
	"fmt"
	"os"
	"testing"
	"unsafe"
)

// TODO: Move this to test setup
var num = flag.Int64("num", 100_000, "number of iterations")

func TestIndexPrealloc(t *testing.T) {
	flag.Parse()

	idx, _ := Open(".index.idx", *num)
	defer os.Remove(".index.idx")

	prealloc := *num * int64(unsafe.Sizeof(key{}))
	tests.AssertEqual(t, prealloc, idx.file.Size())
}

func TestIndexSetGet(t *testing.T) {
	flag.Parse()

	idx, _ := Open(".index.idx", *num)
	defer os.Remove(".index.idx")

	for i := 0; i < int(*num); i++ {
		key := fmt.Sprintf("key_%d", i)
		idx.Set([]byte(key))
	}

	for i := 0; i < int(*num); i++ {
		key := fmt.Sprintf("key_%d", i)
		idx.Get([]byte(key))
	}
}
