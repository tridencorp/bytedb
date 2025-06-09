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

	tests.AssertEqual(t, (*num * int64(unsafe.Sizeof(key{}))), idx.file.Size())
}

func TestIndexSet(t *testing.T) {
	flag.Parse()

	idx, _ := Open(".index.idx", *num)
	defer os.Remove(".index.idx")

	for i := 0; i < int(*num); i++ {
		key := fmt.Sprintf("key_%d", i)
		idx.Set([]byte(key))
	}
}

// func TestWrites(t *testing.T) {
// 	num := 2_000_000
// 	file, _ := Load("index.idx", uint64(num))
// 	defer os.Remove("./index.idx")

// 	tests.RunConcurrently(1, func() {
// 		for i := 0; i < 2_000_000; i++ {
// 			key := fmt.Sprintf("key_%d_%d", i, time.Now().UnixMicro())
// 			file.Set([]byte(key), 10, 10, 1)
// 		}
// 	})

// 	err := unix.Msync(file.data, unix.MS_SYNC)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println("collisions: ", file.nextCollision.Load())
// }
