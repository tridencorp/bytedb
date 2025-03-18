package buckets

import (
	"bucketdb/tests"
	"os"
	"testing"
)

func TestOpenBuckets(t *testing.T) {
	conf := Config{2, 1_000_000, 2, 100}

	_, err := Open("./test", conf)
	defer os.RemoveAll("./test")

	tests.Assert(t, err, nil)
}

func TestRefCount(t *testing.T) {
	conf := Config{2, 1_000_000, 2, 100}
	buckets, _ := Open("./test", conf)
	defer os.RemoveAll("./test")

	tests.RunConcurrently(50_000, func(){
		b := buckets.Last()
		buckets.Put(b)	
	})

	tests.Assert(t, 1, buckets.items[1].refCount.Load())
}
