package buckets

import (
	"bucketdb/tests"
	"sync"
	"testing"
)

func TestOpenBuckets(t *testing.T) {
	conf := Config{2, 1_000_000, 2, 100}

	_, err := Open("./test", conf)
	tests.Assert(t, err, nil)
}

func TestRefCount(t *testing.T) {
	var wg sync.WaitGroup

	conf := Config{2, 1_000_000, 2, 100}
	buckets, _ := Open("./test", conf)

	for i:=0;i < 50_000; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			b := buckets.Last()
			buckets.Put(b)
		}()
	}

	wg.Wait()
	tests.Assert(t, buckets.items[1].refCount.Load(), 1)
}
