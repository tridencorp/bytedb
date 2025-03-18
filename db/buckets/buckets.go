package buckets

import (
	"sync/atomic"
)

type Config struct {
	MaxKeys   uint32
	MaxSize   int64
	MaxPerDir int32
	MaxOpened int16
}

type item struct {
	bucket   *Bucket
	refCount atomic.Int32
}

func Item(b *Bucket) *item {
	item := &item{ bucket: b }
	item.refCount.Store(1)

	return item
}

type Buckets struct {
	items map[uint32]*item

	// Max opened buckets at a time.
	MaxOpened int16

	// Last bucket is special, all new keys go into it.
	last atomic.Pointer[item]
}

// Open latest bucket.
func Open(root string, conf Config) (*Buckets, error) {
	bucket, err := OpenBucket(root, conf)
	if err != nil {
		return nil, err
	}

	buckets := &Buckets{ MaxOpened: conf.MaxOpened, items: map[uint32]*item{} }
	item := Item(bucket)

	buckets.last.Store(item)
	buckets.items[bucket.ID] = item

	return buckets, nil
}

func (b *Buckets) Last() *Bucket {
	last := b.last.Load()
	last.refCount.Add(1)

	return last.bucket
}

// Put bucket back so it can be reused by other routines.
// In reality we just decrease the refCount so we would know if
// it's safe to close.
func (b *Buckets) Put(bucket *Bucket) {
	item := b.items[bucket.ID]
	item.refCount.Add(-1)
}
