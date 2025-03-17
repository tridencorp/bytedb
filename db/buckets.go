package db

import "sync/atomic"

type item struct {
	bucket   *Bucket
	refCount atomic.Int32
}

func Item(b *Bucket) *item {
	item := &item{bucket: b}
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
func OpenBuckets(root string, conf Config) (*Buckets, error) {
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
