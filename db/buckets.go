package db

import "sync/atomic"

type item struct {
	bucket   *Bucket
	refCount atomic.Int32
}

func Item(b *Bucket) *item {
	i := &item{bucket: b}
	i.refCount.Store(1)
	return i
}

type Buckets struct {
	items map[uint32]*item

	// Max opened buckets at a time.
	MaxOpened int16

	// The latest bucket is special, all new keys go into it.
	latest atomic.Pointer[item]
}

// Open latest bucket.
func OpenBuckets(root string, maxFiles int16, conf Config) (*Buckets, error) {
	bucket, err := OpenBucket(root, conf)
	if err != nil {
		return nil, err
	}

	buckets := &Buckets{ MaxOpened: maxFiles, items: map[uint32]*item{} }
	item := Item(bucket)

	buckets.latest.Store(item)
	buckets.items[bucket.ID] = item

	return buckets, nil
}

func (b *Buckets) Latest() *Bucket {
	latest := b.latest.Load()
	latest.refCount.Add(1)

	return latest.bucket
}