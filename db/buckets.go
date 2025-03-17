package db

import "sync/atomic"

type Buckets struct {
	items map[uint32]*Bucket

	// Max opened buckets at a time.
	MaxOpened int16

	// The latest bucket is special, all new keys go into it.
	latest atomic.Pointer[Bucket]
}

// Open latest bucket.
func OpenBuckets(root string, maxFiles int16, conf Config) (*Buckets, error) {
	bucket, err := OpenBucket(root, conf)
	if err != nil {
		return nil, err
	}

	buckets := &Buckets{ MaxOpened: maxFiles }
	buckets.latest.Store(bucket)

	return buckets, nil
}
