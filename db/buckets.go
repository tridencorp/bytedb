package db

import "sync/atomic"

type Buckets struct {
	items map[uint32]*Bucket

	// The latest bucket is special, all new keys go into it.
	latest atomic.Pointer[Bucket]
}
