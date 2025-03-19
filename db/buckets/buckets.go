package buckets

import (
	"bucketdb/db/utils"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

type Config struct {
	MaxKeys   uint32 // Max keys per bucket
	MaxSize   int64  // Max bucket size after which we will resize
	MaxPerDir int32  // Max buckets per directory
	MaxOpened int16  // Max opened buckets at a time
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
	mux   sync.RWMutex
	items map[uint32]*item
	Root  string

	MaxOpened int16
	MaxPerDir int32

	// Last bucket is special, all new keys go into it.
	last atomic.Pointer[item]
}

// Open buckets directory and initialize it with last bucket - create one if we don't have any.
func Open(root string, conf Config) (*Buckets, error) {
	bucket, err := OpenBucket(root, conf)
	if err != nil {
		return nil, err
	}

	buckets := &Buckets{
		items: 		 map[uint32]*item{},
		Root: 		 root,
		MaxOpened: conf.MaxOpened,
		MaxPerDir: conf.MaxPerDir,
	}

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

// Return path for bucket id.
func (b *Buckets) Path(id uint32) string {
	folder := int(math.Ceil(float64(id) / float64(b.MaxPerDir)))
	return fmt.Sprintf("%s/%d/%d.bucket", b.Root, folder, id)
}

// Open/Create bucket with given id.
func (b *Buckets) Open(id uint32) (*Bucket, error) {
	file, err := utils.OpenPath(b.Path(id))
	if err != nil {
		return nil, err
	}

	bucket := &Bucket{ ID: id, file: file }
	bucket.offset.Store(0)
	bucket.keysCount.Store(0)
	bucket.ResizeCount = 0

	return bucket, nil
}

// Add bucket to items - keep it in memory.
func (b *Buckets) Add(id uint32) *item {
	b.mux.Lock()
	defer b.mux.Unlock()

	// Check if some other goroutine didn't already add our bucket.
	item, exists := b.items[id]
	if exists {
		item.refCount.Add(1)
		return item
	}

	bucket, _ := b.Open(id)
	b.items[bucket.ID] = Item(bucket)

	return b.items[bucket.ID]
}

// Get a bucket by ID. If a bucket with the given ID
// is not already opened, we will try to open it.
func (b *Buckets) Get(id uint32) *Bucket {
	b.mux.RLock()
	item, exists := b.items[id]
	b.mux.RUnlock()

	if exists {
		item.refCount.Add(1)
		return item.bucket
	}

	// Bucket is not opened yet, add it.
	item = b.Add(id)
	return item.bucket 
}

// Put bucket back so it can be reused by other routines.
// In reality we just decrease the refCount so we would know if
// it's safe to close.
func (b *Buckets) Put(bucket *Bucket) {
	b.mux.RLock()
	defer b.mux.RUnlock()

	item := b.items[bucket.ID]
	item.refCount.Add(-1)
}
