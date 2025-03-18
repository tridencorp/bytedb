package buckets

import (
	"fmt"
	"math"
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
	items map[uint32]*item
	Root  string

	MaxOpened int16
	MaxPerDir int32

	// Last bucket is special, all new keys go into it.
	last atomic.Pointer[item]
}

// Open last bucket - create one if there isn't any.
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
	return fmt.Sprintf("%d/%d.bucket", folder, id)
}

// Create bucket with given id.
func (b *Buckets) Create(id int32) (*Bucket, error) {

	// path := filepath.Join(b.Root, fmt.Sprintf("%d", folder))
	// err  := os.MkdirAll(path, 0755)
	// if err != nil {
	// 	return nil, err
	// }

	// path = filepath.Join(b.Root, fmt.Sprintf("%d", folderId), fmt.Sprintf("%d.bucket", id))
	// fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)


	return nil, nil
}

// Get a bucket by ID. If a bucket with the given ID
// is not already open, we will find and open it.
func (b *Buckets) Get(id int) *Bucket {
	return nil
}

// Put bucket back so it can be reused by other routines.
// In reality we just decrease the refCount so we would know if
// it's safe to close.
func (b *Buckets) Put(bucket *Bucket) {
	item := b.items[bucket.ID]
	item.refCount.Add(-1)
}
