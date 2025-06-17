package buckets

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

var ErrMaxKeys = errors.New("Bucket key limit reached")

type Bucket struct {
	file *os.File
	Dir  string

	// In combination with WriteAt, it should give
	// us the ultimate concurrent writes.
	offset atomic.Int64

	// If offset reach size limit, we resize the file.
	// We double it's size.
	// TODO: will be changed.
	sizeLimit uint64

	// Bucket ID.
	ID uint32

	TestOffset atomic.Int64

	// Keeping track how many times we resize bucket.
	ResizeCount uint32

	// Number of bucket files per directory.
	bucketsPerDir int16

	// Keep track of the number of keys in the bucket.
	//
	// TODO: This should also go to File. It's tracking
	// number of keys per file so it would make sense to do it.
	keysCount atomic.Int64
	keysLimit uint64

	// Mutex.
	mux sync.RWMutex
}

func OpenBucket(root string, conf Config) (*Bucket, error) {
	file, err := Last(root)
	if err != nil {
		return nil, err
	}

	// TODO: Temporary values untill we have proper bucket management.[]
	bucket := &Bucket{
		ID:            1,
		file:          file,
		Dir:           root,
		keysLimit:     uint64(conf.MaxKeys),
		sizeLimit:     uint64(conf.MaxSize),
		ResizeCount:   0,
		bucketsPerDir: int16(conf.MaxPerDir),
	}

	bucket.offset.Store(getOffset(bucket))
	bucket.TestOffset.Store(0)

	return bucket, nil
}

// Write data to bucket.
//
// TODO: Should buckets know about keys and other
// types ? Should they operate only on raw bytes ?
//
// TODO: We could return Offset{} here.
func (b *Bucket) Write(data []byte) (int64, int64, uint32, error) {
	count := b.keysCount.Add(1)
	limit := int64(b.keysLimit)

	if count >= limit {
		return 0, 0, 0, ErrMaxKeys
	}

	// We are adding len to atomic value and then deducting it
	// from the result, this should give us space for our data.
	offset := b.offset.Add(int64(len(data)))
	keyOffset := offset - int64(len(data))

	off := int64(0)
	size := int64(0)

	// Resize the file when we reach size limit.
	if offset >= int64(b.sizeLimit) {
		b.mux.Lock()
		// Check if our condition is still valid - some other goroutine
		// could changed the size limit in the time we was waiting for lock.
		if offset >= int64(b.sizeLimit) {
			err := b.resize()
			if err != nil {
				return 0, 0, 0, err
			}
		}
		b.mux.Unlock()
	}

	if count <= limit {
		// b.mux.RLock()
		off, size, _ = b.write(b.file, keyOffset, data)
		// b.mux.RUnlock()
	}

	return off, size, b.ID, nil
}

func (bucket *Bucket) resize() error {
	bucket.sizeLimit += bucket.sizeLimit
	err := bucket.file.Truncate(int64(bucket.sizeLimit))
	bucket.ResizeCount += 1
	return err
}

// Getting last offset from which we can start writing data.
// For now we just do it dead simple, read file from beginning
// record by record till end of data.
// It would basically be done only for last block - the one we are currently writing to.
// Other blocks will be immutable (so no offset needed).
func getOffset(bucket *Bucket) int64 {
	it := Iterator{Bucket: bucket}
	_, size, _ := it.Iterate()
	return size
}

func (b *Bucket) write(file *os.File, off int64, data []byte) (int64, int64, error) {
	// We are using WriteAt because, when carefully
	// handled, it's concurrent-friendly.
	size, err := file.WriteAt(data, off)
	if err != nil {
		return off, int64(size), err
	}

	return off, int64(size), nil
}

// Read data from bucket.
func (b *Bucket) Read(offset int64, size int64) ([]byte, error) {
	data := make([]byte, size)

	_, err := b.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	return data, nil
}
