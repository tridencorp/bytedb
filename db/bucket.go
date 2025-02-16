package db

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Bucket struct {
	ID 		uint32
	Dir   string
	file *os.File

	// We will be using atomic.Add() for each key.
	// In combination with WriteAt, it should give
	// us the ultimate concurrent writes.
	offset atomic.Int64

	// Number of bucket files per directory.
	bucketsPerDir int16

	// Keep track of the number of keys in the bucket.
	keysCount atomic.Int64
	keysLimit uint64

	// If offset reach size limit, we resize the file.
	// We double it's size.
	sizeLimit uint64

	// Mutex
	mux sync.RWMutex
}

func OpenBucket(root string, keysLimit uint32, sizeLimit int64, bucketsPerDir int32) (*Bucket, error) {
	f, err := getLastBucket(root)
	if err != nil {
		return nil, err
	}

	// TODO: Temporary values untill we have proper bucket management.
	bck := &Bucket{ID:1, Dir: root, file: f, sizeLimit: uint64(sizeLimit)}
	return bck, nil;
}

// Find the last bucket ID for given root.
// Empty string in response means that there is no bucket yet.
func getLastBucket(root string) (*os.File, error) {
	// Sort directories.
	dirs, _ := os.ReadDir(root)
	max := 0

	for _, dir := range dirs {
		id, _ := strconv.Atoi(dir.Name())
		if id > max { max = id }
	}

	// Directory is empty, no buckets yet, so we have to create first one.
	if max == 0 {
		root += "/1/"
		os.MkdirAll(root, 0755)

		root += "1.bucket"
		file, err := os.OpenFile(root, os.O_RDWR|os.O_CREATE, 0644)
		return file, err
	}

	// Sort files.
	root += fmt.Sprintf("/%d", max)
	files, _ := os.ReadDir(root)

	for _, file := range files {
		// Split .bucket file.
		fileId := strings.Split(file.Name(), ".")[0]

		id, _ := strconv.Atoi(fileId) 
		if id > max { max = id }
	}

	root += fmt.Sprintf("/%d.bucket", max)
	file, err := os.OpenFile(root, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// Create next bucket.
func (bucket *Bucket) nextBucket() (*os.File, error) {
	id := bucket.ID + 1

	// Based on buckets per dir we can calculate folder ID in which
	// bucket should be.
	folderId := int(math.Ceil(float64(id) / float64(bucket.bucketsPerDir)))

	path := fmt.Sprintf("%d/", folderId)
	err  := os.MkdirAll(bucket.Dir + path, 0755)
	if err != nil {
		return nil, err
	}

	path = fmt.Sprintf("%d/%d.bucket", folderId, id)
	file, err := os.OpenFile(bucket.Dir + path, os.O_RDWR|os.O_CREATE, 0644)

	bucket.ID   = id
	bucket.file = file

	return file, err
}

// Write data to bucket.
//
// TODO: Should buckets know about keys and other
// types ? Should they operate only on raw bytes ?
func (bucket *Bucket) Write(data []byte) (int64, int64, error) {
	bucket.keysCount.Add(1)

	// We are adding len to atomic value and then deducting it
	// from the result, this should give us space for our data.
	totalOff := bucket.offset.Add(int64(len(data)))
	writeOff := totalOff - int64(len(data))

	// Resize the file when we reach size limit.
	if totalOff >= int64(bucket.sizeLimit) {
		bucket.mux.Lock()

		// Check if our condition is still valid - some other goroutine 
		// could changed the size limit in the time we was waiting for lock.
		if totalOff >= int64(bucket.sizeLimit) {
			bucket.sizeLimit *= 2
			bucket.file.Truncate(int64(bucket.sizeLimit))
		}

		bucket.mux.Unlock()
	}

	// We are using WriteAt because, when carefully
	// handled, it's concurrent-friendly.
	size, err := bucket.file.WriteAt(data, writeOff)
	if err != nil {
		return writeOff, int64(size), err
	}

	return writeOff, int64(size), nil
}

// Read data from bucket.
func (bucket *Bucket) Read(offset int64, size int64) ([]byte, error) {
	data := make([]byte, size)

	_, err := bucket.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	return data, nil
}
