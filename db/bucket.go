package db

import (
	"fmt"
	"os"
	"path/filepath"
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

	// Keep track of the number of keys in the bucket.
	numOfKeys atomic.Int64
	keysLimit uint64

	// If offset reach size limit, we resize the file.
	// We double it's size.
	sizeLimit uint64

	// Mutex
	mux sync.RWMutex
}

func OpenBucket(filepath string, keysLimit uint32, sizeLimit int64) (*Bucket, error) {
	// Make sure that the filepath exists.
	path, err := createPath(filepath)
	if err != nil {
		return nil, err
	}

	// Open bucket file.
	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// TODO: Temporary values untill we have proper bucket management.
	bck := &Bucket{ID:1, Dir: path, file: f, sizeLimit: uint64(sizeLimit)}
	return bck, nil;
}

// Find the last bucket ID for given root.
// We return 0 if no buckets were found.
func findLastBucket(root string) (int, string) {
	// List bucket directories first.
	dirs, err := os.ReadDir(root)
	if err != nil {
		return 0, ""
	}

	if len(dirs) == 0 {
		return 0, ""
	}

	// Sort directories.
	max  := 1
	path := root
	for _, dir := range dirs {
		num, _ := strconv.Atoi(dir.Name())
		if num > max {
			max = num;
			path += "/" + dir.Name()
		}
	}

	// Get the last bucket id from directory.
	files, err := os.ReadDir(fmt.Sprintf("%s/%d/", root, max))
	if err != nil {
		return 0, ""
	}

	for _, file := range files {
		id := strings.Split(file.Name(), ".")[0]
		num, _ := strconv.Atoi(id)
		if num > max {
			max = num
			path += "/" + file.Name()
		}
	}

	return max, path
}

// Write data to bucket.
//
// TODO: Should buckets know about keys and other
// types ? Should they operate only on raw bytes ?
func (bucket *Bucket) Write(data []byte) (int64, int64, error) {
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

	bucket.numOfKeys.Add(1)
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

// Creating path.
func createPath(path string) (string, error) {
	dir := filepath.Dir(path)

	// Create directory structure. Do nothing if it already exist.
	if err := os.MkdirAll(dir, 0755)
	err != nil {
		return "", err
	}

	return dir, nil
}
