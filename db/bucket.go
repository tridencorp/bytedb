package db

import (
	"os"
	"path/filepath"
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

	// Keep track of the current number of keys in the bucket.
	KeyCount    uint32
	MaxKeyCount uint32
}

func OpenBucket(filepath string) (*Bucket, error) {
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
	bck := &Bucket{ID:1, Dir: path, file: f}
	return bck, nil;
}

// Write data to bucket.
//
// TODO: Should buckets know about keys and other
// types ? Should they operate only on raw bytes ?
func (bucket *Bucket) Write(data []byte) (int64, int64, error) {
	// We are adding len to atomic value and then deducting it
	// from the result, this should give us space for our data.
	//
	// TODO: file must be truncated first !!! Make sure that we have
	// enough space for data. For truncating we can use write mutex 
	// and try to allocate enough space.
	off := bucket.offset.Add(int64(len(data)))
	off -= int64(len(data))

	// We are using WriteAt because, when carefully
	// handled, it's concurrent-friendly.

	// TODO: handle file truncation here. Make sure that we have 
	// enough space for offset and data.
	size, err := bucket.file.WriteAt(data, off)
	if err != nil {
		return off, int64(size), err
	}

	return off, int64(size), nil
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