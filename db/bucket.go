package db

import (
	"os"
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
}

func OpenBucket(file string) (*Bucket, error) {
	// Open bucket file.
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// TODO: Temporary values untill we have proper bucket management.
	bck := &Bucket{ID:1, Dir: "", file: f}
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
