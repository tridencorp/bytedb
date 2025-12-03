package db

import (
	"sync"
)

const (
	DirKeys    = "keys"
	DirBuckets = "buckets"
	DirHashes  = "hashes"
)

type Collection struct {
	name string
	Dir  string

	mu      sync.RWMutex
	Buckets map[uint64]Bucket
}

// Open collection from disk
func OpenCollection(dir string) *Collection {
	return &Collection{Dir: dir}
}

// Load file from disk. Create file if it doesn't exist.
func (c *Collection) LoadFile(path string, hash uint64) (*File, error) {
	f, err := OpenFile(path)
	if err != nil {
		return nil, err
	}

	return f, nil
}
