package db

import (
	"sync"
)

const (
	ExtBucket = ".bck"
	ExtHash   = ".hsh"
	ExtArray  = ".arr"
)

type Collection struct {
	Hash uint64
	Path string

	mu      sync.RWMutex
	Buckets map[uint64]Bucket
}

// Open collection from disk. Create it if necessary.
func OpenCollection(hash uint64, path string) *Collection {
	return &Collection{Hash: hash, Path: path}
}

// Load file from disk. Create file if it doesn't exist.
func (c *Collection) LoadFile(path string, hash uint64) (*File, error) {
	f, err := OpenFile(path)
	if err != nil {
		return nil, err
	}

	return f, nil
}
