package db

import (
	"bytedb/collection"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	PathKeys    = "keys"
	PathBuckets = "buckets"
	PathHashes  = "hashes"
)

type Collection struct {
	name string
	Dir  string

	mu    sync.RWMutex
	Files map[uint64]*os.File
}

// Open collection from disk
func OpenCollection(dir string) *Collection {
	return &Collection{Dir: dir}
}

// Add key-value to collection
func (c *Collection) Add(key *collection.Key, val []byte) {
	_, ok := c.File(key.Prefix)

	// If file is not in cache, load it from disk
	if !ok {
		// ./collection/keys/prefix_hex.kv
		path := filepath.Join(c.Dir, PathKeys, fmt.Sprintf("%x.kv", key.Prefix))
		c.LoadFile(path, key.Prefix)
	}
}

// Get key from memory
func (c *Collection) File(hash uint64) (*os.File, bool) {
	c.mu.RLock()
	val, ok := c.Files[hash]
	c.mu.RUnlock()

	return val, ok
}

// Load file from disk. Create file if it doesn't exist.
func (c *Collection) LoadFile(path string, hash uint64) error {
	f, err := OpenFile(path)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.Files[hash] = f
	c.mu.Unlock()

	return nil
}
