package db

import (
	"bytedb/collection"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/ethereum/go-ethereum/log"
)

const (
	DirKeys    = "keys"
	DirBuckets = "buckets"
	DirHashes  = "hashes"
)

type Collection struct {
	name string
	Dir  string

	mu    sync.RWMutex
	Files map[uint64]*File
}

// Open collection from disk
func OpenCollection(dir string) *Collection {
	return &Collection{Dir: dir, Files: make(map[uint64]*File, 1000)}
}

// Add key-value to collection
func (c *Collection) Add(key *collection.Key, val []byte) error {
	f, ok := c.File(key.Prefix)

	// Load file from disk if we cannot get it from memory
	if !ok {
		var err error

		// collection/keys/prefix_hex.kv
		file := fmt.Sprintf("%x.kv", key.Prefix)
		path := filepath.Join(c.Dir, DirKeys, file)

		f, err = c.LoadFile(path, key.Prefix)
		if err != nil {
			return err
		}

		err = f.Init()
		if err != nil {
			return err
		}

		f, ok = c.File(key.Prefix)
		if !ok {
			return fmt.Errorf("failed to load file for prefix %x", key.Prefix)
		}
	}

	err := f.WriteKV(key, val)
	log.Error(err.Error())

	return err
}

// Get key from memory
func (c *Collection) File(hash uint64) (*File, bool) {
	c.mu.RLock()
	val, ok := c.Files[hash]
	c.mu.RUnlock()

	return val, ok
}

// Load file from disk. Create file if it doesn't exist.
func (c *Collection) LoadFile(path string, hash uint64) (*File, error) {
	f, err := OpenFile(path)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.Files[hash] = f
	c.mu.Unlock()

	return f, nil
}
