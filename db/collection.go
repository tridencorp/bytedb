package db

import "bytedb/collection"

type Collection struct {
	name  string
	Files map[uint64]File
}

// Add key-value to collection
func (c *Collection) Add(key *collection.Key, val []byte) {
	_, ok := c.File(key.Prefix)

	// if file is not in cache, load it from disk
	if !ok {
		c.LoadFile(key.Prefix)
	}
}

// Get key from memory
func (c *Collection) File(hash uint64) (*File, bool) {
	f, ok := c.Files[hash]
	return &f, ok
}

func (c *Collection) LoadFile(hash uint64) {
}
