package index

import (
	"hash/fnv"
	"os"
)

// Cache keeps block lengths in memory, so we don't have to read
// the entire block before writing.
type Cache struct {
}

type IndexFile struct {
	len       uint64
	blocks    uint64
	file      *os.File
	resizeCap uint64
	Cache     *Cache
}

// Open opens the index file, creating it if it does not exist.
func Open(file *os.File, cap uint64) *IndexFile {
	return &IndexFile{blocks: cap, resizeCap: cap}
}

// hash calculates FNV 64-bit hash of the given key
func hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))

	return h.Sum64()
}
