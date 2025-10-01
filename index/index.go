package index

import (
	"bytedb/db/mmap"
	"hash/fnv"
	"os"
)

type IndexFile struct {
	len       uint64
	maxBlocks uint64
	file      *os.File
	blocks    *mmap.Mmap
	growBy    uint64 // blocks added per each resize
}

// Open mmaps the given file and returns an IndexFile.
// blockCount specifies how many blocks will be mapped from the file into memory.
func Open(file *os.File, blockCount uint64) (*IndexFile, error) {
	blocks, err := mmap.Open(file, int(blockCount), 0)
	if err != nil {
		return nil, err
	}

	return &IndexFile{maxBlocks: blockCount, blocks: blocks, growBy: blockCount}, nil
}

// Write writes index key to file
func (f *IndexFile) Write(key []byte) {
	// Calculate block number starting from 1 (0 is reserved for file header)
	// n := (hash(key) % f.maxBlocks) + 1

	// Get block
}

// hash calculates FNV 64-bit hash of the given key
func hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))

	return h.Sum64()
}
