package db

import (
	"bytes"
	"hash/fnv"
	"os"
	"unsafe"
)

type Index struct {
	file        *File
	keysPerFile int64
	IndexSize   int8
}

// Open and load indexes. It creates a new index file
// if it doesn't already exist.
func OpenIndex(dir string, keysPerFile int64) (*Index, error) {
	f, err := OpenFile(dir, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, nil
	}

	i := &Index{
		file:        f,
		keysPerFile: keysPerFile,
		IndexSize:   int8(unsafe.Sizeof(Offset{})),
	}

	i.Prealloc(keysPerFile)
	return i, nil
}

// Preallocate space for max number of keys per file.
// Also adding extra space for collisions.
func (i *Index) Prealloc(keys int64) (int64, error) {
	// Calculate required space for all keys.
	size := keys * int64(i.IndexSize)
	size = (size * 130) / 100 // 30% space for collisions

	// Resize if file is smaller than expected.
	if i.file.Size() < size {
		err := i.file.Resize(size)
		if err != nil {
			return 0, err
		}
	}

	return size, nil
}

// Set index for the given kv and stores it in the index file.
func (i *Index) Set(key []byte, off *Offset) error {
	h := Hash(key)

	// Get block number for key.
	n := int64(h % uint64(i.file.BlockCount()))

	i.file.WriteBlock(n, key)
	return nil
}

// Get index.
func (i *Index) Get(key []byte) ([]byte, error) {
	h := Hash(key)

	// Get block number for key.
	n := int64(h % uint64(i.file.BlockCount()))

	// Read block.
	b, err := i.file.ReadBlock(n)
	if err != nil {
		return nil, err
	}

	// Find our key.
	// TODO: We know the fixed  size, so we can make it quicker than Index - probably 🤞
	index := bytes.Index(b, key)
	if index == -1 {
		return nil, nil
	}

	return b[index : index+len(key)], nil
}

// Compute hash for given key.
func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}
