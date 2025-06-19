package db

import (
	"bytes"
	"errors"
	"hash/fnv"
	"unsafe"
)

type Index struct {
	file        *File
	keysPerFile int64
	IndexSize   int8
}

// Open and load indexes. It creates a new index file
// if it doesn't already exist.
func OpenIndex(dir *Directory, keysPerFile int64) (*Index, error) {
	i := &Index{
		file:        dir.Last,
		keysPerFile: keysPerFile,
		IndexSize:   int8(unsafe.Sizeof(Offset{})),
	}

	i.Prealloc(keysPerFile)
	return i, nil
}

// Preallocate space for max number of keys per file.
func (i *Index) Prealloc(keys int64) (int64, error) {
	// Calculate required space for all keys.
	size := keys * int64(i.IndexSize)
	size = (size * 140) / 100 // +40% for collisions

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
	off.Hash = [8]byte(ToBytes(&h))

	// Get block number for key.
	n := int64(h % uint64(i.file.BlockCount()))

	// If block is full, write to next one.
	for j := 0; j < 2; j++ {
		_, err := i.file.WriteBlock(n, ToBytes(off))

		// Block is full, write to next one.
		if errors.Is(err, ErrFull) {
			n += 1
			continue
		}
		return nil
	}

	return nil
}

// Get index.
func (i *Index) Get(key []byte) (*Offset, error) {
	h := Hash(key)

	// Get block number for key.
	n := int64(h % uint64(i.file.BlockCount()))
	off := &Offset{}

	// Find index key in block. If not found we will search in next block.
	// What if not found ? Undecided yet 🫣
	for j := 0; j < 2; j++ {
		// Read block.
		b, err := i.file.ReadBlock(n)
		if err != nil {
			return nil, err
		}

		// Read all offsets from block and compare theirs hash to ours.
		for b.Read(ToBytes(off)) {
			if bytes.Equal(off.Hash[:], ToBytes(&h)) {
				return off, nil
			}
		}

		// We didn't find anything, increment to next block.
		n += 1
	}

	return nil, nil
}

// Compute hash for given key.
func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}
