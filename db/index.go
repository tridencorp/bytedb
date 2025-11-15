package db

import (
	"bytes"
	"hash/fnv"
	"unsafe"
)

type Index struct {
	files       *Directory
	keysPerFile int64
	IndexSize   int8
}

// Open index for given directory.
func OpenIndex(files *Directory, keysPerFile int64) (*Index, error) {
	i := &Index{
		files:       files,
		keysPerFile: keysPerFile,
		IndexSize:   int8(unsafe.Sizeof(Offset{})),
	}

	i.Prealloc(keysPerFile)
	return i, nil
}

// Preallocate space for max number of keys per file.
func (i *Index) Prealloc(num int64) (int64, error) {
	f := i.files.Last

	// Calculate required space for all keys.
	// TODO: Extract this.
	size := num * int64(i.IndexSize)
	size = (size * 140) / 100 // +30% for collisions

	// Resize if file is smaller than expected.
	if f.Size() < size {
		err := f.Resize(size)
		if err != nil {
			return 0, err
		}
	}

	return size, nil
}

// Set index for the given kv and stores it in the index file.
// func (i *Index) Set(key []byte, off *Offset) error {
// 	f := i.files.Last
// 	h := Hash(key)

// 	off.Hash = [8]byte(ToBytes(&h))

// 	// Get block number for key.
// 	n := int64(h % uint64(f.BlockCount()))

// 	// If block is full, write to next one.
// 	for j := 0; j < 2; j++ {
// 		_, err := f.WriteBlock(n, ToBytes(off))

// 		// Block is full, write to next one.
// 		if errors.Is(err, ErrFull) {
// 			n += 1
// 			continue
// 		}

// 		return nil
// 	}

// 	return nil
// }

// Get index.
func (i *Index) Get(key []byte) (*Offset, error) {
	f := i.files.Last
	h := Hash(key)

	// Get block number for key.
	n := int64(h % uint64(f.BlockCount()))
	off := &Offset{}

	// Find index key in block. If not found we will search in next block.
	for j := 0; j < 2; j++ {
		// Read block.
		b, err := f.ReadBlock(n)
		if err != nil {
			return new(Offset), err
		}

		// Read all offsets from block and compare them to the hash we are looking for.
		for b.Read(ToBytes(off)) {
			if bytes.Equal(off.Hash[:], ToBytes(&h)) {
				return off, nil
			}
		}

		// We didn't find anything, increment to next block.
		n += 1
	}

	return new(Offset), nil
}

// Compute hash for given key.
func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}
