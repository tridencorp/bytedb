package index

import (
	"bucketdb/db/file"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Index size in bytes
const IndexSize = 32

// This will be dynamic
const Size = 24

// key stores information about KV position in database files.
type key struct {
	Hash   uint64 // 8 bytes
	Offset uint32 // 4 bytes
	Bucket uint32 // 4 bytes
	Size   uint16 // 2 bytes
	Flag   uint8  // 1 byte

	// memory alignment
	// TODO: Maybe we won't need this one
	_ [5]byte
}

type Index struct {
	file *file.File
	keys []key
}

type File struct {
	fd   *os.File
	data []byte

	// Keeping keys/collisions in memory.
	mux        sync.RWMutex
	Keys       []Key
	Collisions []Key

	nextCollision   atomic.Uint32 // Index in Collisions table.
	collisionOffset atomic.Uint64 // Offset in index file.

	// Max number of indexes file can have.
	capacity uint64
}

// Open and load indexes. It creates a new index file
// if it doesn't already exist.
func Open(dir string, capacity uint64) (*Index, error) {
	file, err := file.Open(dir)
	if err != nil {
		return nil, nil
	}

	return &Index{file: file}, nil
}

// Set index for the given kv and stores it in the index file.
func (i *Index) Set(kv []byte) error {

	return nil
}

// Find the last key with given hash.
// Because of collisions we can have the same hash for
// different keys. This function finds the last one.
func (f *File) Last(key *Key) *Key {
	// Iterate until there are no collisions.
	for key.HasCollision() {
		key = &f.Collisions[key.Position()]
	}

	return key
}

// Find key for given hash.
func (f *File) Find(hash uint64) *Key {
	key := &f.Keys[hash%f.capacity]

	// No key found or we have our match.
	if key.Empty() || key.Equal(hash) {
		return key
	}

	// Find key in collisions table.
	for !key.Equal(hash) && key.HasCollision() {
		key = &f.Collisions[key.Position()]
	}

	return key
}

func (f *File) Write(key *Key, offset int64, bucket, size uint32, keyOffset uint64) error {
	// index := Index{
	// 	Hash:     key.Hash(),
	// 	Position: uint32(offset),
	// 	Bucket:   bucket,
	// 	Size:     uint32(size),
	// 	Offset:   keyOffset,
	// }

	// s := unsafe.Sizeof(index)
	// buf := unsafe.Slice((*byte)(unsafe.Pointer(&index)), s)

	// // _, err := f.fd.WriteAt(buf, key.Offset())
	// // _, err := f.data.WriteAt(buf, key.Offset())
	// copy(f.data[key.Offset():], buf)
	return nil
}

// Return next collision index. If index exceed collisions table length,
// it will resize it.
func (f *File) NextCollision() uint32 {
	return f.nextCollision.Add(1)
}

// Calculate index offset for new key.
func (f *File) offset(hash uint64) uint64 {
	return hash % f.capacity * IndexSize
}

// Calculate collision offset in index file.
func (f *File) collisionOff() uint64 {
	return f.collisionOffset.Add(IndexSize)
}

// Read index for given key.
func (f *File) Get(name []byte) (*Index, error) {
	key := f.Find(HashKey(name))

	if key.Empty() {
		return nil, fmt.Errorf("Key not found")
	}

	index := Index{}
	size := unsafe.Sizeof(index)
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&index)), size)

	n := copy(buf, f.data[key.Offset():])
	if n == 0 {
		return nil, fmt.Errorf("Cannot read from index")
	}

	// if index.Deleted {
	// 	return nil, fmt.Errorf("Key was %s deleted", key)
	// }

	return &index, nil
}

// Delete key.
func (f *File) Del(key []byte) error {
	// Find index offset.
	offset := f.offset(HashKey(key))

	// If we know the position of index, we can just
	// set it's Deleted field to 1.
	_, err := f.fd.WriteAt([]byte{1}, int64(offset+29))
	return err
}

// Hash the key.
func HashKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}
