package index

import (
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Index size in bytes.
const IndexSize = 32

// Index will represent key in our database.
type Index struct {
	Hash 		 uint64  // 8 bytes
	Offset   uint64  // 8 bytes
	Position uint32  // 4 bytes
	Bucket   uint32  // 4 bytes
	Size     uint32  // 4 bytes
	Deleted  bool    // 1 byte
	_        [3]byte // 3 bytes for memory alignment
}

type File struct {
	fd *os.File

	// Keeping keys/collisions in memory.
	mux        sync.RWMutex
	Keys       []Key
	Collisions []Key

	nextCollision   atomic.Uint32 // Index in Collisions table.
	collisionOffset atomic.Uint64 // Offset in index file.

	// Max number of indexes file can have.
	capacity uint64	
}

// Load index file from given directory. 
func Load(dir string, capacity uint64) (*File, error) {
	// TODO: Only temporary and will be replaced by proper index file.
	file, err := os.OpenFile(dir, os.O_RDWR | os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

	f := &File{fd: file, capacity: capacity}
	f.Keys = make([]Key, f.capacity)

	f.nextCollision.Store(0)
	f.collisionOffset.Store(f.capacity * IndexSize)

	// Collisions are ~40% of file capacity. 
	size := uint64(math.Ceil(float64(40.0*float64(f.capacity)/100)))
	f.Collisions = make([]Key, size*3)

	return f, nil
}

func (f *File) position(key *Key) uint32 {
	return key.Position()
}

func (f *File) HasCollision(key *Key) bool {
	return key.HasCollision()
}

// Create an index for the given key/value and store it in the index file.
// This will allow us for faster lookups.
func (f *File) Set(name []byte, size int, keyOffset uint64, bucketID uint32) error {	
	key := f.set(HashKey(name))
	return f.Write(key, bucketID, uint32(size), keyOffset)
}

// Set new key.
func (f *File) set(hash uint64) *Key {
	f.mux.Lock()
	defer f.mux.Unlock()

	if f.nextCollision.Load() + 100 >= uint32(len(f.Collisions)) {
		f.Collisions = append(f.Collisions, make([]Key, 1000)...)
	}

	key := f.Last(hash)

	// Set new key.
	if key.Empty() {
		key.SetHash(hash)
		key.SetOffset(f.offset(hash))
		
		return key
	}

	// Set collision key.
	collision := new(Key)
	collision.SetHash(hash)
	collision.SetOffset(f.collisionOff() - IndexSize)

	// Set position and put new collision to collisions table.
	position := f.NextCollision()
	key.SetPosition(position)
	f.Collisions[position] = *collision

	return collision
}

// Find the last key with given hash.
// Because of collisions we can have the same hash for
// different keys. This function finds the last one.
func (f *File) Last(hash uint64) *Key {
	key := &f.Keys[hash % f.capacity]

	// Iterate until there are no collisions.
	for f.HasCollision(key) {
		key = &f.Collisions[f.position(key)]
	}

	return key
}

// Find key for given hash.
func (f *File) Find(hash uint64) *Key {
	key := &f.Keys[hash % f.capacity]

	// No key found or we have our match.
	if key.Empty() || key.Equal(hash) {
		return key
	}

	// Find key in collisions table.
	for !key.Equal(hash) && f.HasCollision(key) {
		key = &f.Collisions[f.position(key)]
	}

	return key
}

func (f *File) Write(key *Key, bucket, size uint32, offset uint64) error {
	index := Index{
		Hash: 		key.Hash(),
		Position: f.position(key),
		Bucket: 	bucket,
		Size: 		uint32(size),
		Offset: 	offset,
	}

	s   := unsafe.Sizeof(index)
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&index)), s)

	_, err := f.fd.WriteAt(buf, key.Offset())
	return err
}

// Return next collision index. If index exceed collisions table length,
// it will resize it. 
func (f *File) NextCollision() uint32 {
	return f.nextCollision.Add(1)
}

// Load index file.
func LoadIndexFile(path string, capacity uint64) (*File, error) {
	path += "/index.idx"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}
	
	f := &File{fd: file, capacity: capacity}
	return f, nil
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
	f.mux.RLock()
	key := f.Find(HashKey(name))
	f.mux.RUnlock()

	if key.Empty() {
		return nil, fmt.Errorf("Key not found")
	}

	index := Index{}
	size  := unsafe.Sizeof(index)
	buf   := unsafe.Slice((*byte)(unsafe.Pointer(&index)), size)

	_, err := f.fd.ReadAt(buf, key.Offset())
	if err != nil {
		return nil, err
	}

	if index.Deleted {
		return nil, fmt.Errorf("Key was %s deleted", key)
	}

	return &index, nil
}

// Delete key.
func (f *File) Del(key []byte) error {
	// Find index offset.
	offset := f.offset(HashKey(key))

	// If we know the position of index, we can just
	// set it's Deleted field to 1.
	_, err := f.fd.WriteAt([]byte{1}, int64(offset + 29))
	return err
}

// Hash the key.
func HashKey(key []byte) uint64 {
	h := fnv.New64()
	h.Write(key)
	return h.Sum64()
}
