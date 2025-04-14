package index

import (
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
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
	size := uint64(math.Ceil(float64(40.0 * float64(f.capacity) / 100)))
	f.Collisions = make([]Key, size)

	total := len(f.Keys) * IndexSize + len(f.Collisions) * IndexSize
	f.fd.Truncate(int64(total))

	data, err := unix.Mmap(int(file.Fd()), 0, int(total), unix.PROT_READ | unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	f.data = data
	return f, nil
}

// Create an index for the given key/value and store it in the index file.
// This will allow us for faster lookups.
func (f *File) Set(name []byte, size int, keyOffset uint64, bucketID uint32) error {	
	key, offset := f.set(HashKey(name))
	return f.Write(key, offset, bucketID, uint32(size), keyOffset)
}

// Set new key.
func (f *File) set(hash uint64) (*Key, int64) {
	if f.nextCollision.Load() + 100 >= uint32(len(f.Collisions)) {
		f.Collisions = append(f.Collisions, make([]Key, 1000)...)
	}

	key := &f.Keys[hash % f.capacity]

	// Set new key.
	if key.Empty() {
		offset := f.offset(hash)
	
		key.SetHash(hash)
		key.SetOffset(offset)

		return key, int64(offset)
	}

	key = f.Last(key)

	// Set collision key.
	collision := new(Key)
	collision.SetHash(hash)
	collision.SetOffset(f.collisionOff() - IndexSize)

	// Set position and put new collision to collisions table.
	position := f.NextCollision()

	key.SetPosition(position)
	f.Collisions[position] = *collision
	
	return collision, int64(position)
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
	key := &f.Keys[hash % f.capacity]

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
	index := Index{
		Hash: 		key.Hash(),
		Position: uint32(offset),
		Bucket: 	bucket,
		Size: 		uint32(size),
		Offset: 	keyOffset,
	}

	s   := unsafe.Sizeof(index)
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&index)), s)

	// _, err := f.fd.WriteAt(buf, key.Offset())
	// _, err := f.data.WriteAt(buf, key.Offset())
	copy(f.data[key.Offset():], buf)
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
	size  := unsafe.Sizeof(index)
	buf   := unsafe.Slice((*byte)(unsafe.Pointer(&index)), size)

	n := copy(buf, f.data[key.Offset():])
	if n == 0 {
		return nil, fmt.Errorf("Cannot read from index")
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
