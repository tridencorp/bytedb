package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"sync/atomic"
)

// Index size in bytes.
const IndexSize = 29

// Index will represent key in our database.
type Index struct {
	Hash 		 uint64  // 8 bytes
	Offset   uint64  // 8 bytes
	Position uint32  // 4 bytes
	Bucket   uint32  // 4 bytes
	Size     uint32  // 4 bytes
	Deleted  bool    // 1 byte
}

type File struct {
	fd *os.File
	
	// Keeping keys/collisions in memory.
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
	dir += "/index.idx"

	file, err := os.OpenFile(dir, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

	f := &File{fd: file, capacity: capacity}
	f.Keys = make([]Key, f.capacity)

	f.nextCollision.Store(0)
	f.collisionOffset.Store(f.capacity * IndexSize)

	// Collisions are ~40% of file capacity. 
	size := uint64(math.Ceil(float64(40.0*float64(f.capacity)/100)))
	f.Collisions = make([]Key, size)

	return f, nil
}

// Create an index for the given key/value and store it in the index file.
// This will allow us for faster lookups.
func (f *File) Set(name []byte, size int, keyOffset uint64, bucketID uint32) error {	
	if f.nextCollision.Load() + 100 >= uint32(len(f.Collisions)) {
		f.Collisions = append(f.Collisions, make([]Key, 1000)...)
	}

	key := f.Last(HashKey(name))

	if key.Empty() {
		f.setKey(key, name)
	} else {
		key = f.newCollision(key, name)
	}

	err := f.Write(key, bucketID, uint32(size), keyOffset)
	return err
}

// Find the last key with given hash.
// Because of collisions we can have the same hash for
// different keys. This function finds the last one.
func (f *File) Last(hash uint64) *Key {
	offset := hash % f.capacity
	key := &f.Keys[offset]

	// No key found or key doesn't have any collisions yet.
	if key.Empty() || !key.HasCollision() {
		return key
	}

	// At this point key has minimum one collision. Let's iterate
	// and get the last one. 
	for key.HasCollision() {
		key = &f.Collisions[key.Position()]
	}

	return key
}

func (f *File) Write(key *Key, bucket, size uint32, offset uint64) error {
	idx := Index{
		Hash: 		key.Hash(), 
		Position: key.Position(), 
		Bucket: 	bucket, 
		Size: 		uint32(size), 
		Offset: 	offset,
	}

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, idx)
	if err != nil {
		return err
	}

	_, err = f.fd.WriteAt(buf.Bytes(), key.Offset())
	return err
}

func (f *File) findKey(name []byte) *Key {
	offset := HashKey(name) % f.capacity
	key := &f.Keys[offset]

	// No key or we have match for the first time.
	if key.Empty() || key.Equal(name) {
		return key
	}

	// Find key in collisions table.
	for key.HasCollision() {
		key = &f.Collisions[key.Position()]

		if key.Equal(name) {
			return key
		}
	}

	return key
}

func (f *File) newCollision(key *Key, collisionKey []byte) *Key {
	index := f.NextCollision()
	key.SetPosition(index)

	// New collision key.
	key = new(Key)
	key.SetHash(HashKey(collisionKey))

	offset := f.collisionOff()
	key.SetOffset(offset - IndexSize)

	f.Collisions[index] = *key
	return key
}

// Return next collision index. If index exceed collisions table length,
// it will resize it. 
func (f *File) NextCollision() uint32 {
	return f.nextCollision.Add(1)
}

// Set key.
func (f *File) setKey(key *Key, name []byte) {
	hash   := HashKey(name)
	offset := f.offset(hash)

	key.SetHash(hash)
	key.SetOffset(offset)
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
	key := f.findKey(name)
	if key.Empty() {
		return nil, fmt.Errorf("Key not found")
	}

	// TODO: Optimize this.
	data := make([]byte, IndexSize)
	_, err := f.fd.ReadAt(data, key.Offset())

	index := Index{}

	buf := bytes.NewBuffer(data)
	err  = binary.Read(buf, binary.BigEndian, &index)
	if err != nil {
		return nil, err
	}

	if index.Deleted {
		return nil, fmt.Errorf("Key was %s deleted", key)
	}

	return &index, nil
}

// Delete index for given key.
func (f *File) Del(key []byte) error {
	// Find index offset.
	offset := f.offset(HashKey(key))

	// If we know the position of index, we can just
	// set it's second byte to 1.
	// TODO: struct changed, this won't work anymore.
	_, err := f.fd.WriteAt([]byte{1}, int64(offset + 1))
	return err
}

// Hash the key.
func HashKey(key []byte) uint64 {
	h := fnv.New64()
	h.Write(key)
	return h.Sum64()
}
