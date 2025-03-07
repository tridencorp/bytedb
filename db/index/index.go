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

const (
  TypeKv   = 0
	TypeHash = 1 
)

// Index size in bytes.
const IndexSize = 37 
const BlockSize = IndexSize * 6

// Index will represent key in our database.
type Index struct {
	Key [20]byte // 20 bytes

  // KeyVal
	Deleted    bool  // 1 byte
	BucketId uint32  // 4 bytes
	Size     uint32  // 4 bytes
	Offset   uint64  // 8 bytes
}

type File struct {
  fd *os.File

  // Keeping key/collision offsets in memory.
  Keys       []Key
  Collisions []Key

	collisionIndex  atomic.Uint32 // Index in Collisions table.
	collisionOffset atomic.Uint64 // Offset in index file.

	// Number of indexes file can handle.
	indexesPerFile uint64
}

// Load index file from given directory. 
func Load(dir string, indexesPerFile uint64) (*File, error) {
	// TODO: Only temporary and will be replaced by proper index file.
	dir += "/index.idx"

	file, err := os.OpenFile(dir, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

  f := &File{fd: file, indexesPerFile: indexesPerFile}
	f.Keys = make([]Key, f.indexesPerFile)

	f.collisionIndex.Store(0)
	f.collisionOffset.Store(f.indexesPerFile * IndexSize)

	size := uint64(math.Ceil(float64(30.0*float64(f.indexesPerFile)/100))) 
	f.Collisions = make([]Key, size)

	return f, nil
}

// Create an index for the given key/value and store it in the index file.
// This will allow us for faster lookups.
func (f *File) Set(keyName []byte, size int, keyOffset uint64, bucketID uint32) error {	
	hash := HashKey(keyName)
	off  := hash % f.indexesPerFile

	// Find key in Keys.
	key := &f.Keys[off]
	fmt.Println(key)

	if key.Empty() {
		key.Set(keyName)

		offset := f.offset(hash)
		key.SetOffset(offset)
	} else {
		// We have collision. We must pick next empty index in Collisions table.
		index := f.collisionIndex.Add(1)
		key.SetSlot(index)

		offset := f.collisionOff()
		key.SetOffset(offset)
	}

  idx := Index{BucketId: bucketID, Size: uint32(size), Offset: keyOffset}

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, idx)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = f.fd.WriteAt(buf.Bytes(), key.Offset())
	if err != nil {
		return err
	}

	fmt.Println(f.Keys[off])
	return nil
}
		
// Load index file.
func LoadIndexFile(path string, indexesPerFile uint64) (*File, error) {
	path += "/index.idx"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

	f := &File{fd: file, indexesPerFile: indexesPerFile}

	// ~30% of keys size.
	// size := uint64(math.Ceil(float64(30.0*float64(f.indexesPerFile)/100))) 
	return f, nil
}

// Calculate index offset for new key.
// Also checks for hash collisions 
// and update the offset accordingly.
func (f *File) offset(hash uint64) uint64 {
  return hash % f.indexesPerFile * IndexSize
}

// Calculate collision offset in index file.
func (f *File) collisionOff() uint64 {
	return f.collisionOffset.Add(IndexSize) - IndexSize
}

// Read index for given key.
func (f *File) Get(key []byte) (*Index, error) {
	// Find index position
	offset := f.offset(HashKey(key))
	data := make([]byte, IndexSize)

	f.fd.ReadAt(data, int64(offset))
	idx := Index{}

	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &idx)
	if err != nil {
		return nil, err
	}

	if idx.Deleted {
		return nil, fmt.Errorf("Key was %s deleted", key)
	}

	return &idx, nil
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
