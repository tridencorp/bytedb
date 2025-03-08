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

  // Keeping keys/collisions in memory.
  Keys       []Key
  Collisions []Key

	nextCollision   atomic.Uint32 // Index in Collisions table.
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

	f.nextCollision.Store(0)
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

	if key.Empty() {
		key.Set(keyName)

		offset := f.offset(hash)
		key.SetOffset(offset)

		} else {
		// We have collision. We must pick next empty index in Collision table.
		if !key.HasCollision() {
			// First collision.
			index := f.nextCollision.Add(1)
			key.SetSlot(index)

			// New collision key.
			key = new(Key)
			key.Set(keyName)

			offset := f.collisionOff()
			key.SetOffset(offset-IndexSize)

			if index >= uint32(len(f.Collisions)) {
				f.Collisions = append(f.Collisions, make([]Key, 100)...)
			}

			f.Collisions[index] = *key
			} else {

			// We had more than 1 collision already. Iterate and find last one.
			for {
				slot := key.Slot()
				if slot == 0 {
					break
				}

				key = &f.Collisions[slot]
			}

			index := f.nextCollision.Add(1)
			key.SetSlot(index)

			key = new(Key)
			key.Set(keyName)

			offset := f.collisionOff()
			key.SetOffset(offset-IndexSize)

			if index >= uint32(len(f.Collisions)) {
				f.Collisions = append(f.Collisions, make([]Key, 100)...)
			}

			f.Collisions[index] = *key
		}
	}


  idx := Index{BucketId: bucketID, Size: uint32(size), Offset: keyOffset}
	copy(idx.Key[:], key[:20])

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, idx)
	if err != nil {
		return err
	}

	_, err = f.fd.WriteAt(buf.Bytes(), key.Offset())
	if err != nil {
		return err
	}

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
	return f.collisionOffset.Add(IndexSize)
}

// Read index for given key.
func (f *File) Get(kv []byte) (*Index, error) {
	offset := HashKey(kv) % f.indexesPerFile
	key := f.Keys[offset]
	
	// No key.
	if key.Empty() {
		return nil, fmt.Errorf("no key")
	}

	// Find key in Keys or Collisions.
	for {
		// Key without collisions.
		if !key.HasCollision(){
			break
		}

		// Key with collisions, find the correct one.
		if key.Equal(kv) {
			break
		} else {
			key = f.Collisions[key.Slot()]
		}
	}

	// TODO: Optimize this.
	data := make([]byte, IndexSize)
	_, err := f.fd.ReadAt(data, key.Offset())

	index := Index{}
	buf := bytes.NewBuffer(data)
	err = binary.Read(buf, binary.BigEndian, &index)
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
