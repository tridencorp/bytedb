package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"os"
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
  // Because of collisions we will keep first 20 bytes of each
	// key in index. Each index block will have space for around 
	// 6 collision keys. We will read them at once and will be able
	// to match them in memory. This will save us 1-6 file reads 
	// (worst case scenario).
	//
	// TODO: In the end try to align this struct in memory.
	Key [20]byte     // 20 bytes

	Deleted    bool  // 1 byte
	BucketId uint32  // 4 bytes
	Size     uint32  // 4 bytes
	Offset   uint64  // 8 bytes
}

// We are keeping this in arrays because structs and slices have
// around 24 bytes overhead each.
type Block [28]byte // 24b + 4b(next)

type IndexFile struct {
  fd *os.File

  // Keeping key/collision offsets in memory.
  Keys       []Block
  Collisions []Block

	// Number of indexes file can handle.
	indexesPerFile uint64
}

// Load index file.
func LoadIndexFile(path string, indexesPerFile uint64) (*IndexFile, error) {
	path += "/index.idx"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

  f := &IndexFile{fd: file, indexesPerFile: indexesPerFile}
  f.Keys = make([]Block, f.indexesPerFile)
  
  // ~30% of keys size.
  size := uint64(math.Ceil(float64(30.0*float64(f.indexesPerFile)/100))) 

  f.Collisions = make([]Block, size)

	return f, nil
}

// Create an index for the given key/value and store it in the index file.
// This will allow us for faster lookups.
func (file *IndexFile) Set(key []byte, size int, keyOffset uint64, bucketID uint32) error {	
  hash  := HashKey(key)
  block := Block{}

  idx := Index{BucketId: 1, Size: uint32(size), Offset: keyOffset}
  copy(idx.Key[:], key)

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, block)
	if err != nil {
    return err
	}

  off := file.offset(hash)
	_, err = file.fd.WriteAt(buf.Bytes(), int64(off))
	if err != nil {
		return err
	}

	return nil
}

// Calculate index offset for new key.
// Also checks for hash collisions 
// and update the offset accordingly.
func (file *IndexFile) offset(hash uint64) uint64 {
  return hash % file.indexesPerFile * BlockSize
}

// Read index for given key.
func (file *IndexFile) Get(key []byte) (*Index, error) {
	// Find index position
	offset := file.offset(HashKey(key))
	data := make([]byte, IndexSize)

	file.fd.ReadAt(data, int64(offset))
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
func (file *IndexFile) Del(key []byte) error {
	// Find index offset.
	offset := file.offset(HashKey(key))

	// If we know the position of index, we can just
	// set it's second byte to 1.
	// TODO: struct changed, this won't work anymore.
	_, err := file.fd.WriteAt([]byte{1}, int64(offset + 1))
	return err
}

// Hash the key.
func HashKey(key []byte) uint64 {
 	h := fnv.New64()
	h.Write(key)
	return h.Sum64()
}
