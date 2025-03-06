package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
)

const (
  TypeKv   = 0
	TypeHash = 1 

  IndexesPerFile = 5_000
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
	Collisions bool  // 1 byte
	BucketId uint32  // 4 bytes
	Size     uint32  // 4 bytes
	Offset   uint64  // 8 bytes
}

// Index block that will be used to read/write indexes from file.
// One index block will be able to fit 6 keys: 1 key + 5 collisions.
type Block struct {
	Keys [5]Index
}

type IndexFile struct {
  fd *os.File

  // We are keeping track of all collisions that are happening in the 
  // latest block (block with the highest ID). We increase the counter 
  // each time collision happens. Thanks to that we know which entry 
  // in index block we should fill. 
  Collisions map[uint64]int8

	// Number of indexes file can handle.
	indexesPerFile uint64
}

// Load index file.
func LoadIndexFile(path string) (*IndexFile, error) {
	path += "/index.idx"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

	f := &IndexFile{fd: file, indexesPerFile: IndexesPerFile}
	return f, nil
}

// Create an index for the given key/value and store it in the index file.
// This will allow us for faster lookups.
func (file *IndexFile) Set(key []byte, size int, keyOffset uint64, bucketID uint32) error {	
  hash  := HashKey(key)
  block := Block{}
  pos   := file.collisions(hash)

  idx := Index{BucketId: 1, Size: uint32(size), Offset: keyOffset}
  copy(idx.Key[:], key)

  block.Keys[pos] = idx

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

// Get number of collisions for given hash.
// Returned value will determine position in block.
func (file *IndexFile) collisions(hash uint64) int8 {
  _, exists := file.Collisions[hash]
  if exists {
    file.Collisions[hash] += 1
  }

  return file.Collisions[hash]
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
