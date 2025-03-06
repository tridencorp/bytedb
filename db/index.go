package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
)

const(
	IndexesPerFile = 5_000

	// Index size in bytes.
	IndexSize = 37
)

const (
	TypeKv   = 0
	TypeHash = 1 
)

// Index will represent key in our database.
type Index struct {
	// Because of collisions we will keep first 20 bytes of each
	// key in index. Each index block will have space for around 
	// 6 collision keys. We will read them at once and will be able
	// to match them in memory. This will save us 1-6 file reads 
	// (worst case scenario).
	//
	// TODO: In the end try to align this struct in memory.
	Key [20]byte

	Deleted  bool    // 1 byte
	BucketId uint32  // 4 bytes
	Size     uint32  // 4 bytes
	Offset   uint64  // 8 bytes
}

// Index block that will be used to read/write indexes from file.
// One index block will be able to fit 6 keys: 1 key + 5 collisions.
type Block struct {
	Keys [6]Index
}

type IndexFile struct {
  fd *os.File

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

	indexFile := &IndexFile{fd: file, indexesPerFile: IndexesPerFile}
	return indexFile, nil
}

// Create an index for the given key/value and store it in the index file.
// This will allow us for faster lookups.
func (file *IndexFile) Add(key []byte, size int, keyOffset uint64, bucketID uint32) error {	
	idx  := Index{BucketId: 1, Size: uint32(size), Offset: keyOffset}

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, idx)
	if err != nil {
		return err
	}

	off := file.offset(key)
	_, err = file.fd.WriteAt(buf.Bytes(), int64(off))
	if err != nil {
		return err
	}

	return nil
}

// Calculate index offset for new key.
func (indexes *IndexFile) offset(key []byte) uint64 {
	hash := HashKey(key)
  return hash % indexes.indexesPerFile * IndexSize
}

// Read index for given key.
func (file *IndexFile) Get(key []byte) (*Index, error) {
	// Find index position
	offset := file.offset(key)
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
	offset := file.offset(key)

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
