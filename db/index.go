package db

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"os"
)

const(
	MaxIndexesPerFile = 10_000
	IndexSize         = 16
)

// Index will represent key in our database.
type Index struct {
	BucketId uint32  // 4 bytes
	Size     uint32  // 4 bytes
	Offset   uint64  // 8 bytes
}

type IndexFile struct {
	file *os.File

	// Maximum number of indexes per index file.
	maxNumber uint32
}

// Load index file.
func LoadIndexFile(coll *Collection) (*IndexFile, error) {
	path := coll.root + "/index.idx"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

	indexFile := &IndexFile{file: file, maxNumber: MaxIndexesPerFile}
	return indexFile, nil
}

// Create an index for the given key/value and store it in the index file.
// This will allow us for faster lookups.
func (indexes *IndexFile) Add(key string, val []byte, offset uint64) error {	
	hash := HashKey(key)
	idx  := Index{BucketId: 1, Size: uint32(len(val)), Offset: offset}

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, idx)
	if err != nil {
		return err
	}

	pos := (hash % indexes.maxNumber) * IndexSize
	indexes.file.WriteAt(buf.Bytes(), int64(pos))
	return nil
}

// Read index for given key.
func (indexes *IndexFile) Get(key string) (*Index, error) {
	hash := HashKey(key)

	// Find index position
	pos := (hash % indexes.maxNumber) * IndexSize
	data := make([]byte, IndexSize)

	indexes.file.ReadAt(data, int64(pos))
	idx := Index{}

	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.LittleEndian, &idx)
	if err != nil {
		return nil, err
	}

	return &idx, nil
}

// Hash key.
func HashKey(key string) uint32 {
  h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}
