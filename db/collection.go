package db

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

type Collection struct {
	file   *os.File

	// We will be using atomic.Swap() for each key.
	// In combination with WriteAt, it should give
	// us the ultimate concurrent writes.
	offset atomic.Int64
}

// Key structure that represents each key stored in collection.
// TODO: Maybe better naming will be Record?
type Key struct {
	data []byte
	// Key size
	size uint32
}

func NewKey(val []byte) *Key {
	return &Key{val, uint32(len(val))}
}

// Encode key to bytes.
func (key *Key) Bytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Encode size.
	err := binary.Write(buf, binary.LittleEndian, key.size)
	if err != nil {
		return nil, err
	}

	// Add key data.
	_, err = buf.Write(key.data)
	if err != nil {
		return nil, err		
	}

	return buf.Bytes(), nil
}

// Open the collection. If it doesn't exist,
// create it with default values.
func (db *DB) Collection(name string) (*Collection, error) {
	// Build collection path.
	path := db.root + CollectionsPath + name + "/1.bucket"
	dir  := filepath.Dir(path)

	// Create directory structure. Do nothing if it already exist.
	if err := os.MkdirAll(dir, 0755)
	err != nil {
		return nil, err
	}

	// Open collection bucket file.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	coll := &Collection{file: file}

	// We must set offset to current file size - end of file offset.
	offset, err := file.Seek(0, io.SeekEnd)
	coll.offset.Store(offset)

	return coll, nil
}

// Store key in collection.
func (coll *Collection) Set(key string, val []byte) (int, error) {
	data, err := NewKey(val).Bytes()
	if err != nil {
		return 0, err
	}

	// We are adding len to atomic value and then deducting it
	// from the result, this should give us space for our data.
	//
	// We could also use CompareAndSwap combination but it's 
	// more complex. 
	off := coll.offset.Add(int64(len(data)))
	off  = off-int64(len(data))

	// We are using WriteAt because, when carefully 
	// handled, it's concurrent-friendly.
	return coll.file.WriteAt(data, off)
}
