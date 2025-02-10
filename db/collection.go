package db

import (
	"fmt"
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

	// We must set offset to current file size.
	offset, err := file.Seek(0, io.SeekEnd)
	fmt.Printf("Size: %d\n", offset)
	coll.offset.Store(offset)

	return coll, nil
}

// Store key in collection.
func (coll *Collection) Set(key string, val []byte) (int, error) {
	off := coll.offset.Swap(int64(len(val)))

	// We are using WriteAt because, when carefully 
	// handled, it's concurrent-friendly.
	return coll.file.WriteAt(val, coll.offset.Add(off))
}