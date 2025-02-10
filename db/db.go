package db

import (
	"os"
	"path/filepath"
)

const (
	CollectionsPath = "/collections/"
)

// Main database class.
type DB struct {
	// Database root directory.
	root string
}

type Collection struct {
	file *os.File
}

// Open database.
func Open(path string) (*DB, error) {
	db := new(DB)
	db.root = path

	return db, nil
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
	return coll, nil
}
