package db

import (
	"os"
)

const (
	CollectionsPath = "/collections/"
)

// Main database class.
type DB struct {
	// Database root directory.
	root string
}

// Open database.
func Open(path string) (*DB, error) {
	return &DB{root: path}, nil
}

// Delete the entire database.
func (db *DB) Delete() (error) {
	return os.RemoveAll(db.root)
}
