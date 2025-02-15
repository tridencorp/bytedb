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

	internals *DB
}

// Open database.
func Open(path string) (*DB, error) {
	// Create main database and internal one.
	path += "/internal"

	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err	
	}

	internals := &DB{root: path}
	return &DB{root: path, internals: internals}, nil
}

// Delete the entire database.
func (db *DB) Delete() error {
	return os.RemoveAll(db.root)
}
