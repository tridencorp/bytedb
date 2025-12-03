package db

import (
	"os"
)

const (
	CollectionsPath = "/collections/"
)

// Main database class
type DB struct {
	// Database root directory.
	root string

	internals *DB
}

// Open database.
func Open(path string) (*DB, error) {
	// Create main database and internal one
	internal := path + "/internal"
	err := os.MkdirAll(internal, 0755)
	if err != nil {
		return nil, err
	}

	internals := &DB{root: internal}
	return &DB{root: path, internals: internals}, nil
}

// Delete the entire database
func (db *DB) Delete() error {
	return os.RemoveAll(db.root)
}
