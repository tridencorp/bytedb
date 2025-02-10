package db

// Main database class.
type DB struct {
	// Database root directory.
	root string
}

// Open database.
func Open(path string) (*DB, error) {
	db := new(DB)
	db.root = path

	return db, nil
}
