package db

// Main class for hash like data type.
//
// Each Hash will be in one file,
// standalone or with other hashes.
//
// Each Hash key will be indexed and could
// have separate index file (or not)
//
// It's basically designed for storing data
// close together.
type Hash struct {
	root string

	// Hash could be used as collection with some 
	// minor changes, ex: all data will be in one file,
	// separate indexes, ...
	keys *Collection
}

// Set key in hash.
func (hash *Hash) Set(key string, val []byte) (int64, int64, error) {
	hash.keys.Set(key, val)
	return hash.keys.Set(key, val)
}

// Get key from hash.
func (hash *Hash) Get(key string) ([]byte, error) {
	return hash.keys.Get(key)
}

// Delete key from hash.
func (hash *Hash) Del(key string) error {
	return hash.keys.Del(key)
}

// Update key in hash.
func (hash *Hash) Update(key string, val []byte) error {
	return hash.keys.Update(key, val)
}
