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
	// Data will be stored in one bucket.
	bucket *Bucket

	// As default keys will be indexed to main 
	// collection index, shared with other keys.
	// 
	// In the next stage, we will introduce custom 
	// index files, allowing us to group hashes 
	// together into a single index file.
	index *IndexFile
}

// Open the hash file, or create it if it doesn't exist.
func OpenHash(col *Collection) (*Hash, error) {
	// Open most recent bucket. Hardcoded for now.
	bucket, err := OpenBucket(col.root + "/hash/" + "1.bucket")
	if err != nil {
		return nil, err
	}

	return &Hash{bucket: bucket, index: col.indexes}, nil
}

// Set key in hash.
func (hash *Hash) Set(key string, val []byte) (int64, int64, error) {
	data, err := NewKey(val).Bytes()
	if err != nil {
		return 0, 0, err
	}

	off, size, err := hash.bucket.Write(data)
	
	// Index new key.
	err = hash.index.Add(key, data, uint64(off))
	if err != nil {
		return 0, 0, err
	}

	return off, size, err
}

// Get key from hash.
func (hash *Hash) Get(key string) ([]byte, error) {
	idx, err := hash.index.Get(key)
	if err != nil {
		return nil, err
	}

	val, err := hash.bucket.Read(int64(idx.Offset), int64(idx.Size))
	if err != nil {
		return nil, err
	}

	// TODO: quick hack, we are reading the whole kv bute we should 
	// remove the size which is 4 bytes.
	return val[4:], err
}
