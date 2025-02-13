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
