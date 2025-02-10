package db

import "os"

const(
	MaxIndexesPerFile = 10_000
)

// Index will represent key in our database.
type Index struct {
	bucketId uint32  // 4 bytes
	size 		 uint32  // 4 bytes
	offset 	 uint64  // 8 bytes
}

type IndexFile struct {
	file *os.File
}

// Load index file for collection.
func LoadIndexFile(coll *Collection) (*IndexFile, error) {
	return nil, nil
}
