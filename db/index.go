package db

import (
	"os"
)

const(
	MaxIndexesPerFile = 10_000
)

// Index will represent key in our database.
type Index struct {
	bucketId uint32  // 4 bytes
	size     uint32  // 4 bytes
	offset   uint64  // 8 bytes
}

type IndexFile struct {
	file *os.File

	// Maximum number of indexes per index file.
	maxNumber uint32
}

// Load index file.
func LoadIndexFile(coll *Collection) (*IndexFile, error) {
	path := coll.root + "/index.idx"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

	indexFile := &IndexFile{file: file, maxNumber: MaxIndexesPerFile}
	return indexFile, nil
}
