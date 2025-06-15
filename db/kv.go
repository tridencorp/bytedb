package db

import (
	"bucketdb/db/file"
	"bucketdb/db/utils"
	"os"
)

// Container for key-value data.
type KV struct {
	File *file.File
}

func OpenKV(path string) (*KV, error) {
	// Open kv file.
	f, err := utils.OpenPath(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}

	return &KV{File: f}, nil
}
