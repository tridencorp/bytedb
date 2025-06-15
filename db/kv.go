package db

import (
	"bucketdb/db/file"
	"bucketdb/db/utils"
	"os"
)

// Container for key-value data.
type KV struct {
	file *file.File
}

func OpenKV(path string) (*KV, error) {
	// Open kv file.
	f, err := utils.OpenPath(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}

	return &KV{file: f}, nil
}

func (kv *KV) Set(key, val []byte) (*file.Offset, error) {
	data := append(key, val...)

	// Write kv to data file.
	off, err := kv.file.Write(data)
	if err != nil {
		return nil, err
	}

	return off, nil
}
