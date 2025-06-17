package db

import (
	"os"
)

// Container for key-value data.
type KV struct {
	file  *File
	index *Index

	// block size, preallocate,
}

func OpenKV(path string) (*KV, error) {
	// Open kv file.
	f, err := OpenPath(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}

	return &KV{file: f}, nil
}

// Store kv on disk.
func (kv *KV) Set(key, val []byte) (*Offset, error) {
	// TODO: add data length prefix
	data := append(key, val...)

	// Write kv to file.
	off, err := kv.file.Write(data)
	if err != nil {
		return nil, err
	}

	// Write key to index.
	err = kv.index.Set(key, off)
	if err != nil {
		return nil, err
	}

	return off, nil
}
