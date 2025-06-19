package db

import (
	"os"
)

// Container for key-value data.
type KV struct {
	file  *File
	index *Index
}

func OpenKV(path string, index *Index) (*KV, error) {
	// Open kv file.
	f, err := OpenPath(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}

	return &KV{file: f, index: index}, nil
}

// Store kv on disk.
func (kv *KV) Set(key, val []byte) (*Offset, error) {
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

// Get key from disk.
func (kv *KV) Get(key []byte) ([]byte, error) {
	// Get index for key.
	i, err := kv.index.Get(key)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
