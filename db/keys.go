package db

import (
	"bytes"
)

// Container for key-value data.
type Keys struct {
	files *Directory
	index *Index
}

func OpenKeys(files *Directory, indexDir *Directory) (*Keys, error) {
	i, _ := OpenIndex(indexDir, 100_000)
	return &Keys{files: files, index: i}, nil
}

// Store kv on disk.
func (kv *Keys) Set(key, val []byte) (*Offset, error) {
	file := kv.files.Last
	data, _ := Encode(key, val)

	// Write kv to file.
	off, err := file.Write(data.Bytes())

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
func (kv *Keys) Get(key []byte) ([]byte, error) {
	// Get index for key.
	i, err := kv.index.Get(key)
	if err != nil {
		return nil, err
	}

	// Get data file and read from it.
	f, _ := kv.files.Get(int(i.FileID))
	buf := make([]byte, i.Size)

	_, err = f.ReadAt(buf, int64(i.Start))

	val := []byte{}
	raw := bytes.NewBuffer(buf)

	Decode(raw, &key, &val)
	return val, err
}
