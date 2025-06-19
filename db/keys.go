package db

// Container for key-value data.
type Keys struct {
	files *Directory
	index *Index
}

func OpenKeys(files *Directory, index *Directory) (*Keys, error) {
	i, _ := OpenIndex(index, 100_000)
	return &Keys{files: files, index: i}, nil
}

// Store kv on disk.
func (kv *Keys) Set(key, val []byte) (*Offset, error) {
	data := append(key, val...)
	file := kv.files.Last

	// Write kv to file.
	off, err := file.Write(data)
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
	return buf, err
}
