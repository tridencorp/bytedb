package db

// Container for key-value data.
type KV struct {
	file    *File
	dataDir *Directory
	index   *Index
}

func OpenKV(path string, dataDir *Directory, index *Index) (*KV, error) {
	return &KV{file: dataDir.Last, dataDir: dataDir, index: index}, nil
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

	// Get data file and read from it.
	f, _ := kv.dataDir.Get(int(i.FileID))
	buf := make([]byte, i.Size)

	_, err = f.ReadAt(buf, int64(i.Start))
	return buf, err
}
