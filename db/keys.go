package db

// Container for key-value data.
type Keys struct {
	files *Directory
	index *Index
}

// func OpenKeys(files *Directory, indexes *Directory) (*Keys, error) {
// 	i, _ := OpenIndex(indexes, 100_000)
// 	return &Keys{files: files, index: i}, nil
// }

// // Store key on disk.
// func (k *Keys) Set(key, val []byte) (*Offset, error) {
// 	file := k.files.Last
// 	data, _ := Encode(key, val)

// 	// Write key data to file.
// 	off, err := file.Write(data.Bytes())

// 	if err != nil {
// 		return nil, err
// 	}

// 	// Write key to index.
// 	err = k.index.Set(key, off)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return off, nil
// }

// Get key from disk
// func (k *Keys) Get(key []byte) ([]byte, error) {
// 	// Look up index
// 	i, err := k.index.Get(key)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Get data file
// 	f, _ := k.files.Get(int(i.FileID))

// 	// Read from file
// 	buf := make([]byte, i.Size)
// 	_, err = f.ReadAt(buf, int64(i.Start))

// 	// Decode key/val
// 	var val []byte
// 	Decode(bytes.NewBuffer(buf), &key, &val)

// 	return val, err
// }
