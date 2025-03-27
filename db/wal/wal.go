package wal

import "os"

type Wal struct {
	file *os.File
	log chan []byte
}

// Open wal file that we will be writing to.
func Open(path string) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	w := &Wal{ file: file, log: make(chan []byte, 1000)}
	return w, nil
}
