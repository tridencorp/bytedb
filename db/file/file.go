package file

import "os"

// File structure that manages data in blocks.
type File struct {
	file *os.File
}

func Open(path string) (*File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

	return &File{file: file}, nil
}
