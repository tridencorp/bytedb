package file

import "os"

// File structure that manages data in blocks.
type File struct {
	file      *os.File
	blockSize uint16
}

func Open(path string) (*File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

	return &File{file: file, blockSize: 4096}, nil
}

// Resize file to given size.
func (f *File) Resize(size int64) error {
	err := f.file.Truncate(size)
	if err != nil {
		return err
	}

	return nil
}

// Write data to given block number.
func (f *File) WriteBlock(num int, data []byte) error {
	return nil
}
