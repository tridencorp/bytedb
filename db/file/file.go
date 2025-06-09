package file

import (
	"bytes"
	"os"
)

// File structure that manages data in blocks.
type File struct {
	file      *os.File
	blockSize int64
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

// Return file size in bytes.
func (f *File) Size() int64 {
	info, _ := os.Stat(f.file.Name())
	return info.Size()
}

// Get number of blocks in file.
func (f *File) BlockCount() int64 {
	return f.Size() / f.blockSize
}

// Write data to given block number. If there won't be any space
// left in the block, it will return -1.
func (f *File) WriteBlock(num int64, data []byte) (int, error) {
	// Read block and check if we have enough free space.
	// TODO: We will add option to keep this in memory.
	// TODO: We will keep data offset in block header.
	block, err := f.ReadBlock(num)
	if err != nil {
		return 0, err
	}

	// Naive way to check it block has enough free space.
	// We are checking if block has enough '0' bytes.
	i := bytes.Index(block, make([]byte, len(data)))
	if i == -1 {
		return i, nil
	}

	offset := (num * f.blockSize) + int64(i)
	n, err := f.file.WriteAt(data, offset)
	return n, err
}

// Read data from given block.
func (f *File) ReadBlock(num int64) ([]byte, error) {
	buf := make([]byte, f.blockSize)
	offset := num * f.blockSize

	_, err := f.file.ReadAt(buf, offset)
	return buf, err
}
