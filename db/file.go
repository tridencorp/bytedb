package db

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
)

// Special 4-byte sequence used to mark end of data.
var EOFMarker = []byte{0xFF, 0xFF, 0xFF, 0xFF}

// File represents a data file with fixed-size blocks.
type File struct {
	file        *os.File
	blockSize   int64
	writeOffset int64
}

// Offset represents the position and size of a data within a file.
type Offset struct {
	FileID uint32
	Start  uint32
	Size   uint32
	Hash   [8]byte
}

// Open path. Create one if it doesn't exists.
func OpenPath(path string, flag int) (*File, error) {
	dir := filepath.Dir(path)

	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	file, err := OpenFile(path, flag)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func OpenFile(path string, flag int) (*File, error) {
	file, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, nil
	}

	return &File{file: file, blockSize: 4096, writeOffset: 0}, nil
}

// Resize file to given size.
func (f *File) Resize(size int64) error {
	err := f.file.Truncate(size)
	if err != nil {
		return err
	}

	return nil
}

// Size Returns file size in bytes.
func (f *File) Size() int64 {
	info, err := os.Stat(f.file.Name())
	if err != nil {
		return -1
	}

	return info.Size()
}

// Get the number of blocks in file.
func (f *File) BlockCount() int64 {
	return f.Size() / f.blockSize
}

// Write data to file.
func (f *File) Write(data []byte) (*Offset, error) {
	n, err := f.file.WriteAt(data, f.writeOffset)
	if err != nil {
		return nil, err
	}

	if n != len(data) {
		return nil, fmt.Errorf("error when writing data to file")
	}

	off := &Offset{Start: uint32(f.writeOffset), Size: uint32(n)}

	f.writeOffset += int64(n)
	return off, nil
}

// Write data to given block number. If there won't be any space
// left in the block, it will return -1.
func (f *File) WriteBlock(num int64, data []byte) (int, error) {
	// If block size is not set, we are dealing with normal file.
	if f.blockSize == 0 {
		return 0, fmt.Errorf("wrong file type, cannot read blocks")
	}

	// Read block and check if we have enough free space.
	// TODO: v1: We will add option to keep this in memory.
	block, err := f.ReadBlock(num)
	if err != nil {
		return 0, err
	}

	// Get block offset from block footer.
	// footer := block.ReadFooter()

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
func (f *File) ReadBlock(num int64) (Block, error) {
	b := make(Block, f.blockSize)
	offset := num * f.blockSize

	_, err := f.file.ReadAt(b, offset)
	return b, err
}
