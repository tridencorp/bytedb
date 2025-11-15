package db

import (
	"bytedb/collection"
	"fmt"
	"os"
	"path/filepath"
)

// Offset keeps information about the location of the data.
type Offset struct {
	FileID uint32
	Start  uint32
	Size   uint32
	Hash   [8]byte
}

type File struct {
	ID        int
	file      *os.File
	blockSize int64

	DataBlocks  []Block
	IndexBlocks []Block
}

func OpenFile(path string) (*File, error) {
	dir := filepath.Dir(path)

	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	flags := os.O_CREATE | os.O_RDWR
	f, err := os.OpenFile(filepath.Ext(path), flags, os.ModePerm)

	file := &File{
		file:        f,
		DataBlocks:  make([]Block, 0, 1_000),
		IndexBlocks: make([]Block, 0, 1_000),
	}

	return file, err
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

// Get the number of blocks in file
func (f *File) BlockCount() int64 {
	return f.Size() / f.blockSize
}

// Write key-val to block
func (f *File) WriteKV(key *collection.Key, val []byte) error {
	fmt.Println(key, " --- ", val)

	return nil
}

// Read data from file into dst, starting from given offset.
func (f *File) ReadAt(dst []byte, off int64) (int, error) {
	return f.file.ReadAt(dst, off)
}

// Write data to given block number. If there won't be any space
// left in the block, it will return -1.
func (f *File) WriteBlock(num int64, data []byte) (int, error) {
	// If block size is not set, we are dealing with normal file
	// which doesn't operate on our blocks.
	if f.blockSize == 0 {
		return 0, fmt.Errorf("wrong file type, cannot read blocks")
	}

	// Read block and check if we have enough free space.
	// TODO: v1: We will add option to keep this in memory.
	block, err := f.ReadBlock(num)
	if err != nil {
		return 0, err
	}

	if block.isFull(int(block.footer.Len) + len(data)) {
		return -1, nil
	}

	block.Write(data)

	// Write entire block back to the file.
	n, err := f.file.WriteAt(block.data, block.offset)
	return n, err
}

// Read data from given block.
func (f *File) ReadBlock(num int64) (*Block, error) {
	// Get block offset.
	offset := num * f.blockSize

	// Read block.
	data := make([]byte, f.blockSize)
	_, err := f.file.ReadAt(data, offset)

	b := NewBlock(data, int32(f.blockSize))
	b.offset = offset

	return b, err
}
