package db

import (
	"bytedb/block"
	bit "bytedb/lib/bitbox"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	DefaultHeaderBlocks = 1
	DefaultIndexBlocks  = 10
)

type File struct {
	// header
	IndexOffset uint32
	IndexBlocks uint32

	file *os.File
	Hash uint64

	mu        sync.Mutex
	lastBlock *block.Block
	blocks    map[uint32]*block.Block
}

// Open database file.
// If file is empty, initialize it.
func OpenFile(path string) (*File, error) {
	dir := filepath.Dir(path)

	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	flags := os.O_CREATE | os.O_RDWR
	f, err := os.OpenFile(filepath.Ext(path), flags, os.ModePerm)

	file := &File{
		file:   f,
		blocks: make(map[uint32]*block.Block, 10),
	}

	return file, err
}

// Resize file
func (f *File) Resize(size int64) error {
	err := f.file.Truncate(size)
	if err != nil {
		return err
	}

	return nil
}

// Return file size in bytes
func (f *File) Size() int64 {
	info, err := os.Stat(f.file.Name())
	if err != nil {
		return -1
	}

	return info.Size()
}

// Count total number of blocks in file
func (f *File) BlockCount() int64 {
	return f.Size() / block.BlockSize
}

// Write key-val to blockso
func (f *File) WriteKV(key *Key, val []byte) error {
	// Write data
	_, idx := f.Write(f.lastBlock, val)

	fmt.Println("index block: ", idx)
	return nil
}

// Write data to blocks, starting at offset.
// Return index and number of bytes written.
func (f *File) Write(offset *block.Block, data []byte) (int, *IndexKey) {
	idx := &IndexKey{Offset: offset.ID, Span: 1}
	buf := bit.NewBuffer(data)
	n := int(0)

	for {
		n = offset.Write(buf.Data())
		buf.Consume(n)

		if buf.Len() == 0 {
			break
		}

		// We need another block
		b := block.NewBlock(offset.ID + 1)
		f.Append(b)

		// Increment number of blocks used
		idx.Span++
	}

	return n, idx
}

// Append block to file
func (f *File) Append(b *block.Block) {
	f.lastBlock = b
	f.blocks[b.ID] = b
}

// Read data from file into dst, starting from given offset
func (f *File) ReadAt(dst []byte, off int64) (int, error) {
	return f.file.ReadAt(dst, off)
}

// Return block from cache, read it from disk otherwise
func (f *File) Block(id uint32) *Block {
	// lock - read from cache
	// unlock

	// Read from disk

	// lock - put to cache
	// unlock
	return nil
}

// Read block from file
func (f *File) Read(block *block.Block) (int, error) {
	// Get offset
	off := int64((block.ID - 1) * BlockSize)

	// Read block
	return f.file.ReadAt(block.Data[:], off)
}
