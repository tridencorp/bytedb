package db

import (
	"bytedb/block"
	"bytedb/collection"
	"bytedb/common"
	bit "bytedb/lib/bitbox"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	HeaderBlocks = 1
	IndexSize    = 24
)

// DataClass
type FileHeader struct {
	IndexLen    uint64
	IndexBlocks uint32
	DataBlocks  uint32
}

type File struct {
	ID        int
	file      *os.File
	blockSize int64
	Header    FileHeader
	LastBlock *block.Block

	// Blocks currently keeped in memory
	mu          sync.Mutex
	IndexBlocks map[uint32]*block.Block
	DataBlocks  map[uint32]*block.Block
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
		IndexBlocks: make(map[uint32]*block.Block, 100),
		DataBlocks:  make(map[uint32]*block.Block, 1000),
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

// Get the number of blocks in file
func (f *File) BlockCount() int64 {
	return f.Size() / f.blockSize
}

// Write key-val to blocks
func (f *File) WriteKV(key *collection.Key, val []byte) error {
	fmt.Println(key, " xoxoxo ", val)

	// data block
	blocks, err := f.WriteBlocks(val)
	if err != nil {
		return err
	}

	// index block
	idx, err := f.GetAndReserveIndex(key.Hash)
	if err != nil {
		return err
	}

	fmt.Println("index block: ", idx)
	return nil
}

// Write data to blocks, starting from LastBlock.
// Return number of bytes written and block numbers
// to which data was written.
func (f *File) WriteBlocks(data []byte) (int, []uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	buf := bit.NewBuffer(data)
	blocks := []uint32{f.LastBlock.Num}

	for {
		n := f.LastBlock.Write(buf.Data())
		buf.Consume(n)

		if buf.Len() == 0 {
			break
		}

		// We didn't write everything.
		// Let's create new block and continue.
		b := block.NewBlock()
		b.Num = f.LastBlock.Num + 1

		f.LastBlock = b
		f.DataBlocks[b.Num] = b
		f.Header.DataBlocks++

		blocks = append(blocks, b.Num)
	}

	return buf.Off, blocks
}

func (f *File) GetAndReserveIndex(hash uint64) (*block.Block, error) {
	// get block number for hash
	num := hash % uint64(f.Header.IndexBlocks)
	num += HeaderBlocks // add space for file header

	// TODO: iterate till end of index blocks
	for {
		// Get block for hash
		b, found := f.IndexBlocks[uint32(num)]

		// Read block from disk
		if !found {
			var err error

			b, err = f.ReadBlock(uint32(num))
			if err != nil {
				return nil, err
			}

			f.IndexBlocks[uint32(num)] = b
		}

		// Check if ther is space left, iterate
		// till we find block with free space
		if b.SpaceLeft() >= IndexSize {
			// Each block has fixed number of indexes,
			// we must reserve space for one.
			b.Header.Len += 1

			return b, nil
		}

		// Current block doesn't have enough space.
		// Increment and load another block.
		num += 1
	}
}

// Allocate space for header and indexes.
// Set default headers.
func (f *File) Init() error {
	// Read bytes directly to file header
	ptr := common.BytesPtr(&f.Header)
	n, _ := f.file.ReadAt(ptr, 0)

	// Check if file was already initialized
	if n == 0 || f.Header.IndexBlocks == 0 {
		f.Header.IndexBlocks = 10 // default number of index blocks
		return f.Resize((HeaderBlocks + 10) * block.BlockSize)
	}

	return nil
}

// Read data from file into dst, starting from given offset.
func (f *File) ReadAt(dst []byte, off int64) (int, error) {
	return f.file.ReadAt(dst, off)
}

// Read block from file
func (f *File) ReadBlock(num uint32) (*block.Block, error) {
	// Get offset
	fmt.Println("num: ", num)
	off := int64((num - 1) * block.BlockSize)

	// If block offset is > f.Size()
	// we have new block, not fsynced to file yet

	// Read data from file
	buf := make([]byte, block.BlockSize)

	n, err := f.file.ReadAt(buf, off)
	if n != block.BlockSize {
		return nil, fmt.Errorf("[ReadBlock] read wrong number of bytes. Expected %d, got %d", block.BlockSize, n)
	}

	// Create and decode block
	b := &block.Block{Num: num}

	err = b.Decode(buf)
	if err != nil {
		return nil, err
	}

	if f.LastBlock.Num < b.Num {
		f.LastBlock = b
	}

	return b, err
}
