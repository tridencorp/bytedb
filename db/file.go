package db

import (
	"bytedb/block"
	"bytedb/collection"
	"bytedb/common"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/log"
)

const (
	NumOfHeaderBlocks = 1
	IndexSize         = 24
)

// Offset keeps information about the location of the data.
type Offset struct {
	FileID uint32
	Start  uint32
	Size   uint32
	Hash   [8]byte
}

// DataClass
type FileHeader struct {
	NumOfIndexBlocks uint32
	NumOfDataBlocks  uint32
}

type File struct {
	ID        int
	file      *os.File
	blockSize int64
	Header    FileHeader

	// Blocks currently keeped in memory
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
		DataBlocks:  make(map[uint32]*block.Block, 1_000),
		IndexBlocks: make(map[uint32]*block.Block, 1_000),
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
	fmt.Println(key, " --- ", val)

	// We are always writing new data to last block (blocks)
	lastBlock := NumOfHeaderBlocks + f.Header.NumOfIndexBlocks + f.Header.NumOfDataBlocks

	// Try to get last block from memory. Once read, last block should always
	// be keeped in memory.
	b, found := f.DataBlocks[lastBlock]

	// If not found, we must read it from file
	if !found {
		b = &block.Block{}

		if f.Size() >= int64(lastBlock*block.BlockSize) {
			var err error

			b, err = f.ReadBlock(lastBlock)
			if err != nil {
				log.Error(err.Error())
				return err
			}
		}

		f.DataBlocks[lastBlock] = b
		fmt.Println("block: ", b)
	}

	fmt.Println("flag 1")

	// index block
	idx, err := f.GetAndReserveIndex(key.Hash)
	if err != nil {
		return err
	}

	fmt.Println(idx, " -- index block")
	// index
	// data
	// file header
	// index stats
	// tests (1, 10, 100 mln keys)
	return nil
}

func (f *File) GetAndReserveIndex(hash uint64) (*block.Block, error) {
	// get block number for hash
	num := hash % uint64(f.Header.NumOfIndexBlocks)
	num += NumOfHeaderBlocks // add space for file header

	fmt.Println("flag 1")
	// Get block for hash
	b, found := f.IndexBlocks[uint32(num)]

	// Load block from disk
	if !found {
		var err error

		// Load from disk
		b, err = f.ReadBlock(uint32(num * block.BlockSize))
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}

		f.IndexBlocks[uint32(num)] = b
		fmt.Println("Block Loaded: ", b)
	}

	// Check if space left, iterate till we find block with free space
	if b.SpaceLeft() >= IndexSize {
		// Reserve space for index. Each block has fixed number
		// of indexes, so we need to reserve space for one.
		b.Header.Len += 1
		return b, nil
	}

	return nil, nil
}

// Allocate space for header and indexes.
// Set default headers.
func (f *File) Init() error {
	ptr := common.BytesPtr(&f.Header)

	// Read bytes directly to file header
	n, _ := f.file.ReadAt(ptr, 0)

	// Check if file was already initialized
	if n == 0 || f.Header.NumOfIndexBlocks == 0 {
		f.Header.NumOfIndexBlocks = 10 // default number of index blocks
		return f.Resize((NumOfHeaderBlocks + 10) * block.BlockSize)
	}

	return nil
}

// Read data from file into dst, starting from given offset.
func (f *File) ReadAt(dst []byte, off int64) (int, error) {
	return f.file.ReadAt(dst, off)
}

// Write data to given block number. If there won't be any space
// left in the block, it will return -1.
// func (f *File) WriteBlock(num int64, data []byte) (int, error) {
// 	// If block size is not set, we are dealing with normal file
// 	// which doesn't operate on our blocks.
// 	if f.blockSize == 0 {
// 		return 0, fmt.Errorf("wrong file type, cannot read blocks")
// 	}

// 	// Read block and check if we have enough free space.
// 	// TODO: v1: We will add option to keep this in memory.
// 	block, err := f.ReadBlock(num)
// 	if err != nil {
// 		return 0, err
// 	}

// 	if block.isFull(int(block.footer.Len) + len(data)) {
// 		return -1, nil
// 	}

// 	block.Write(data)

// 	// Write entire block back to the file.
// 	n, err := f.file.WriteAt(block.data, block.offset)
// 	return n, err
// }

// Read block from file
func (f *File) ReadBlock(num uint32) (*block.Block, error) {
	// Get offset
	off := int64((num - 1) * block.BlockSize)

	// Read data
	buf := make([]byte, block.BlockSize)

	n, err := f.file.ReadAt(buf, off)
	if n == block.BlockSize {
		return nil, fmt.Errorf("read wrong number of bytes. Expected %d, got %d", block.BlockSize, n)
	}

	// Create block
	b := &block.Block{}

	copy(common.BytesPtr(&b.Header), buf[0:block.HeaderSize])
	copy(b.Data[:], buf[block.HeaderSize:])

	return b, err
}
