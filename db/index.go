package db

import (
	"bytedb/block"
	bit "bytedb/lib/bitbox"
	"sync"
)

const IndexSize = 16 // size in bytes

// Header used in index blocks
type IndexHeader struct {
	Tombstones uint8
}

// Index manages file index blocks and their headers
type Index struct {
	file *File
	mu   sync.Mutex

	FirstID uint32 // ID of first index block
	LastID  uint32 // ID of last index block

	Headers map[uint32]*IndexHeader // map of block ID -> header
}

type IndexKey struct {
	Hash   uint64
	Offset uint32
	Span   uint16
}

// Add index
func (i *Index) Add(idx *IndexKey) (*block.Block, error) {
	// Calculate block ID
	id := i.FirstID
	id += uint32(idx.Hash % uint64(i.LastID))

	for {
		// 1. Get block
		b, err := i.Block(id)
		if err != nil {
			return nil, err
		}

		// 2. Write to it
		ok := i.write(idx, b)
		if ok {
			// Successfully written
			return nil, nil
		}

		// 3. At this point block was full, iterate and try next one
		id++
		if id > i.LastID {
			// No blocks left, we need to reindex
			return nil, nil
		}
	}
}

// Write index to block. It will write whole index or nothing at all.
// Return false if nothing was wrote.
func (i *Index) write(idx *IndexKey, block *block.Block) bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.SpaceLeft(block) {
		// write data
		return true // example only
	}

	return false
}

// Get block from file
func (i *Index) Block(offset uint32) (*block.Block, error) {
	// 1. Try to read block from memory
	b := i.file.blocks[offset]

	if b != nil && i.SpaceLeft(b) {
		return b, nil
	}

	// 2. If block is not in memory, read it from file
	_, err := i.file.Read(b)
	if err != nil {
		return nil, err
	}

	// Read headers
	h := &IndexHeader{}
	b.Read(0, bit.BytesPtr(&h))
	i.Headers[b.ID] = h

	return b, nil
}

func (i *Index) Header(id uint32) *IndexHeader {
	return nil
}

// Check if block has space for index
func (i *Index) SpaceLeft(block *block.Block) bool {
	h := i.Header(block.ID)

	if h.Tombstones > 0 || block.SpaceLeft() >= IndexSize {
		return true
	}

	return false
}
