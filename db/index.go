package db

import (
	bit "bytedb/lib/bitbox"
	"sync"
)

const IndexSize = 16 // size in bytes

// DataClass
type IndexKey struct {
	Hash   uint64
	Offset uint32
	Span   uint16
	Flag   uint16
}

// Index block header
type IndexHeader struct {
	Tombstones uint8 // number of deleted keys
}

// Index manages file index blocks and their headers
type Index struct {
	file *File
	mu   sync.Mutex

	FirstID uint32 // ID of first index block
	LastID  uint32 // ID of last index block

	// index block headers
	Headers map[uint32]*IndexHeader
}

// Add index
func (i *Index) Add(idx *IndexKey) (*Block, error) {
	id := i.BlockID(idx)

	for {
		// get block
		b, err := i.Block(id)
		if err != nil {
			return nil, err
		}

		// write to it
		ok := i.write(idx, b)
		if ok {
			// successfully written
			return nil, nil
		}

		// block was full, iterate and try next one
		id++
		if id > i.LastID {
			// no blocks left, we need to return and reindex
			return nil, nil
		}
	}
}

// Get block ID for index
func (i *Index) BlockID(idx *IndexKey) uint32 {
	id := i.FirstID
	id += uint32(idx.Hash % uint64(i.LastID))

	return id
}

// Write index to block. It will write whole index or nothing at all.
// Return false if nothing was written.
func (i *Index) write(idx *IndexKey, block *Block) bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.SpaceLeft(block) {
		// write data
		return true
	}

	return false
}

// Get index block from file
func (i *Index) Block(offset uint32) (*Block, error) {
	// read block from memory
	b := i.file.blocks[offset]

	if b != nil && i.SpaceLeft(b) {
		// block found, return it
		return b, nil
	}

	// read it from file
	_, err := i.file.Read(b)
	if err != nil {
		return nil, err
	}

	// read headers
	h := &IndexHeader{}
	b.Read(0, bit.BytesPtr(&h))
	i.Headers[b.ID] = h

	return b, nil
}

func (i *Index) Header(id uint32) *IndexHeader {
	return nil
}

// Check if block has enough space for index
func (i *Index) SpaceLeft(block *Block) bool {
	h := i.Header(block.ID)

	if h.Tombstones > 0 || block.SpaceLeft() >= IndexSize {
		return true
	}

	return false
}
