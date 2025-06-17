package db

import (
	"fmt"
	"unsafe"
)

// Default file block.
type Block struct {
	data   []byte
	offset int64
}

type BlockFooter struct {
	Size int32
}

func NewBlock(size int64) *Block {
	return &Block{data: make([]byte, size)}
}

// Write data to block.
func (b *Block) Write(data []byte) (int, error) {
	f := b.ReadFooter()

	// Check if we have enough space in block.
	if int(f.Size)+len(data) > len(b.data)-int(unsafe.Sizeof(*f)) {
		return 0, fmt.Errorf("not enough space in block")
	}

	// Copy data to block.
	copy(b.data[f.Size:], data)

	// Update block size.
	f.Size += int32(len(data))
	b.WriteFooter(f)

	return 0, nil
}

// Read block footer.
func (b *Block) ReadFooter() *BlockFooter {
	f := &BlockFooter{}
	ptr := ToBytes(f)

	// Read footer from the end of block.
	i := len(b.data) - len(ptr)
	Decode2(b.data[i:], ptr)

	return f
}

// Write block footer.
func (b *Block) WriteFooter(footer *BlockFooter) {
	// Write footer to the end of the block.
	s := int(unsafe.Sizeof(*footer))
	i := len(b.data) - s

	Decode2(ToBytes(footer), b.data[i:])
}
