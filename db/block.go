package db

import (
	"fmt"
)

// Default file block.
type Block struct {
	data   []byte
	offset int64
	Len    int32
	Cap    int32

	ReadOffset int
}

func NewBlock(cap int32) *Block {
	return &Block{
		data:       make([]byte, cap),
		Cap:        cap,
		Len:        0,
		ReadOffset: 0,
	}
}

// Write data to block.
func (b *Block) Write(src []byte) (int, error) {
	b.ReadFooter(ToBytes(&b.Len))

	// Check if we have enough space in block.
	if b.isFull(int(b.Len) + len(src)) {
		return 0, fmt.Errorf("EOF")
	}

	// Copy data to block.
	copy(b.data[b.Len:], src)

	// Update block size.
	b.Len += int32(len(src))

	b.WriteFooter(ToBytes(&b.Len))
	return 0, nil
}

// Read data from block.
func (b *Block) Read(dst []byte) (int, error) {
	if b.isFull(b.ReadOffset + len(dst)) {
		return 0, fmt.Errorf("EOF")
	}

	n := copy(dst, b.data[b.ReadOffset:])
	b.ReadOffset += len(dst)
	return n, nil
}

// Read footer from the end of the block.
func (b *Block) ReadFooter(footer []byte) {
	i := len(b.data) - len(footer)
	Decode2(b.data[i:], footer)
}

// Write footer to the end of the block.
func (b *Block) WriteFooter(footer []byte) {
	i := len(b.data) - len(footer)
	Decode2(footer, b.data[i:])
}

func (b *Block) isFull(s int) bool {
	return s > int(b.Cap)-4 // footer size
}
