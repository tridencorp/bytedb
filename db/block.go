package db

import (
	"fmt"
	"unsafe"
)

// Default file block.
type Block struct {
	// I dont like embedded structs but in this case
	// it make sense. I don't want to map each field
	// separately.
	*blockFooter

	data   []byte
	offset int64
	Cap    int32

	ReadOffset int
}

type blockFooter struct {
	Len int32
}

func NewBlock(data []byte, cap int32) *Block {
	b := &Block{
		data:       data,
		Cap:        cap,
		ReadOffset: 0,
	}

	// Points footer directly to underlying data bytes.
	PointTo(&b.blockFooter, b.data[len(b.data)-int(unsafe.Sizeof(*b.blockFooter)):])

	return b
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
