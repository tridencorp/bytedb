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
	*footer

	data   []byte
	offset int64
	Cap    int32

	ReadOffset int
}

type footer struct {
	Len int32
}

func NewBlock(data []byte, cap int32) *Block {
	b := &Block{
		data:       data,
		Cap:        cap,
		ReadOffset: 0,
	}

	// Points footer directly to underlying data bytes.
	off := len(b.data) - int(unsafe.Sizeof(*b.footer))
	PointTo(&b.footer, b.data[off:])

	return b
}

// Write data to block.
func (b *Block) Write(src []byte) (int, error) {
	// Check if we have enough space in block.
	if b.isFull(int(b.footer.Len) + len(src)) {
		return 0, fmt.Errorf("EOF")
	}

	// Copy data to block.
	copy(b.data[b.footer.Len:], src)

	// Update block size.
	b.footer.Len += int32(len(src))

	return 0, nil
}

// Read data from block. Return false if there is not enough data.
func (b *Block) Read(dst []byte) bool {
	if b.isFull(b.ReadOffset + len(dst)) {
		return false
	}

	n := copy(dst, b.data[b.ReadOffset:])
	if n != len(dst) {
		return false
	}

	b.ReadOffset += len(dst)
	return true
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
