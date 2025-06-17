package db

import (
	"fmt"
	"unsafe"
)

// Default file block.
type Block struct {
	data   []byte
	offset int64
	Len    int32
	Cap    int32

	ReadOffset int
}

type BlockFooter struct {
	Size int32
}

func NewBlock(cap int32) *Block {
	return &Block{
		data:       make([]byte, cap),
		Cap:        cap - int32(unsafe.Sizeof(BlockFooter{})),
		Len:        0,
		ReadOffset: 0,
	}
}

// Write data to block.
func (b *Block) Write(src []byte) (int, error) {
	f := b.ReadFooter()

	// Check if we have enough space in block.
	if int(f.Size)+len(src) > int(b.Cap) {
		return 0, fmt.Errorf("not enough space in block")
	}

	// Copy data to block.
	copy(b.data[f.Size:], src)

	// Update block size.
	f.Size += int32(len(src))
	b.Len += int32(len(src))
	b.WriteFooter(f)

	return 0, nil
}

// Read data from block.
func (b *Block) Read(dst []byte) (int, error) {
	if b.ReadOffset+len(dst) > int(b.Cap) {
		return 0, fmt.Errorf("EOF")
	}

	n := copy(dst, b.data[b.ReadOffset:])
	b.ReadOffset += len(dst)
	return n, nil
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
