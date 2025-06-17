package db

import (
	"unsafe"
)

type Block struct {
	data []byte
}

type BlockFooter struct {
	Offset uint32
}

func NewBlock(size int64) *Block {
	return &Block{data: make([]byte, size)}
}

// Read block footer.
func (b *Block) ReadFooter() *BlockFooter {
	f := &BlockFooter{}
	s := int(unsafe.Sizeof(*f))

	// Read footer from the end of block.
	i := len(b.data) - s
	Decode2(b.data[i:], ToBytes(f))

	return f
}

// Write block footer.
func (b *Block) WriteFooter(footer *BlockFooter) {
	// Write footer in the end of block.
	s := int(unsafe.Sizeof(*footer))
	i := len(b.data) - s
	Decode2(ToBytes(footer), b.data[i:])
}
