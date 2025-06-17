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
	off := len(b.data) - s
	Decode2(b.data[off:], ToBytes(f))

	return f
}
