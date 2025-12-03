package db

const BlockSize = 4096

type Block struct {
	ID   uint32 // position in file
	Off  uint16 // current offset
	Data []byte // raw 4 KB data
}

// Create new block
func NewBlock(id uint32) *Block {
	b := &Block{Data: make([]byte, BlockSize, BlockSize), ID: id}
	return b
}

// Copy bytes from buf to block.
// It returns the number of bytes copied.
func (b *Block) Write(buf []byte) int {
	// Check if we have any space left. Partial writes are acceptable.
	if b.Off >= BlockSize {
		return 0
	}

	n := copy(b.Data[b.Off:], buf)
	b.Off += uint16(n)

	return n
}

// Copy bytes from block, starting at offset, into dst.
// It returns the number of bytes copied.
func (b *Block) Read(offset int, dst []byte) int {
	// Check offset overflow
	if offset >= len(b.Data) {
		return 0
	}

	return copy(dst, b.Data[offset:])
}

// Check how much space we have
func (b *Block) SpaceLeft() int {
	return BlockSize - int(b.Off)
}
