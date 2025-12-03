package db

const BlockSize = 4096

// Block represents a physical 4 KB section of a file
type Block struct {
	ID    uint32
	bytes [BlockSize]byte
	off   uint32
}

// Copy bytes from data to block.
// Returns the number of bytes copied.
func (b *Block) Write(data []byte) int {
	// Check if we have any space left. Partial writes are acceptable.
	if int(b.off) >= len(b.bytes) {
		return 0
	}

	n := copy(b.bytes[b.off:], data)
	b.off += uint32(n)

	return n
}

// Copy bytes from block into dst.
// It returns the number of bytes copied.
func (b *Block) Read(dst []byte) int {
	return copy(dst, b.bytes[b.off:])
}

// Check how much space we have
func (b *Block) SpaceLeft() int {
	return len(b.bytes) - int(b.off)
}
