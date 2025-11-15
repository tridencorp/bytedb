package block

const BlockSize = 4096
const HeaderSize = 8
const DataSize = uint32(BlockSize - HeaderSize)

// DataStruct
type Header struct {
	Len uint32 // total number of keys
	Off uint32 // current data offset
}

type Block struct {
	Header Header
	Data   [DataSize]byte
}

// Write copied bytes from data into the block.
// It returns the number of bytes copied.
func (b *Block) Write(data []byte) int {
	// Check if we have any space left - allow partial writes
	if int(b.Header.Off) >= len(b.Data) {
		return 0
	}

	n := copy(b.Data[b.Header.Off:], data)
	b.Header.Off += uint32(n)

	return n
}

// Read copies bytes from the block, starting at offset, into dst.
// It returns the number of bytes copied.
func (b *Block) Read(offset int, dst []byte) int {
	// Check offset overflow
	if offset >= len(b.Data) {
		return 0
	}

	return copy(dst, b.Data[offset:])
}

// Check how much space left
func (b *Block) SpaceLeft() uint32 {
	return DataSize - b.Header.Off
}
