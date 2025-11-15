package block

const BlockSize = 4096
const HeaderSize = 8
const DataSize = BlockSize - HeaderSize

// DataStruct
type Header struct {
	Len  uint32 // total number of keys
	Size uint32 // current data size in bytes
}

type Block struct {
	Header Header
	Data   [DataSize]byte
}

// Write copied bytes from data into the block.
// It returns the number of bytes copied.
func (b *Block) Write(data []byte) int {
	// Check if we have any space left - allow partial writes
	if int(b.Header.Size) >= len(b.Data) {
		return 0
	}

	n := copy(b.Data[b.Header.Size:], data)
	b.Header.Size += uint32(n)

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
