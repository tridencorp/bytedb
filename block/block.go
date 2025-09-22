package block

import (
	"unsafe"
)

const BlockSize = 4096
const HeaderSize = int(unsafe.Sizeof(BlockHeader{}))
const DataSize = BlockSize - HeaderSize

// @Data Struct
type BlockHeader struct {
	Offset uint32
}

// @Data Struct
type Block struct {
	Header BlockHeader
	Data   [DataSize]byte
}

// Write function writes bytes from data to block.
// It returns the numbe of bytes written.
func (b *Block) Write(data []byte) int {
	// Check offset overflow
	if int(b.Header.Offset) > len(b.Data) {
		return 0
	}

	n := copy(b.Data[b.Header.Offset:], data)
	b.Header.Offset += uint32(n)

	return n
}

// Read function reads bytes from block into a dst.
// It returns the number of bytes read.
func (b *Block) Read(offset int, dst []byte) int {
	// Check offset overflow
	if offset >= len(b.Data) {
		return 0
	}

	return copy(dst, b.Data[offset:])
}
