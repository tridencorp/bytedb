package index

import (
	"unsafe"
)

const BlockSize = 4096
const HeaderSize = int(unsafe.Sizeof(Header{}))
const DataSize = BlockSize - HeaderSize

type Header struct {
	Offset uint32
}

type Block struct {
	Header Header
	Data   [DataSize]byte
}

// Write bytes to block and return the number of bytes written
func (b *Block) Write(data []byte) int {
	// Check if block has enough space
	if int(b.Header.Offset)+len(data) > cap(b.Data) {
		return 0
	}

	n := copy(b.Data[b.Header.Offset:], data)
	b.Header.Offset += uint32(n)

	return n
}
