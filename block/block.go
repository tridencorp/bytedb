package block

import (
	"bytedb/common"
	"fmt"
)

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

	Num uint32
}

// Decode block from bytes
func (b *Block) Decode(buf []byte) error {
	if len(buf) < HeaderSize {
		return fmt.Errorf("buffer too small: got %d, need at least %d", len(buf), HeaderSize)
	}

	copy(common.BytesPtr(&b.Header), buf[0:HeaderSize])
	copy(b.Data[:], buf[HeaderSize:])

	return nil
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
func (b *Block) Read(off int, dst []byte) int {
	// Check offset overflow
	if off >= len(b.Data) {
		return 0
	}

	return copy(dst, b.Data[off:])
}

// Check how much space left
func (b *Block) SpaceLeft() uint32 {
	return DataSize - b.Header.Off
}
