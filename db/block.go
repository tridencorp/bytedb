package db

const BlockSize = 4096

// Block represents a physical 4 KB section of a file
type Block struct {
	DataOffset uint16
	Bytes      [BlockSize]byte
}

func NewBlock(data []byte, cap int32) *Block {
	b := &Block{}
	return b
}
