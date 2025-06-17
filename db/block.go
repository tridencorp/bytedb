package db

import (
	"fmt"
	"unsafe"
)

type Block []byte

type BlockFooter struct {
	Offset uint32
}

func (b *Block) ReadFooter() *BlockFooter {
	f := &BlockFooter{}
	s := unsafe.Sizeof(f)
	fmt.Println("size: ", s)

	Decode2(*b, f)
	return f
}
