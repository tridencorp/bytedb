package mmap

import "os"

type Mmap struct {
	file   *os.File
	data   []byte
	offset uint64
}
