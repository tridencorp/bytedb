package mmap

import (
	"os"

	"golang.org/x/sys/unix"
)

type Mmap struct {
	file   *os.File
	data   []byte
	offset uint64
}

// Mmap file.
func Open(file *os.File, size, flags int) (*Mmap, error) {
	if flags == 0 {
		flags = unix.PROT_READ | unix.PROT_WRITE
	}

	data, err := unix.Mmap(int(file.Fd()), 0, size, flags, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	mmap := &Mmap{file: file, data: data, offset: 0}
	return mmap, nil
}
