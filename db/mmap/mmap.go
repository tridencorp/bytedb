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

	// Use file size if necessary.
	if size == 0 {
		info, _ := file.Stat()
		size = int(info.Size())
	}

	data, err := unix.Mmap(int(file.Fd()), 0, size, flags, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	mmap := &Mmap{file: file, data: data, offset: 0}
	return mmap, nil
}

// Sync data.
func (m *Mmap) Sync() error {
	return unix.Msync(m.data, unix.MS_SYNC)
}

// Write to mmaped file.
func (m *Mmap) Write(bytes []byte) int {
	n := copy(m.data[m.offset:], bytes)
	m.offset += uint64(len(bytes))

	return n
}

// Resize the underlying file.
func (m *Mmap) Resize(size int64) error {
	// Let's sync data before unmapping the file.
	err := m.Sync()
	if err != nil {
		return err
	}

	// To be safe we must unmap file before resize.
	err = unix.Munmap(m.data)
	if err != nil {
		return err
	}

	// Resize file
	err = m.file.Truncate(size)
	if err != nil {
		return err
	}

	// Mmap file again.
	mmap, err := Open(m.file, int(size), 0)
	if err != nil {
		return err
	}

	// Assign new mapping.
	*m = *mmap
	return nil 
}