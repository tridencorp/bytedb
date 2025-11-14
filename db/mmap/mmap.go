package mmap

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

var ErrRead = errors.New("missing bytes when reading")

type Mmap struct {
	file       *os.File
	data       []byte
	blockSize  int
	ReadOffset int
}

// Mmap file
func Open(file *os.File, blockSize, size, prot int) (*Mmap, error) {
	if prot == 0 {
		prot = unix.PROT_READ | unix.PROT_WRITE
	}

	size = blockSize * size
	info, _ := file.Stat()

	// Truncate file if it's smaller than size=
	if int(info.Size()) < size {
		err := file.Truncate(int64(size))
		if err != nil {
			return nil, err
		}
	}

	data, err := unix.Mmap(int(file.Fd()), 0, size, prot, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	mmap := &Mmap{
		blockSize: blockSize,
		file:      file,
		data:      data,
	}

	return mmap, nil
}

// Sync data.
func (m *Mmap) Sync() error {
	return unix.Msync(m.data, unix.MS_SYNC)
}

// Write copies data from src into the block at the given number.
// src should include block data.
// Returns the number of bytes copied.
func (m *Mmap) Write(number int, src []byte) int {
	off := number * m.blockSize

	if off >= len(m.data) {
		return 0
	}

	return copy(m.data[off:], src)
}

// Read bytes to dst
func (m *Mmap) ReadTo(dst []byte) error {
	if m.ReadOffset+len(dst) > len(m.data) {
		return io.EOF
	}

	n := copy(dst, m.data[m.ReadOffset:])
	if n != len(dst) {
		return ErrRead
	}

	m.ReadOffset += n
	return nil
}

// Read copies the block at a given number into dst.
// Returns the number of bytes copied or an error otherwise.
func (m *Mmap) Read(number int, dst []byte) int {
	off := number * m.blockSize

	if off >= len(m.data) {
		return 0
	}

	return copy(dst, m.data[off:])
}

// Read reads n blocks starting from offset
func (m *Mmap) ReadN(offset, n int) ([]byte, error) {
	data := make([]byte, n*m.blockSize)
	m.ReadTo(data)
	return data, nil
}

// Resize the underlying file.
func (m *Mmap) Resize(size int64) error {
	// Let's sync data before unmapping the file.
	err := m.Sync()
	if err != nil {
		return err
	}

	// To be safe we must unmap file before resizing.
	err = unix.Munmap(m.data)
	if err != nil {
		return err
	}

	// Resize the file.
	err = m.file.Truncate(size)
	if err != nil {
		return err
	}

	// Mmap file again.
	mmap, err := Open(m.file, int(size), m.blockSize, 0)
	if err != nil {
		return err
	}

	// Assign new mapping.
	*m = *mmap
	return nil
}
