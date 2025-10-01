package mmap

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

var ErrRead = errors.New("missing bytes when reading")

type Mmap struct {
	file *os.File
	data []byte

	WriteOffset int
	ReadOffset  int
}

// Mmap file.
func Open(file *os.File, size, prot int) (*Mmap, error) {
	if prot == 0 {
		prot = unix.PROT_READ | unix.PROT_WRITE
	}

	info, _ := file.Stat()

	// Truncate file if it's to small
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
		file: file,
		data: data,
	}

	return mmap, nil
}

// Sync data.
func (m *Mmap) Sync() error {
	return unix.Msync(m.data, unix.MS_SYNC)
}

// Write to mmaped file.
func (m *Mmap) Write(bytes []byte) int {
	n := copy(m.data[m.WriteOffset:], bytes)
	m.WriteOffset += len(bytes)
	return n
}

// Read bytes to dst.
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

// Read n bytes from mmaped file.
func (m *Mmap) Read(n int) ([]byte, error) {
	data := make([]byte, n)
	m.ReadTo(data)
	return data, nil
}

// Resize the underlying file.
// TODO: Set offset properly.
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
	mmap, err := Open(m.file, int(size), 0)
	if err != nil {
		return err
	}

	// Assign new mapping.
	*m = *mmap
	return nil
}
