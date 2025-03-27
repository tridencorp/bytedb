package wal

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

type Wal struct {
	file   *os.File
	data   []byte
	offset uint64

	Log chan []byte
}

// Open the wal file that we will be writing to.
// Each wal file will be truncated to given size in MB.
func Open(path string, size int64) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Truncate in MB.
	size = 1024 * 1024 * size
	err = file.Truncate(size)
	if err != nil {
		return nil, err
	}

	data, err := unix.Mmap(int(file.Fd()), 0, int(size), unix.PROT_READ | unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	w := &Wal{file: file, data: data, Log: make(chan []byte, 1000)}
	return w, nil
}

// Start main loop responsible for writing data to wal file.
func (w *Wal) Start(timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		// Got new data, write it to wal file.
		case data, open := <- w.Log:
			if !open { 
				unix.Msync(w.data, unix.MS_SYNC)
				return 
			}
			w.write(data)

		// Periodically call msync.
		case _ = <-ticker.C:
			err := unix.Msync(w.data, unix.MS_SYNC)
			if err != nil {
				fmt.Println(err)
			}
		}
	}	
}

func (w *Wal) write(bytes []byte) {
	copy(w.data[w.offset:], bytes)
	w.offset += uint64(len(bytes))
}
