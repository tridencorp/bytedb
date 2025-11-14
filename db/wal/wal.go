package wal

import (
	"bucketdb/db/mmap"
	"fmt"
	"os"
	"time"
	"unsafe"
)

type Wal struct {
	file *mmap.Mmap
	Logs chan []byte
}

// Open the wal file that we will be writing to.
// Each wal file will be truncated to given size (in bytes).
func Open(path string, size int64) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	mmap, err := mmap.Open(file, int(size), 0)
	mmap.Resize(size)

	w := &Wal{file: mmap, Logs: make(chan []byte, 1000)}
	return w, nil
}

// Start main loop responsible for writing data to wal file
// TODO: This shouldn't be here - it's caller responsibility
func (w *Wal) Start(timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		// Got new data, write it to the wal file
		case data, open := <-w.Logs:
			// If channel was closed, sync data and return
			if !open {
				w.file.Sync()
				return
			}
			w.write(data)

		// Periodically call msync and flush data to file
		case _ = <-ticker.C:
			err := w.file.Sync()
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

// Write log to wal file.
func (w *Wal) write(data []byte) {
	// We need a length prefix for each log so we will
	// be able to iterate them.
	size := uint32(len(data))

	ptr := (*[4]byte)(unsafe.Pointer(&size))
	log := make([]byte, 4+len(data))

	copy(log, ptr[:])
	copy(log[4:], data)

	n := w.file.Write(log)
	if n != len(log) {
		fmt.Printf("Wal should write %d bytes, wrote only %d\n", len(log), n)
	}
}

func (w *Wal) Map(fn func(log []byte)) error {
	for {
		len := uint32(0)
		ptr := (*[4]byte)(unsafe.Pointer(&len))

		w.file.ReadTo(ptr[:])

		// No more logs to read
		if len == 0 {
			return nil
		}

		log, _ := w.file.Read(int(len))
		fn(log)
	}
}
