package wal

import (
	"bucketdb/db/mmap"
	"fmt"
	"os"
	"time"
)

type Wal struct {
	file   *os.File
	data   *mmap.Mmap


	Logs chan []byte
}

// Open the wal file that we will be writing to.
// Each wal file will be truncated to given size in MB.
func Open(path string, size int64) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	mmap, err := mmap.Open(file, int(size), 0)
	mmap.Resize(1024 * 1024 * size)

	w := &Wal{file: file, data: mmap, Logs: make(chan []byte, 1000)}
	return w, nil
}

// Start main loop responsible for writing data to wal file.
func (w *Wal) Start(timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		// Got new data, write it to wal file.
		case data, open := <-w.Logs:
			if !open {
				w.data.Sync()
				return 
			}
			w.write(data)

		// Periodically call msync.
		case _ = <-ticker.C:
			err := w.data.Sync()
			if err != nil {
				fmt.Println(err)
			}
		}
	}	
}

func (w *Wal) write(bytes []byte) error {
	n := w.data.Write(bytes)
	if n != len(bytes) {
		return fmt.Errorf("Mmap write error, expected %d bytes, %d were written", len(bytes), n)
	}

	return nil
}
