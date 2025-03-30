package wal

import (
	"bucketdb/db/mmap"
	"fmt"
	"os"
	"time"
)

type Wal struct {
	file *mmap.Mmap
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

	w := &Wal{ file: mmap, Logs: make(chan []byte, 1000) }
	return w, nil
}

// Start main loop responsible for writing data to wal file.
func (w *Wal) Start(timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case data, open := <-w.Logs:
			// Got new data, write it to the wal file.
			// If channel was closed, sync data and return.
			if !open { w.file.Sync(); return }
			w.file.Write(data)

		case _ = <-ticker.C:
			// Periodically call msync and flush data to file.
			err := w.file.Sync()
			if err != nil { fmt.Println(err) }
		}
	}	
}
