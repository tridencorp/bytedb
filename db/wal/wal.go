package wal

import (
	"fmt"
	"os"
	"time"
)

type Wal struct {
	file *os.File
	Log chan []byte
}

// Open wal file that we will be writing to.
func Open(path string) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	w := &Wal{ file: file, Log: make(chan []byte, 1000)}
	return w, nil
}

// Start main loop responsible for writing data to wal file.
func (w *Wal) Start(timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Microsecond)
	defer ticker.Stop()

	for {
		select {
		// Got new data, write it to wal file.
		case data, open := <- w.Log:
			if !open { return }
			fmt.Println(data)

		// Periodically call msync.
		case _ = <-ticker.C:
			fmt.Println("--- doing msync ---")
		}
	}
}