package wal

import (
	"os"
	"testing"
	"time"
)

func TestWrite(t *testing.T) {
	wal, _ := Open("test.wal", 1)
	defer os.Remove("test.wal")

	go func() {
		wal.Log <- []byte("incomming data")
		time.Sleep(40 * time.Microsecond)
		close(wal.Log)
	}()

	wal.Start(20)
}
