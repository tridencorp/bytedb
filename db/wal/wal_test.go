package wal

import (
	"os"
	"testing"
	"time"
)

func TestWrite(t *testing.T) {
	wal, _ := Open("test.wal", 200)
	defer os.Remove("test.wal")

	go func() {
		data := make([]byte, 200)

		for i:=0; i < 1_000_000; i++ {
			wal.Log <- data 
		}

		time.Sleep(40 * time.Microsecond)
		close(wal.Log)
	}()

	wal.Start(50)
}
