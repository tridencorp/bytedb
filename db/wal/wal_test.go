package wal

import (
	"os"
	"testing"
)

func TestWrite(t *testing.T) {
	wal, _ := Open("test.wal", 100)
	defer os.Remove("test.wal")

	go func() {
		data := make([]byte, 100)

		for i:=0; i < 1_000_000; i++ {
			wal.Log <- data 
		}

		close(wal.Log)
	}()

	wal.Start(50)

}
