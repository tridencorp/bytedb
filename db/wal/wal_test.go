package wal

import (
	"bucketdb/tests"
	"os"
	"testing"
)

func TestWrite(t *testing.T) {
	wal, _ := Open("test.wal", 10)
	defer os.Remove("test.wal")

	go func() {
		data := make([]byte, 10)
		for i:=0; i < 1_000_000; i++ {
			wal.Logs <-data 
		}

		close(wal.Logs)
	}()

	wal.Start(50)
	tests.Assert(t, wal.file.Offset, 10_000_000)
}
