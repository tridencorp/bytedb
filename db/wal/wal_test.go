package wal

import (
	"bucketdb/tests"
	"os"
	"testing"
)

func TestWrite(t *testing.T) {
	wal, _ := Open("test.wal", 14_000_000)
	defer os.Remove("test.wal")

	go func() {
		data := make([]byte, 10)
		for i := 0; i < 1_000_000; i++ { 
			wal.Logs <- data 
		}
		close(wal.Logs)
	}()

	wal.Start(20)

	// data size + length prefix (10_000_000 + 4_000_000)
	tests.Assert(t, 14_000_000, wal.file.Offset) 
}
