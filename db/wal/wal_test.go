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
	tests.Assert(t, 14_000_000, wal.file.WriteOffset) 
}

func TestMap(t *testing.T) {
	wal, _ := Open("test.wal", 2_000_000)
	defer os.Remove("test.wal")

	data := []byte("Hello Wal :D")
	for i := 0; i < 99_000; i++ { 
		wal.write(data)
	}

	counter := 0
	count   := func(log []byte) { counter += 1 } 

	wal.Map(count)
	tests.Assert(t, 99_000, counter)
}
