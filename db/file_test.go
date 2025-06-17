package db

import (
	"bucketdb/tests"
	"os"
	"testing"
)

func TestFileWriteReadBlock(t *testing.T) {
	f, _ := OpenFile(".index.idx", os.O_RDWR|os.O_CREATE)
	defer os.Remove(".index.idx")

	f.Resize(100_000)
	f.WriteBlock(10, []byte("Hello database"))

	buf, _ := f.ReadBlock(10)
	tests.Assert(t, string([]byte("Hello database")), string(buf[:14]))
}
