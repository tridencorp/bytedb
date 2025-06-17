package db

import (
	"bucketdb/tests"
	"testing"
)

func TestReadFooter(t *testing.T) {
	b := Block{data: []byte{7, 0, 0, 0}}
	f := b.ReadFooter()

	tests.Assert(t, 7, f.Offset)
}
