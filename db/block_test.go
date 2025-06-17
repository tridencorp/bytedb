package db

import (
	"bucketdb/tests"
	"testing"
)

func TestFooterRead(t *testing.T) {
	b := Block{data: []byte{7, 0, 0, 0}}
	f := b.ReadFooter()

	tests.Assert(t, 7, f.Offset)
}

func TestFooterWrite(t *testing.T) {
	b := Block{data: []byte{0, 0, 0, 0}}
	f := BlockFooter{Offset: 7}

	b.WriteFooter(&f)
	tests.AssertEqual(t, []byte{7, 0, 0, 0}, b.data)
}
