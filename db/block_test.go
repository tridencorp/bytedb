package db

import (
	"bucketdb/tests"
	"testing"
)

func TestFooterRead(t *testing.T) {
	b := Block{data: []byte{7, 0, 0, 0}}
	f := b.ReadFooter()

	tests.Assert(t, 7, f.Size)
}

func TestFooterWrite(t *testing.T) {
	b := Block{data: []byte{0, 0, 0, 0}}
	f := BlockFooter{Size: 7}

	b.WriteFooter(&f)
	tests.AssertEqual(t, []byte{7, 0, 0, 0}, b.data)
}

func TestBlockWrite(t *testing.T) {
	b := NewBlock(20)
	a := uint32(1337)

	for i := 0; i < 10; i++ {
		b.Write(ToBytes(&a))
	}

	f := b.ReadFooter()
	tests.Assert(t, 16, f.Size)
}
