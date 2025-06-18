package db

import (
	"bucketdb/tests"
	"testing"
)

func TestFooterRead(t *testing.T) {
	b := Block{data: []byte{7, 0, 0, 0}}
	s := uint32(0)
	b.ReadFooter(ToBytes(&s))

	tests.Assert(t, 7, s)
}

func TestFooterWrite(t *testing.T) {
	b := Block{data: []byte{0, 0, 0, 0}}
	s := uint32(7)

	b.WriteFooter(ToBytes(&s))
	tests.AssertEqual(t, []byte{7, 0, 0, 0}, b.data)
}

func TestBlockWriteRead(t *testing.T) {
	b := NewBlock(20)
	a := uint32(1337)

	for i := 0; i < 10; i++ {
		b.Write(ToBytes(&a))
	}

	s := uint32(0)
	b.ReadFooter(ToBytes(&s))
	tests.Assert(t, 16, s)

	for i := 0; i < 4; i++ {
		res := uint32(0)
		b.Read(ToBytes(&res))
		tests.Assert(t, a, res)
	}
}
