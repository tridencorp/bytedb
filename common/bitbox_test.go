package common

import (
	"bytedb/tests"
	"testing"
)

func TestDecodeUin64(t *testing.T) {
	u1 := uint64(666)
	u2 := uint64(0)

	buf := Encode(&u1)
	Decode(buf, &u2)

	tests.Assert(t, u1, u2)
}

func TestDecodeFloat64(t *testing.T) {
	f1 := float32(123.12301)
	f2 := float32(0)

	buf := Encode(&f1)
	Decode(buf, &f2)

	tests.Assert(t, f1, f2)
}
