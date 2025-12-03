package db

import (
	"bytedb/tests"
	"testing"
)

func TestBlockSpaceLeft(t *testing.T) {
	buf := make([]byte, 1096)

	b := NewBlock(1)
	b.Write(buf)

	want := BlockSize - len(buf)
	tests.AssertEqual(t, want, b.SpaceLeft())
}

type BlockX struct {
	ID   uint32
	Off  uint32
	Size uint32
	Data [4096]byte
}

func BenchmarkMapIteration30K(b *testing.B) {
	n := 30_000

	m := make(map[uint32]*BlockX, n)

	for i := 0; i < n; i++ {
		m[uint32(i)] = &BlockX{ID: uint32(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k, v := range m {
			_ = k
			_ = v
		}
	}
}
