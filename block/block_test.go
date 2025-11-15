package block

import (
	"testing"
)

// func TestBlockWriteRead(t *testing.T) {
// 	foo := []byte("foo")
// 	bar := make([]byte, 3)

// 	b := Block{}
// 	b.Write(foo)
// 	b.Read(0, bar)

// 	tests.AssertEqual(t, foo, bar)
// }

type TR struct {
	a uint64
	b uint64
}

func BenchmarkBlockWriteRead2(b *testing.B) {
	a := [500]TR{}

	for i := 0; i < b.N; i++ {
		for i := 0; i < 1_000_000; i++ {
			copy(a[1:], a[2:])
			a[1] = TR{}
		}
	}
}
