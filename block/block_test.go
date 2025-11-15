package block

import (
	"bytedb/tests"
	"testing"
)

func TestBlockSpaceLeft(t *testing.T) {
	buf := make([]byte, 1096)

	b := Block{}
	b.Write(buf)

	want := DataSize - len(buf)
	tests.AssertEqual(t, want, b.SpaceLeft())
}

// func BenchmarkBlockWriteRead2(b *testing.B) {
// 	a := [500]TR{}

// 	for i := 0; i < b.N; i++ {
// 		for i := 0; i < 1_000_000; i++ {
// 			copy(a[1:], a[2:])
// 			a[1] = TR{}
// 		}
// 	}
// }
