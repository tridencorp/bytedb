package block

import (
	"bucketdb/tests"
	"testing"
)

func TestBlockWriteRead(t *testing.T) {
	foo := []byte("foo")
	bar := make([]byte, 3)

	b := Block{}
	b.Write(foo)
	b.Read(0, bar)

	tests.AssertEqual(t, foo, bar)
}
