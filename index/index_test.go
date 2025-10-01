package index

import (
	"bytedb/tests"
	"fmt"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, _ := os.OpenFile("test.file", os.O_RDWR|os.O_CREATE, 0644)
	defer os.Remove("test.file")

	indexes, err := Open(f, 1)
	tests.AssertEqual(t, err, nil)

	fmt.Println(indexes.blocks)
}
