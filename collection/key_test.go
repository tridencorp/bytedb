package collection

import (
	"bytedb/tests"
	"testing"
)

func TestKey(t *testing.T) {
	k := NewKey([]byte("Hello"))
	tests.AssertNot(t, 0, k.Hash)
}
