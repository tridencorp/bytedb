package db

import (
	"bytes"
	"testing"
)

type UserType []byte

func TestEncode(t *testing.T) {
	a := UserType("test")
	data, _ := Encode(a)

	expected := []byte{0,0,0,0,0,0,0,4,116,101,115,116}

	if bytes.Equal(data, expected) {
		t.Errorf("Expected encoded bytes to\nbe  %v\ngot %v", expected, data)
	}
}
