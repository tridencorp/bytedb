package db

import (
	"bytes"
	"slices"
	"testing"
)

type UserType []byte

func sliceEncodeDecode[T comparable](elem, result []T, t *testing.T) {
	raw, _ := Encode(elem)
	Decode(raw, &result)

	if !slices.Equal(elem, result) { 
		t.Errorf("Expected \n to get %v,\nbut got %v", elem, result) 
	}
}

func TestEncodeDecode(t *testing.T) {
	// int8
	val := int8(10)
	raw, _ := Encode(val)

	val = 0
	Decode(raw, &val)

	if val != 10 { t.Errorf("Expected %d, got %d", 10, val) }

	// int32
	val1  := int32(100)
	raw, _ = Encode(val1)

	val1 = 0
	Decode(raw, &val1)

	if val1 != 100 { t.Errorf("Expected %d, got %d", 100, val1) }

	// int64
	val2  := int64(1000)
	raw, _ = Encode(val2)

	val2 = 0
	Decode(raw, &val2)

	if val2 != 1000 { t.Errorf("Expected %d, got %d", 1000, val2) }

	// Custom types
	val3  := UserType("value")
	raw, _ = Encode(val3)

	val3 = UserType{}
	Decode(raw, &val3)

	if !bytes.Equal(val3, []byte("value")) { 
		t.Errorf("Expected \n to get %d,\nbut got %d", []byte("value"), val3) 
	}

	val4  := []byte{1, 2, 3}
	raw, _ = Encode(val4)

	val4 = []byte{}
	Decode(raw, &val4)

	if !bytes.Equal(val4, []byte{1, 2, 3}) { 
		t.Errorf("Expected \n to get %d,\nbut got %d", []byte{1, 2, 3}, val4) 
	}

	sliceEncodeDecode([]int64{11, 22, 33}, []int64{}, t)
}
