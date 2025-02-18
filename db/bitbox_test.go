package db

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
)

type CustomByte []byte
type CustomInt  []int64

// Helper for encoding/decoding slices.
func EncodeDecodeSlice[T comparable](elem, result []T, t *testing.T) {
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

	val4  := []byte{1, 2, 3}
	raw, _ = Encode(val4)

	val4 = []byte{}
	Decode(raw, &val4)

	if !bytes.Equal(val4, []byte{1, 2, 3}) { 
		t.Errorf("Expected \n to get %d,\nbut got %d", []byte{1, 2, 3}, val4) 
	}

	EncodeDecodeSlice([]float64{11.11, 22.22, 33.33}, []float64{}, t)
	EncodeDecodeSlice([]float32{11.11, 22.22, 33.33}, []float32{}, t)

	EncodeDecodeSlice([]int64{11, 22, 33}, []int64{}, t)
	EncodeDecodeSlice([]int32{11, 22, 33}, []int32{}, t)
	EncodeDecodeSlice([]int16{11, 22, 33}, []int16{}, t)
	EncodeDecodeSlice([]int8{11, 22, 33},  []int8{},  t)

	EncodeDecodeSlice([]uint64{11, 22, 33}, []uint64{}, t)
	EncodeDecodeSlice([]uint32{11, 22, 33}, []uint32{}, t)
	EncodeDecodeSlice([]uint16{11, 22, 33}, []uint16{}, t)
	EncodeDecodeSlice([]uint8{11, 22, 33},  []uint8{},  t)
}

func TestDecodeEncodeCustom(t *testing.T) {
	val := CustomInt{1, 2, 3}
	raw, _ := Encode(val)

	fmt.Println(raw)

	// val = CustomInt{}
	// Decode(raw, &val)

	// fmt.Println(val)
}