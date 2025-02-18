package db

import (
	"slices"
	"testing"
)

type CustomByte []byte
type CustomInt  []int64

type CustomArrByte [32]byte
type CustomArrInt  [32]int32

// Helper for encoding/decoding slices.
func EncodeDecodeSlice[T comparable](elem, result []T, t *testing.T) {
	raw, _ := Encode(elem)
	Decode(&raw, &result)

	if !slices.Equal(elem, result) { 
		t.Errorf("Expected \n to get %v,\nbut got %v", elem, result) 
	}
}

func EncodeDecode[T comparable](elem, result T, t *testing.T) {
	raw, _ := Encode(elem)
	Decode(&raw, &result)

	if elem != result { 
		t.Errorf("Expected \n to get %v,\nbut got %v", elem, result) 
	}
}

func TestEncodeDecode(t *testing.T) {
	// Basic types.
	EncodeDecode(int8(10),     int8(0),  t)
	EncodeDecode(int16(100),   int16(0), t)
	EncodeDecode(int32(1000),  int32(0), t)
	EncodeDecode(int64(10000), int64(0), t)

	EncodeDecode(uint8(10),     uint8(0),  t)
	EncodeDecode(uint16(100),   uint16(0), t)
	EncodeDecode(uint32(1000),  uint32(0), t)
	EncodeDecode(uint64(10000), uint64(0), t)

	// Slices.
	EncodeDecodeSlice([]byte{1, 2, 3}, []byte{}, t)

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
	EncodeDecodeSlice(CustomInt{1, 2, 3, 4}, CustomInt{}, t)
	EncodeDecodeSlice(CustomByte("byte slice"), CustomByte{}, t)

	// Arrays.
	EncodeDecode(CustomArrByte{1,2,3}, CustomArrByte{}, t)
	EncodeDecode(CustomArrInt{1,2,3},  CustomArrInt{},  t)
}
