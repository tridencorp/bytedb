package db

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type CustomByte []byte
type CustomInt  []int64

type CustomArrByte [32]byte
type CustomArrInt  [32]int32

// Helper for encoding/decoding slices.
func EncodeDecodeSlice[T comparable](elem []T, result *[]T, t *testing.T) {
	raw, _ := Encode(elem)
	Decode(raw, result)

	if !slices.Equal(elem, *result) { 
		t.Errorf("Expected \n to get %v,\nbut got %v", elem, *result) 
	}
}

func EncodeDecode[T comparable](elem, result *T, t *testing.T) {
	raw, _ := Encode(elem)
	Decode(raw, result)

	if *elem != *result {
		t.Errorf("Expected \n to get %v,\nbut got %v", *elem, *result) 
	}
}

func TestEncodeDecode(t *testing.T) {
	// Basic types.
	v1, v2 := int8(10), int8(0)
	EncodeDecode(&v1, &v2, t)

	v3, v4 := int16(10), int16(0)
	EncodeDecode(&v3, &v4, t)

	v5, v6 := int32(10), int32(0)
	EncodeDecode(&v5, &v6, t)

	v7, v8 := int64(10), int64(0)
	EncodeDecode(&v7, &v8, t)

	v9, v10 := uint8(10), uint8(0)
	EncodeDecode(&v9, &v10, t)

	v11, v12 := uint16(10), uint16(0)
	EncodeDecode(&v11, &v12, t)

	v13, v14 := uint32(10), uint32(0)
	EncodeDecode(&v13, &v14, t)

	v15, v16 := uint64(10), uint64(0)
	EncodeDecode(&v15, &v16, t)

	// // Slices.
	s1, s2 := []byte{1, 2, 3}, []byte{}
	EncodeDecodeSlice(s1, &s2, t)

	s3, s4 := []float64{11.11, 22.22, 33.33}, []float64{}
	EncodeDecodeSlice(s3, &s4, t)
	
	s5, s6 := []float32{11.11, 22.22, 33.33}, []float32{}
	EncodeDecodeSlice(s5, &s6, t)

	s7, s8 := []int64{11, 22, 33}, []int64{}
	EncodeDecodeSlice(s7, &s8, t)

	s9, s10 := []int32{11, 22, 33}, []int32{}
	EncodeDecodeSlice(s9, &s10, t)

	s11, s12 := []int16{11, 22, 33}, []int16{}
	EncodeDecodeSlice(s11, &s12, t)

	s13, s14 := []int8{11, 22, 33}, []int8{}
	EncodeDecodeSlice(s13, &s14, t)

	s15, s16 := []uint64{11, 22, 33}, []uint64{}
	EncodeDecodeSlice(s15, &s16, t)

	s17, s18 := []uint32{11, 22, 33}, []uint32{}
	EncodeDecodeSlice(s17, &s18, t)

	s19, s20 := []uint16{11, 22, 33}, []uint16{}
	EncodeDecodeSlice(s19, &s20, t)

	s21, s22 := []uint8{11, 22, 33}, []uint8{}
	EncodeDecodeSlice(s21, &s22, t)
}

func TestDecodeEncodeCustom(t *testing.T) {
	c1, c2 := CustomInt{1, 2, 3, 4}, CustomInt{}
	raw, _ := Encode(c1)
	Decode(raw, &c2)

	c3, c4 := CustomByte{1, 2, 3, 4}, CustomByte{}
	raw, _  = Encode(c3)
	Decode(raw, &c4)

	// Arrays.
	c5, c6 := &CustomArrByte{1,2,3}, &CustomArrByte{}
	EncodeDecode(c5, c6, t)
	
	c7, c8 := &CustomArrInt{3, 2, 1}, &CustomArrInt{}
	EncodeDecode(c7, c8, t)
}

func TestDecodeEncodeAny(t *testing.T) {
	v1 := hexutil.Uint64(666)
	v2 := hexutil.Uint64(0)
	EncodeDecode(&v1, &v2, t)
}

type TestStruct struct { Data []byte }
func (t *TestStruct) Encode() []byte { return t.Data }
func (t *TestStruct) Decode(raw []byte) error { t.Data = raw; return nil }

func TestDecodeEncodeStruct(t *testing.T) {
	v1 := TestStruct{[]byte{6, 6, 6}}
	v2 := TestStruct{[]byte{}}

	raw, _ := Encode(&v1)
	Decode(raw, &v2)

	if !bytes.Equal([]byte{0,0,0,0,0,0,0,3,6,6,6}, v2.Data) {
		t.Errorf("Expected \n to get %v,\nbut got %v", v1.Data, v2.Data) 
	}
}

func TestDecodeEncodeArrayOfStructs(t *testing.T) {
	v1 := []*TestStruct{
		&TestStruct{[]byte{1, 2, 3}},
		&TestStruct{[]byte{4, 5, 6}},
	}

	v2 := []*TestStruct{}	

	raw, _ := Encode(v1)
	Decode(raw, &v2)

	if !bytes.Equal(v2[0].Data, v1[0].Data) {
		t.Errorf("Expected \n to get %v,\nbut got %v", v1[0].Data, v2[0].Data) 
	}

	if !bytes.Equal(v2[1].Data, v1[1].Data) {
		t.Errorf("Expected \n to get %v,\nbut got %v", v1[1].Data, v2[1].Data) 
	}
}

// For testing purpose only. Will be removed.
type Tx struct {
	// Type              uint8
	From              *common.Address
	// To                *common.Address
	// Value             *big.Int
	// Nonce             uint64
	// Hash              common.Hash
	// ChainID           *big.Int
	// Status            uint64
	// BlockNumber       *big.Int

	// GasUsed           uint64
	// GasPrice          *big.Int
	// CumulativeGasUsed uint64
	// Gas               uint64
	// GasTipCap         *big.Int
	// V, R, S           *big.Int

	// Data []byte
}

func TestEncodeStructFields(t *testing.T) {
	tx := Tx{From: &common.Address{1, 2}}

	buf, err := Encode(tx)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(buf)
}