package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"
)

// *********************************************************************
// Common interfaces struct must implement to be compatible with bitbox
// *********************************************************************

type Encoder interface {
	Encode() []byte
}

type Decoder interface {
	Decode([]byte) error
}

// **************
//     Encode
// **************

func Encode(elements ...any) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)

	for _, elem := range elements {
		switch v := elem.(type) {
		case *[]byte: encodeBytes(buf, *v)
		case  []byte: encodeBytes(buf, v)

		case []int64: encodeSlice(buf, v)
		case []int32: encodeSlice(buf, v)
		case []int16: encodeSlice(buf, v)
		case []int8:  encodeSlice(buf, v)

		case []uint64: encodeSlice(buf, v)
		case []uint32: encodeSlice(buf, v)
		case []uint16: encodeSlice(buf, v)

		case []float64: encodeSlice(buf, v)
		case []float32: encodeSlice(buf, v)

		case []any: {
			data, _ := Encode(elem.([]any)...)
			write(buf, data.Bytes())
		}

		default:
			val := reflect.ValueOf(elem)

			// We have struct so we try to call Encode() method on it.
			if isStruct(reflect.Indirect(val)) {
				val, ok := val.Interface().(Encoder)
				if ok {
					bytes := val.Encode()
					encodeBytes(buf, bytes)
					continue
				}

				// Encode all struct fields.
				return encodeStructFields(elem)
			}

			if isSlice(val) { 
				_, ok := val.Index(0).Interface().(Encoder)
				if ok {
					// Encode total number of elements in slice.
					write(buf, int64(val.Len()))

					// Iterate all elements.
					for i:=0; i < val.Len(); i++  {
						encoder, _ := val.Index(i).Interface().(Encoder)
						encodeBytes(buf, encoder.Encode())
					}
					continue
				}

				write(buf, int64(val.Len()))
				write(buf, elem)
				continue
			}

			write(buf, elem)
		}
	}

	return buf, nil
}

func encodeStructFields(elem any) (*bytes.Buffer, error) {
	if reflect.TypeOf(elem).Kind() != reflect.Struct {
		return nil, fmt.Errorf("Element is not a struct") 
	}

	v := reflect.ValueOf(elem)
	t := reflect.TypeOf(elem)

	buf := new(bytes.Buffer)

	for i := 0; i < v.NumField(); i++ {
		f  := t.Field(i)
		fv := v.Field(i)
		ft := f.Type
		fmt.Println(ft)

		// If we have pointer, get it's value.
		fv = reflect.Indirect(fv)
		encode(buf, fv)
	}
	return buf, nil
}
	
func encode(buf *bytes.Buffer, val reflect.Value) {
	// Encode arrays and structs.
	if isArray(val) || isSlice(val) {
		switch val.Type().Elem().Kind() {
		case reflect.Uint8:
			encodeSlice2[uint8](buf, val)

		case reflect.Int8:
			encodeSlice2[int8](buf, val)
		}		
	}
}
		
func encodeSlice2[T any](buf *bytes.Buffer, val reflect.Value) {
	// [OLD] Slower but safer.
	// binary.Write(buf, binary.BigEndian, ar.Slice(0, ar.Len()).Interface().([]int8))
	// 
	// [NEW] Unsafe but faster.
	ptr   := unsafe.Pointer(val.Index(0).Addr().UnsafePointer())
	slice := unsafe.Slice((*uint8)(ptr), val.Len())

	binary.Write(buf, binary.BigEndian, slice)
}
	
func encodeSlice[T any](buf *bytes.Buffer, elem []T) {
	write(buf, int64(len(elem)))
	tmp := make([]T, len(elem))
	
	// More efficient than copying?
	elem, tmp = tmp, elem
	write(buf, tmp)
}

func write(buf *bytes.Buffer, elem any) {
	binary.Write(buf, binary.BigEndian, elem)
}

func encodeBytes(buf *bytes.Buffer, bytes []byte) {
	write(buf, int64(len(bytes)))
	write(buf, bytes)
}

// **************
//     Decode
// **************

func Decode(buf *bytes.Buffer, items ...any) error {
	for _, item := range items {
		elem := reflect.TypeOf(item)
		val  := reflect.ValueOf(item)

		if isSlicePtr(item) {
			elem = elem.Elem().Elem()

			switch elem.Kind() {
				case reflect.Uint8:  decodeSlice[uint8](buf, item)
				case reflect.Uint16: decodeSlice[uint16](buf, item)
				case reflect.Uint64: decodeSlice[uint64](buf, item)
				case reflect.Uint32: decodeSlice[uint32](buf, item)
				case reflect.Int64:  decodeSlice[int64](buf, item)
				case reflect.Int32:  decodeSlice[int32](buf, item)
				case reflect.Int16:  decodeSlice[int16](buf, item)
				case reflect.Int8:   decodeSlice[int8](buf, item)

				case reflect.Float64: decodeSlice[float64](buf, item)
				case reflect.Float32: decodeSlice[float32](buf, item)

			default:
				// Check if we have slice of structs.
				if isStruct(reflect.ValueOf(val.Elem())) {
					decodeArrayStruct(buf, val, item)
					continue
				}

				fmt.Printf("unsupported type: %v\n", elem.Kind())
			}
		}

		if isStruct(reflect.Indirect(val)) {
			val, ok := val.Interface().(Decoder)
			if ok {
				val.Decode(buf.Bytes())
				continue
			}
		}

		decode(buf, item)
	}

	return nil
}

func decodeSlice[T any](buf *bytes.Buffer, dst any) {
	// Decode slice size.
	size := int64(0)
	decode(buf, &size)

	// Make temporary slice with proper size and write buffer data into it.
	tmp := make([]T, size)
	decode(buf, &tmp)

	val1 := reflect.ValueOf(dst)
	val2 := reflect.ValueOf(tmp)

	// Set destination slice.
	val1.Elem().Set(val2)
}

func decode(buf *bytes.Buffer, dst any) {
	err := binary.Read(buf, binary.BigEndian, dst)	
	if err != nil {
		fmt.Println(err)
	}
}

func decodeArrayStruct(buf *bytes.Buffer, val reflect.Value, item any) {
	elemType := reflect.TypeOf(item).Elem().Elem().Elem()
	slice := val.Elem()

	// Get the number of elements in slice. Each element is also a slice.
	// ex: []byte{[]byte, []byte, ...}
	size := int64(0)
	decode(buf, &size)

	for i:=int64(0); i < size; i++ {
		bytes := make([]byte, size)
		decodeSlice[uint8](buf, &bytes)

		elem, _ := reflect.New(elemType).Interface().(Decoder)
		elem.Decode(bytes)

		slice = reflect.Append(slice, reflect.ValueOf(elem))
	}

	val.Elem().Set(slice)
}

// Check if value is struct.
func isStruct(v reflect.Value) bool {
	return v.Kind() == reflect.Struct	
}

// Check if value is slice.
func isSlice(v reflect.Value) bool {
	return v.Kind() == reflect.Slice
}

// Check if value is array.
func isArray(v reflect.Value) bool {
	return v.Kind() == reflect.Array	
}

// Check if element is pointer to slice.
func isSlicePtr(elem any) bool {
	t := reflect.TypeOf(elem)
	return t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Slice
}
