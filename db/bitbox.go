package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
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
		val := reflect.ValueOf(elem)
		
		if isPointer(val) && isStruct(val.Elem()) {
			if isEncoder(val) { structEncode(buf, val) }
			continue
		}
		
		val = reflect.Indirect(val)
		
		// Encode structs.
		if isStruct(val) {
			// Using Encoder() interface.
			if isEncoder(val) {
				structEncode(buf, val)
				continue
			}

			// Encode all fields in struct.
			encodeStructFields(buf, elem)
			continue
		}

		// Encode slices, arrays and basic types.
		encode(buf, val)
	}

	return buf, nil
}

func structEncode(buf *bytes.Buffer, v reflect.Value) {
	val := v.Interface().(Encoder)
	bytes := reflect.ValueOf(val.Encode())
	encode(buf, bytes)
}

func encodeStructFields(buf *bytes.Buffer, elem any) error {
	val := reflect.ValueOf(elem)
	
	for i := 0; i < val.NumField(); i++ {
		fv := val.Field(i)
		encode(buf, fv)
	}

	return nil
}

func encode(buf *bytes.Buffer, val reflect.Value) {
	// Encode arrays and structs.
	if isArray(val) || isSlice(val) {
		switch val.Type().Elem().Kind() {
			case reflect.Uint8:   encodeSlice[uint8](buf, val)
			case reflect.Uint16:  encodeSlice[uint16](buf, val)
			case reflect.Uint32:  encodeSlice[uint32](buf, val)
			case reflect.Uint64:  encodeSlice[uint64](buf, val)		
			case reflect.Int8:    encodeSlice[int8](buf, val)
			case reflect.Int16:   encodeSlice[int16](buf, val)
			case reflect.Int32:   encodeSlice[int32](buf, val)
			case reflect.Int64:   encodeSlice[int64](buf, val)
			case reflect.Float32: encodeSlice[float32](buf, val)
			case reflect.Float64: encodeSlice[float64](buf, val)

			default:
			// We have slice with structs, let's iterate.
			// TODO: Clean this section, possible bugs.
			write(buf, int64(val.Len()))

			for i := 0; i < val.Len(); i++ {
				elem := val.Index(i)
				if isEncoder(elem) {
					bytes := elem.Interface().(Encoder)
					encode(buf, reflect.ValueOf(bytes.Encode()))
				}
			}
		}
	}

	if isStruct(val) {
		// Special case for big.Int
		if isBigInt(val) {
			bigint := val.Interface().(big.Int)
			encodeSlice[uint8](buf, reflect.ValueOf(bigint.Bytes()))
		}
	}

	// Encode single types.
	switch val.Kind() {
		case reflect.Uint8:   write(buf, val.Interface())
		case reflect.Uint16:  write(buf, val.Interface())
		case reflect.Uint32:  write(buf, val.Interface())
		case reflect.Uint64:  write(buf, val.Interface())
		case reflect.Int8:    write(buf, val.Interface())
		case reflect.Int16:   write(buf, val.Interface())
		case reflect.Int32:   write(buf, val.Interface())
		case reflect.Int64:   write(buf, val.Interface())
		case reflect.Float32: write(buf, val.Interface())
		case reflect.Float64: write(buf, val.Interface())
	}
}
		
func encodeSlice[T any](buf *bytes.Buffer, val reflect.Value) {
	// TODO: Handle nil and empty collections
	if val.Len() == 0 {
		return
	}

	// If we have array, we know the number of elements so we
	// don't have to write them to buffer. Decoder should
	// know the exact type.
	if !isArray(val) {
		write(buf, int64(val.Len()))
	}
	
	// [OLD] Slower but safer.
	// binary.Write(buf, binary.BigEndian, ar.Slice(0, ar.Len()).Interface().([]int8))
	// 
	// [NEW] Unsafe but faster.
	ptr   := unsafe.Pointer(val.Index(0).Addr().UnsafePointer())
	slice := unsafe.Slice((*T)(ptr), val.Len())
	
	binary.Write(buf, binary.BigEndian, slice)
}
	
func write(buf *bytes.Buffer, elem any) {
	binary.Write(buf, binary.BigEndian, elem)
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
				case reflect.Uint8:   decodeSlice[uint8](buf, item)
				case reflect.Uint16:  decodeSlice[uint16](buf, item)
				case reflect.Uint64:  decodeSlice[uint64](buf, item)
				case reflect.Uint32:  decodeSlice[uint32](buf, item)
				case reflect.Int64:   decodeSlice[int64](buf, item)
				case reflect.Int32:   decodeSlice[int32](buf, item)
				case reflect.Int16:   decodeSlice[int16](buf, item)
				case reflect.Int8:    decodeSlice[int8](buf, item)
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
			fmt.Println("decoder")

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

// Check if struct implements Encoder interface.
func isEncoder(v reflect.Value) bool {
	_, ok := v.Interface().(Encoder)	
	return ok
}

// Check if value is big.Int.
func isBigInt(v reflect.Value) bool {
	return v.Type().PkgPath() == "math/big" && v.Type().Name() == "Int"
}

// Check if value is pointer.
func isPointer(v reflect.Value) bool {
	return v.Kind() == reflect.Pointer	
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
