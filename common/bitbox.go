package common

import (
	"reflect"
	"unsafe"
)

// Encode objects to []byte
func Encode(objects ...any) []byte {
	buf := []byte{}

	for _, obj := range objects {
		val := reflect.ValueOf(obj)
		val = reflect.Indirect(val) // indirect pointers

		if !val.IsValid() {
			continue // skip nil pointers
		}

		// Encode []byte
		if IsByteList(val) {
			l := uint32(len(val.Bytes()))

			buf = append(buf, BytesPtr(&l)...) // length prefix
			buf = append(buf, val.Bytes()...)  // bytes
			continue
		}

		// Encode basic types
		buf = append(buf, bytesPtr(val)...)
		continue
	}

	return buf
}

func Decode(buf []byte, objects ...any) {
	for _, obj := range objects {
		// 1. Fast Path for basic types
		switch v := obj.(type) {
		case *[]byte:
			*v = append(*v, buf...)
			continue
		case *bool:
			copy(BytesPtr(v), buf)
		case *int8:
			copy(BytesPtr(v), buf)
		case *int16:
			copy(BytesPtr(v), buf)
		case *int32:
			copy(BytesPtr(v), buf)
		case *int64:
			copy(BytesPtr(v), buf)
		case *uint8:
			copy(BytesPtr(v), buf)
		case *uint16:
			copy(BytesPtr(v), buf)
		case *uint32:
			copy(BytesPtr(v), buf)
		case *uint64:
			copy(BytesPtr(v), buf)
		case *float32:
			copy(BytesPtr(v), buf)
		case *float64:
			copy(BytesPtr(v), buf)
		case *complex64:
			copy(BytesPtr(v), buf)
		case *complex128:
			copy(BytesPtr(v), buf)
		}

		// 2. Using reflections
		val := reflect.ValueOf(obj)
		val = reflect.Indirect(val) // indirect pointers

		// Decode basic types
		copy(bytesPtr(val), buf)
		continue
	}
}

// Check if we deal with byte slice/array
func IsByteList(val reflect.Value) bool {
	if IsSlice(val) || IsArray(val) {
		return val.Type().Elem().Kind() == reflect.Uint8
	}

	return false
}

// Check if we have slice
func IsSlice(val reflect.Value) bool {
	return val.Kind() == reflect.Slice
}

// Check if we have array
func IsArray(val reflect.Value) bool {
	return val.Kind() == reflect.Array
}

// Get pointer from fixed type (including structs) and cast it to []byte.
// When you pass struct, make sure it's memory aligned.
func BytesPtr[T any](obj *T) []byte {
	size := unsafe.Sizeof(*obj)
	return unsafe.Slice((*byte)(unsafe.Pointer(obj)), size)
}

// Get pointer from reflect.Value and cast it to []byte.
// Value must be addressable.
func bytesPtr(val reflect.Value) []byte {
	if !val.CanAddr() {
		panic("value is not addressable")
	}

	ptr := unsafe.Pointer(val.UnsafeAddr())
	size := val.Type().Size()

	return unsafe.Slice((*byte)(ptr), size)
}
