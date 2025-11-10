package common

import (
	"reflect"
	"unsafe"
)

// Encode elements to []byte
func Encode(elements ...any) []byte {
	buf := []byte{}

	for _, elem := range elements {
		val := reflect.ValueOf(elem)
		val = reflect.Indirect(val) // indirect pointers

		if !val.IsValid() {
			continue // skip nil pointers
		}

		// Encode bytes
		if IsByteSlice(val) {
			l := uint32(len(val.Bytes()))

			buf = append(buf, BytesPtr(&l)...) // length prefix
			buf = append(buf, val.Bytes()...)  // bytes
			continue
		}

		// Encode simple basic types
		buf = append(buf, bytesPtr(val)...)
		continue
	}

	return buf
}

// Check if value is byte slice/array
func IsByteSlice(val reflect.Value) bool {
	if IsSlice(val) || IsArray(val) {
		return val.Type().Elem().Kind() == reflect.Uint8
	}

	return false
}

// Check is value is slice
func IsSlice(val reflect.Value) bool {
	return val.Kind() == reflect.Slice
}

// Check if value is array
func IsArray(val reflect.Value) bool {
	return val.Kind() == reflect.Array
}

// Get pointer from fixed type (including structs) and cast it to []byte
func BytesPtr[T any](obj *T) []byte {
	size := unsafe.Sizeof(*obj)
	return unsafe.Slice((*byte)(unsafe.Pointer(obj)), size)
}

// Get pointer from reflect.Value and cast it to []byte
func bytesPtr(val reflect.Value) []byte {
	if !val.CanAddr() {
		panic("value is not addressable")
	}

	ptr  := unsafe.Pointer(val.UnsafeAddr())
	size := val.Type().Size()

	return unsafe.Slice((*byte)(ptr), size)
}