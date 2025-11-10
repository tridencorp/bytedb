package common

import (
	"reflect"
	"unsafe"
)

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

			buf = append(buf, BytesPtr(&l)...) // append length prefix
			buf = append(buf, val.Bytes()...)  // append bytes
		}
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

// Get pointer to any fixed type (and struct) and cast it to []byte.
// After that we can copy bytes directly into it using copy().
func BytesPtr[T any](obj *T) []byte {
	size := unsafe.Sizeof(*obj)
	return unsafe.Slice((*byte)(unsafe.Pointer(obj)), size)
}
