package bitbox

import (
	"reflect"
	"unsafe"
)

// Encode objects
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

// Decode objects
func Decode(buf *Buffer, objects ...any) {
	for _, obj := range objects {
		// 1. Fast Path for basic types
		switch v := obj.(type) {
		case *[]byte:
			l := uint32(0)
			buf.Decode(&l)
			*v = append(*v, buf.Take(int(l))...)
			continue
		case *bool:
			buf.Copy(BytesPtr(v))
			continue
		case *int8:
			buf.Copy(BytesPtr(v))
			continue
		case *int16:
			buf.Copy(BytesPtr(v))
			continue
		case *int32:
			buf.Copy(BytesPtr(v))
			continue
		case *int64:
			buf.Copy(BytesPtr(v))
			continue
		case *uint8:
			buf.Copy(BytesPtr(v))
			continue
		case *uint16:
			buf.Copy(BytesPtr(v))
			continue
		case *uint32:
			buf.Copy(BytesPtr(v))
			continue
		case *uint64:
			buf.Copy(BytesPtr(v))
			continue
		case *float32:
			buf.Copy(BytesPtr(v))
			continue
		case *float64:
			buf.Copy(BytesPtr(v))
			continue
		case *complex64:
			buf.Copy(BytesPtr(v))
			continue
		case *complex128:
			buf.Copy(BytesPtr(v))
			continue
		}

		// 2. Using reflections
		val := reflect.ValueOf(obj)
		val = reflect.Indirect(val) // indirect pointers/

		// Decode basic types
		buf.Copy(bytesPtr(val))

		continue
	}
}

// Get pointer to fixed type (including structs) and cast it to []byte.
// When passing structs, make sure it's memory aligned.
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
