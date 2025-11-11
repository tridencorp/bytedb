package common

import (
	"reflect"
	"unsafe"
)

// Simple bytes buffer that tracks it's offset
type Buffer[T any] struct {
	bytes  []T
	Offset int
}

func NewBuffer[T any](bytes []T) *Buffer[T] {
	return &Buffer[T]{bytes: bytes, Offset: 0}
}

func (b *Buffer[T]) Copy(dst []T) int {
	n := copy(dst, b.bytes[b.Offset:])
	b.Offset += n

	return n
}

// Get next N bytes from slice and update offset
func (b *Buffer[T]) Next(num int) []T {
	off := b.Offset
	b.Offset += num

	return b.bytes[off:b.Offset]
}

// Bytes encoder
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

// Bytes decoder
func Decode(buf []byte, objects ...any) {
	b := NewBuffer(buf)

	for _, obj := range objects {
		// 1. Fast Path for basic types
		switch v := obj.(type) {
		case *[]byte:
			l := uint32(0)
			Decode(b.Next(4), &l)
			*v = append(*v, b.Next(int(l))...)
			continue
		case *bool:
			b.Copy(BytesPtr(v))
			continue
		case *int8:
			b.Copy(BytesPtr(v))
			continue
		case *int16:
			b.Copy(BytesPtr(v))
			continue
		case *int32:
			b.Copy(BytesPtr(v))
			continue
		case *int64:
			b.Copy(BytesPtr(v))
			continue
		case *uint8:
			b.Copy(BytesPtr(v))
			continue
		case *uint16:
			b.Copy(BytesPtr(v))
			continue
		case *uint32:
			b.Copy(BytesPtr(v))
			continue
		case *uint64:
			b.Copy(BytesPtr(v))
			continue
		case *float32:
			b.Copy(BytesPtr(v))
			continue
		case *float64:
			b.Copy(BytesPtr(v))
			continue
		case *complex64:
			b.Copy(BytesPtr(v))
			continue
		case *complex128:
			b.Copy(BytesPtr(v))
			continue
		}

		// 2. Using reflections
		val := reflect.ValueOf(obj)
		val = reflect.Indirect(val) // indirect pointers/

		// Decode basic types
		b.Copy(bytesPtr(val))

		continue
	}
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
