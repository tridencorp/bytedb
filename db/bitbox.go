package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"reflect"
	"unsafe"
)

// *********************************
// Bitbox Encoder/Decoder interfaces
// *********************************

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
			if isEncoder(val) {
				structEncode(buf, val)
			}

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

		if isPointer(fv) && fv.IsNil() {
			encodeEmptyCollection(buf, fv)
			continue
		}

		encode(buf, fv)
	}

	return nil
}

// Encode empty (nil) slice, array.
func encodeEmptyCollection(buf *bytes.Buffer, val reflect.Value) {
	kind := val.Type().Kind()

	if isPointer(val) {
		kind = val.Type().Elem().Kind()
	}

	if kind == reflect.Struct {
		// TODO: handle all this types in one custom function.
		if isBigInt(val.Type().Elem()) {
			size := int64(0)
			write(buf, &size)
		}
	}

	// For empty array we must create one and fill buffer with it's default values.
	// We are not adding size for arrays, decoder should now the size.
	if kind == reflect.Array {
		elem := createElem(val)
		write(buf, elem.Interface())
	}

	// For empty slice we only write size 0.
	if kind == reflect.Slice {
		size := int64(0)
		write(buf, &size)
	}
}

func encode(buf *bytes.Buffer, val reflect.Value) {
	// Get rid of pointers, use values.
	val = reflect.Indirect(val)

	// Encode arrays and structs.
	if isArray(val.Type()) || isSlice(val) {
		switch val.Type().Elem().Kind() {
		case reflect.Uint8:
			encodeSlice[uint8](buf, val)
		case reflect.Uint16:
			encodeSlice[uint16](buf, val)
		case reflect.Uint32:
			encodeSlice[uint32](buf, val)
		case reflect.Uint64:
			encodeSlice[uint64](buf, val)
		case reflect.Int8:
			encodeSlice[int8](buf, val)
		case reflect.Int16:
			encodeSlice[int16](buf, val)
		case reflect.Int32:
			encodeSlice[int32](buf, val)
		case reflect.Int64:
			encodeSlice[int64](buf, val)
		case reflect.Float32:
			encodeSlice[float32](buf, val)
		case reflect.Float64:
			encodeSlice[float64](buf, val)

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
		if isBigInt(val.Type()) {
			bigint := val.Interface().(big.Int)
			encodeSlice[uint8](buf, reflect.ValueOf(bigint.Bytes()))
		}
	}

	// Encode single types.
	switch val.Kind() {
	case reflect.Uint8:
		write(buf, val.Interface())
	case reflect.Uint16:
		write(buf, val.Interface())
	case reflect.Uint32:
		write(buf, val.Interface())
	case reflect.Uint64:
		write(buf, val.Interface())
	case reflect.Int8:
		write(buf, val.Interface())
	case reflect.Int16:
		write(buf, val.Interface())
	case reflect.Int32:
		write(buf, val.Interface())
	case reflect.Int64:
		write(buf, val.Interface())
	case reflect.Float32:
		write(buf, val.Interface())
	case reflect.Float64:
		write(buf, val.Interface())
	}
}

func encodeSlice[T any](buf *bytes.Buffer, val reflect.Value) {
	// TODO: Handle nil and empty collections
	if val.Len() == 0 {
		write(buf, int64(0))
		return
	}

	// If we have array, we know the number of elements so we
	// don't have to write them to buffer. Decoder should
	// know the exact type.
	if !isArray(val.Type()) {
		write(buf, int64(val.Len()))
	}

	if !isPointer(val) {
		binary.Write(buf, binary.BigEndian, val.Interface())
		return
	}

	// Unsafe but faster.
	ptr := unsafe.Pointer(val.Index(0).Addr().UnsafePointer())
	slice := unsafe.Slice((*T)(ptr), val.Len())

	binary.Write(buf, binary.BigEndian, slice)
}

func write(buf *bytes.Buffer, elem any) error {
	return binary.Write(buf, binary.BigEndian, elem)
}

// ********
//  Decode
// ********

func Decode2(buf []byte, items ...any) error {
	i := 0

	for _, item := range items {
		switch val := item.(type) {
		case []byte:
			copy(val, buf[i:])
			i += len(val)
		}
	}

	return nil
}

// Get pointer to any fixed type (and struct) and cast it to []byte.
// After that we can copy bytes directly into it using copy().
func ToBytes[T any](obj *T) []byte {
	size := unsafe.Sizeof(*obj)
	return unsafe.Slice((*byte)(unsafe.Pointer(obj)), size)
}

// AsBytes returns a byte slice representation of any fixed-size type (struct or basic type).
// The slice points directly into the object’s memory, so it can be used with copy().
func AsBytes[T any](obj *T) []byte {
	size := unsafe.Sizeof(*obj)
	return unsafe.Slice((*byte)(unsafe.Pointer(obj)), size)
}

// Points object to data bytes. Changing object will change underlying data
//
// TODO: Check length
// TODO: Check alignment of data
func PointTo[T any](obj **T, data []byte) {
	*obj = (*T)(unsafe.Pointer(&data[0]))
}

func Decode(buf *bytes.Buffer, items ...any) error {
	for _, item := range items {
		elem := reflect.TypeOf(item)
		val := reflect.ValueOf(item)

		if isSlicePtr(item) {
			elem = elem.Elem().Elem()

			switch elem.Kind() {
			case reflect.Uint8:
				decodeSlice[uint8](buf, item)
			case reflect.Uint16:
				decodeSlice[uint16](buf, item)
			case reflect.Uint64:
				decodeSlice[uint64](buf, item)
			case reflect.Uint32:
				decodeSlice[uint32](buf, item)
			case reflect.Int64:
				decodeSlice[int64](buf, item)
			case reflect.Int32:
				decodeSlice[int32](buf, item)
			case reflect.Int16:
				decodeSlice[int16](buf, item)
			case reflect.Int8:
				decodeSlice[int8](buf, item)
			case reflect.Float64:
				decodeSlice[float64](buf, item)
			case reflect.Float32:
				decodeSlice[float32](buf, item)

			default:
				// Check if we have slice of structs.
				if isStruct(reflect.ValueOf(val.Elem())) {
					decodeArrayStruct(buf, val, item)
					continue
				}

				fmt.Printf("unsupported type: %v\n", elem.Kind())
			}

			continue
		}

		if isStruct(reflect.Indirect(val)) {
			val, ok := val.Interface().(Decoder)
			if ok {
				val.Decode(buf.Bytes())
				continue
			}

			decodeStructFields(buf, item)
			continue
		}

		// Decode big.Int.
		// TODO: Clean this.
		val = reflect.Indirect(val)
		if isPointer(val) {
			if isBigInt(val.Elem().Type()) {
				tmp := []byte{}
				decodeSlice[uint8](buf, &tmp)

				bigint := new(big.Int).SetBytes(tmp)
				val.Elem().Set(reflect.ValueOf(*bigint))
				continue
			}
		}

		decode(buf, item)
	}

	return nil
}

func decodeStructFields(buf *bytes.Buffer, dst any) {
	val := reflect.ValueOf(dst)
	val = reflect.Indirect(val)

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)

		if isPointer(field) && field.IsNil() {
			elem := createElem(field)

			if isArray(field.Type().Elem()) {
				decode(buf, elem.Interface())
				field.Set(elem)
			}

			if isBigInt(elem.Type().Elem()) {
				tmp := []byte{}
				decodeSlice[uint8](buf, &tmp)

				bigint := elem.Interface().(*big.Int)
				bigint.SetBytes(tmp)
				field.Set(reflect.ValueOf(bigint))
			}
		}

		if !isPointer(field) {
			if isSlice(field) {
				size := int64(0)
				decode(buf, &size)

				// Slice size is 0, there are no elements so we can continue with next field.
				if size == 0 {
					continue
				}

				// Fast path for []byte.
				if field.Type().Elem().Kind() == reflect.Uint8 {
					tmp := make([]byte, size)
					buf.Read(tmp)
					field.SetBytes(tmp[:])
					continue
				}

				decodeSlice2(buf, field, size)
				continue
			}

			elem := createElem(field)
			decode(buf, elem.Interface())
			elem = elem.Elem()

			field.Set(elem)
		}
	}
}

func createElem(val reflect.Value) reflect.Value {
	typ := val.Type()

	if isPointer(val) {
		typ = typ.Elem()
	}

	elem := reflect.New(typ)
	return elem
}

// TODO: Temporary only, will be refactor.
func decodeSlice2(buf *bytes.Buffer, val reflect.Value, size int64) {
	typ := val.Type().Elem()
	elem := reflect.MakeSlice(reflect.SliceOf(typ), int(size), int(size))

	decode(buf, elem.Interface())
	val.Set(elem)
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

func decode(buf *bytes.Buffer, dst any) error {
	return binary.Read(buf, binary.BigEndian, dst)
}

func decodeArrayStruct(buf *bytes.Buffer, val reflect.Value, item any) {
	elemType := reflect.TypeOf(item).Elem().Elem().Elem()
	slice := val.Elem()

	// Get the number of elements in slice. Each element is also a slice.
	// ex: []byte{[]byte, []byte, ...}
	size := int64(0)
	decode(buf, &size)

	for i := int64(0); i < size; i++ {
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
func isBigInt(v reflect.Type) bool {
	return v.PkgPath() == "math/big" && v.Name() == "Int"
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
func isArray(v reflect.Type) bool {
	return v.Kind() == reflect.Array
}

// Check if element is pointer to slice.
func isSlicePtr(elem any) bool {
	t := reflect.TypeOf(elem)
	return t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Slice
}
