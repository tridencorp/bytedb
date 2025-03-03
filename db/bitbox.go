package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
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

func Encode(elements ...any) (bytes.Buffer, error) {
	buf := bytes.Buffer{}

	for _, elem := range elements {
		switch v := elem.(type) {
		case *[]byte: encodeBytes(&buf, *v)
		case  []byte: encodeBytes(&buf, v)

		case []int64: encodeSlice(&buf, v)
		case []int32: encodeSlice(&buf, v)
		case []int16: encodeSlice(&buf, v)
		case []int8:  encodeSlice(&buf, v)

		case []uint64: encodeSlice(&buf, v)
		case []uint32: encodeSlice(&buf, v)
		case []uint16: encodeSlice(&buf, v)

		case []float64: encodeSlice(&buf, v)
		case []float32: encodeSlice(&buf, v)

		case []any: {
			data, _ := Encode(elem.([]any)...)
			encode(&buf, data.Bytes())
		}

		default:
			val := reflect.ValueOf(elem)

			// We have struct so we try to call Encode() method on it.
			if isStruct(reflect.Indirect(val)) {
				val, ok := val.Interface().(Encoder)
				if ok {
					bytes := val.Encode()
					encodeBytes(&buf, bytes)
				}

				// Encode all struct fields.
				encodeStructFields(elem)
				continue
			}

			if isSlice(val) { 
				_, ok := val.Index(0).Interface().(Encoder)
				if ok {
					// Encode total number of elements in slice.
					encode(&buf, int64(val.Len()))

					// Iterate all elements.
					for i:=0; i < val.Len(); i++  {
						encoder, _ := val.Index(i).Interface().(Encoder)
						encodeBytes(&buf, encoder.Encode())
					}
					continue
				}

				encode(&buf, int64(val.Len()))
				encode(&buf, elem)
				continue
			}

			encode(&buf, elem)
		}
	}

	return buf, nil
}

func encodeStructFields(elem any) error {
	if reflect.TypeOf(elem).Kind() != reflect.Struct {
		return fmt.Errorf("Element is not a struct") 
	}

	value := reflect.ValueOf(elem)
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		fmt.Println(field)
	}

	return nil
}

func encodeSlice[T any](buf *bytes.Buffer, elem []T) {
	encode(buf, int64(len(elem)))
	tmp := make([]T, len(elem))

	// More efficient than copying?
	elem, tmp = tmp, elem
	encode(buf, tmp)
}

func encode(buf *bytes.Buffer, elem any) {
	binary.Write(buf, binary.BigEndian, elem)
}

func encodeBytes(buf *bytes.Buffer, bytes []byte) {
	encode(buf, int64(len(bytes)))
	encode(buf, bytes)
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

// Check if element is pointer to slice.
func isSlicePtr(elem any) bool {
	t := reflect.TypeOf(elem)
	return t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Slice
}
