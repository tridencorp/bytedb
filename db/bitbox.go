package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

// Encode elements to bytes.
func Encode(elements ...any) (bytes.Buffer, error) {
	buf := bytes.Buffer{}

	for _, elem := range elements {
		switch v := elem.(type) {
		case *[]byte:
			encode(&buf, len(*v))
			encode(&buf, *v)

		case []byte:
			encode(&buf, len(v))
			encode(&buf, v)

		case []int64: EncodeSlice(&buf, v)
		case []int32: EncodeSlice(&buf, v)
		case []int16: EncodeSlice(&buf, v)
		case []int8:  EncodeSlice(&buf, v)

		case []uint64: EncodeSlice(&buf, v)
		case []uint32: EncodeSlice(&buf, v)
		case []uint16: EncodeSlice(&buf, v)

		case []float64: EncodeSlice(&buf, v)
		case []float32: EncodeSlice(&buf, v)

		default:
			// Fallback for custom types, ex: type Hash []byte
			// Check if we are dealing with slices.
			kind := reflect.TypeOf(elem).Kind()

			if kind == reflect.Slice || kind == reflect.Array {
				val := reflect.ValueOf(elem)

				// Special case for encoding []byte.
				if reflect.TypeOf(elem).Elem() == reflect.TypeOf(uint8(0)) {
					encode(&buf, val.Len())
					encode(&buf, elem)
					continue
				}

				for i:=0; i < val.Len(); i++ {
					item := val.Index(i).Interface()
					encode(&buf, item)
				}

				continue
			}

			encode(&buf, elem)
		}
	}

	return buf, nil
}

func EncodeSlice[T any](buf *bytes.Buffer, elem []T) {
	encode(buf, len(elem))
	tmp := make([]T, len(elem))

	// More efficient than copying?
	elem, tmp = tmp, elem
	encode(buf, tmp)
}

func DecodeSlice[T any](buf *bytes.Buffer, elem any) {
	size := int64(0)
	decode(buf, &size)

	tmp := make([]T, size)
	decode(buf, &tmp)

	val1 := reflect.ValueOf(elem)
	val2 := reflect.ValueOf(tmp)

	val1.Elem().Set(val2)
}

func encode(buf *bytes.Buffer, elem any) {
	switch v := elem.(type) {
	case int:
		binary.Write(buf, binary.BigEndian, int64(v))

	default:
		binary.Write(buf, binary.BigEndian, v)
	}
}

func Decode(buf bytes.Buffer, items ...any) error {
	for _, item := range items {
		kind := reflect.TypeOf(item).Kind()

		if kind == reflect.Ptr {
			elem := reflect.TypeOf(item).Elem()

			if elem.Kind() == reflect.Slice {
				elem  = elem.Elem()

				switch elem.Kind() {
					case reflect.Uint8:  DecodeSlice[uint8](&buf, item)
					case reflect.Uint16: DecodeSlice[uint16](&buf, item)
					case reflect.Uint64: DecodeSlice[uint64](&buf, item)
					case reflect.Uint32: DecodeSlice[uint32](&buf, item)

					case reflect.Int64: DecodeSlice[int64](&buf, item)
					case reflect.Int32: DecodeSlice[int32](&buf, item)
					case reflect.Int16: DecodeSlice[int16](&buf, item)
					case reflect.Int8:  DecodeSlice[int8](&buf, item)

					case reflect.Float64: DecodeSlice[float64](&buf, item)
					case reflect.Float32: DecodeSlice[float32](&buf, item)

				default:
					fmt.Printf("unsupported type: %v", elem.Kind())
				}
			}
		}

		decode(&buf, item)
	}

	return nil
}

func decode(buf *bytes.Buffer, dst any) {
	binary.Read(buf, binary.BigEndian, dst)	
}