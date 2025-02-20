package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

type Encoder interface {
	Encode() []byte
}

type Decoder interface {
	Decode([]byte) error
}

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

					encode(&buf, len(bytes))
					encode(&buf, bytes)
				}
				continue
			}

			// Case for custom slice like types.
			if val.Kind() == reflect.Slice {
				_, ok := val.Index(0).Interface().(Encoder)
				if ok {
					// Encode total number of elements in slice.
					encode(&buf, val.Len())
					// Iterate all elements.
					for i:=0; i < val.Len(); i++  {
						encoder, _ := val.Index(i).Interface().(Encoder)
						bytes := encoder.Encode()

						// Encode each element size and bytes.
						encode(&buf, len(bytes))
						encode(&buf, bytes)
					}
					continue
				}

				encode(&buf, val.Len())
				encode(&buf, elem)
				continue
			}

			encode(&buf, elem)
		}
	}

	return buf, nil
}

func EncodeSlice[T any](buf *bytes.Buffer, elem []T) {
	encode(buf, int64(len(elem)))
	tmp := make([]T, len(elem))

	// More efficient than copying?
	elem, tmp = tmp, elem
	encode(buf, tmp)
}

func DecodeSlice[T any](buf *bytes.Buffer, dst any) {
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

func encode(buf *bytes.Buffer, elem any) {
	switch v := elem.(type) {
	case int:
		binary.Write(buf, binary.BigEndian, int64(v))

	default:
		binary.Write(buf, binary.BigEndian, v)
	}
}

func Decode(buf *bytes.Buffer, items ...any) error {
	for _, item := range items {
		elem := reflect.TypeOf(item)
		val  := reflect.ValueOf(item)

		if elem.Kind() == reflect.Ptr && elem.Elem().Kind() == reflect.Slice {
			elem = elem.Elem().Elem()

			switch elem.Kind() {
				case reflect.Uint8:  DecodeSlice[uint8](buf, item)
				case reflect.Uint16: DecodeSlice[uint16](buf, item)
				case reflect.Uint64: DecodeSlice[uint64](buf, item)
				case reflect.Uint32: DecodeSlice[uint32](buf, item)

				case reflect.Int64: DecodeSlice[int64](buf, item)
				case reflect.Int32: DecodeSlice[int32](buf, item)
				case reflect.Int16: DecodeSlice[int16](buf, item)
				case reflect.Int8:  DecodeSlice[int8](buf, item)

				case reflect.Float64: DecodeSlice[float64](buf, item)
				case reflect.Float32: DecodeSlice[float32](buf, item)

			default:
				// Case for custom slice like types.
				if reflect.TypeOf(item).Kind() == reflect.Ptr && reflect.TypeOf(item).Elem().Kind() == reflect.Slice {
					t1  := reflect.TypeOf(item)
					t2  := t1.Elem().Elem().Elem()
					tmp := val.Elem()

					// // Get the number of elements in slice.
					size := int64(0)
					decode(buf, &size)

					for i:=int64(0); i < size; i++ {
						// // Get number of bytes per each element.
						size = int64(0)
						decode(buf, &size)
	
						// // Get bytes.
						bytes := make([]byte, size)
						decode(buf, bytes)

						ins, _ := reflect.New(t2).Interface().(Decoder)
						ins.Decode(bytes)

						tmp = reflect.Append(tmp, reflect.ValueOf(ins))
					}	

					val.Elem().Set(tmp)
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
		}

		decode(buf, item)
	}

	return nil
}

func decode(buf *bytes.Buffer, dst any) {
	err := binary.Read(buf, binary.BigEndian, dst)	
	if err != nil {
		fmt.Println(err)
	}
}

func isStruct(v reflect.Value) bool {
	return v.Kind() == reflect.Struct	
}
