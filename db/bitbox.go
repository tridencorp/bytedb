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
			binary.Write(&buf, binary.BigEndian, *v)

		case []byte:
			encode(&buf, len(v))
			encode(&buf, v)

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

			// Try to do binary Write for basic types, int, float, ... 
			encode(&buf, elem)
		}
	}

	return buf, nil
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
				size := int64(0)

				switch elem.Kind() {
				// []byte, []uint8
				case reflect.Uint8:
					decode(&buf, &size)

					tmp := make([]byte, size)
					decode(&buf, &tmp)

					val1 := reflect.ValueOf(item)
					val2 := reflect.ValueOf(tmp)

					val1.Elem().Set(val2)

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