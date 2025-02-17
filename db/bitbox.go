package db

import (
	"bytes"
	"encoding/binary"
	"reflect"
)

// Encode elements to bytes.
func Encode(elements ...any) ([]byte, error) {
	buf := bytes.Buffer{}

	for _, elem := range elements {
		switch v := elem.(type) {
		case *[]byte:
			binary.Write(&buf, binary.BigEndian, *v)

		case []byte:
			binary.Write(&buf, binary.BigEndian, v)

		default:
			// Fallback for custom types, ex: type Hash []byte
			// Check if we are dealing with slices.
			kind := reflect.TypeOf(elem).Kind()

			if kind == reflect.Slice || kind == reflect.Array {
				val := reflect.ValueOf(elem)

				// Special case for []byte.
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

	return buf.Bytes(), nil
}

func encode(buf *bytes.Buffer, elem any) {
	switch v := elem.(type) {
	case int:
		binary.Write(buf, binary.BigEndian, int64(v))

	default:
		binary.Write(buf, binary.BigEndian, v)
	}
}