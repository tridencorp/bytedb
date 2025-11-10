package common

import (
	"fmt"
	"reflect"
)

func Encode(elements ...any) {
	for _, elem := range elements {
		// Indirect pointers
		val := reflect.ValueOf(elem)
		val = reflect.Indirect(val)

		fmt.Println(IsByteSlice(val))
	}
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

