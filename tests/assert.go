package tests

import (
	"reflect"
	"testing"
)

func Assert[T comparable](t *testing.T, expected, actual T) {
	t.Helper()

	if expected != actual { 
		t.Errorf("Assertion failed: expected %v, got %v", expected, actual)
	}
}

func AssertEqual[T any](t *testing.T, expected, actual T) {
	t.Helper()

	if !reflect.DeepEqual(expected, actual) { 
		t.Errorf("Assertion failed: expected %v, got %v", expected, actual)
	}
}
