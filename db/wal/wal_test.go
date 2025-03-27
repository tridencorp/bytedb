package wal

import (
	"fmt"
	"testing"
)

func TestWrite(t *testing.T) {
	wal, _ := Open("test.wal")
	fmt.Println(wal)
}
