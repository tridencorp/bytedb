package db

import (
	"fmt"
	"testing"
)

func TestHashSet(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	col, _ := db.Collection("test")
	hash, err := OpenHash(col)
	if err != nil {
		fmt.Printf("err: %s\n", err)
	}

	off, size, err := hash.Set("hash:key1", []byte("hash: value1"))
	if err != nil {
		fmt.Printf("err: %s\n", err)
	}

	if (off != 0) && (size != 16) {
		t.Errorf("Offset should be %d, got %d. Size should be %d, got %d", 0, off, 16, size)
	}
}
