package db

import (
	"bytes"
	"testing"
)

func TestHashSetGet(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	col, _  := db.Collection("test")
	hash, _ := OpenHash("test", col)

	// Test Sete
	val := []byte("hash: value1")
	off, size, _ := hash.Set("hash:key1", val)

	if (off != 0) && (size != 16) {
		t.Errorf("Offset should be %d, got %d. Size should be %d, got %d", 0, off, 16, size)
	}

	// Test Get
	res, _ := hash.Get("hash:key1")
	if !bytes.Equal(res, val) { 
		t.Errorf("Expected %v, got %v", val, res) 
	}
}
