package db

import (
	"bucketdb/db/buckets"
	"bytes"
	"testing"
)

func TestHash(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	conf := buckets.Config{100, 30, 2, 100}
	col, _  := db.Collection("test", conf)
	hash, _ := col.Hash("test_hash")

	// Test Sete.
	val := []byte("hash: value1")
	off, size, _ := hash.Set("hash:key1", val)

	if (off != 0) && (size != 16) {
		t.Errorf("Offset should be %d, got %d. Size should be %d, got %d", 0, off, 16, size)
	}

	// Test Get.
	res, _ := hash.Get("hash:key1")
	if !bytes.Equal(res, val) { 
		t.Errorf("Expected %v, got %v", val, res) 
	}

	// Test Update.
	val = []byte("hash: value updated")
	hash.Update("hash:key1", val)

	res, _ = hash.Get("hash:key1")
	if !bytes.Equal(res, val) { 
		t.Errorf("Expected '%s', got '%s'", val, res) 
	}
	
	// Test Get.
	hash.Del("hash:key1")
	if !bytes.Equal(res, val) { 
		t.Errorf("Expected '%s', got '%s'", val, res) 
	}

	res, _ = hash.Get("hash:key1")
	if !bytes.Equal(res, []byte{}) { 
		t.Errorf("Key should be deleted, instead we got '%s'", res) 
	}
}