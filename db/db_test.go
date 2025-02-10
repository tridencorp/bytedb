package db

import (
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	if db.root != "./db" {
		t.Errorf("Database root directory was not set.")
	}
}

func TestCollection(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	db.Collection("test")

	// At this point, we only want to check if the proper
	// directories have been created.
	_, err := os.Stat("./db/collections/test/1.bucket")
	if err != nil {
		t.Errorf("Collection path doesn't exist.")
	}
}

func TestDelete(t *testing.T) {
	db, _ := Open("./db")
	db.Collection("test")

	db.Delete()

	// Whole database should be removed.
	_, err := os.Stat("./db")
	if err == nil {
		t.Errorf("Database still exists but should be removed.")
	}
}

// func TestSet(t *testing.T) {
// 	db, _ := Open("./db")
// 	defer db.Delete()

// 	value	:= []byte("value 1")

// 	coll, _ := db.Collection("test")
// 	size, _ := coll.Set("key1", value)

// 	if size != len(value) {
// 		t.Errorf("Error while writing to collection. Expected %d bytes to be written, got %d.", len(value), size)
//  }
// }

func TestSet(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	coll, _ := db.Collection("test")

	coll.Set("key1", []byte("value 1"))
	coll.Set("key2", []byte("value 2"))
	coll.Set("key3", []byte("value 3"))
}
