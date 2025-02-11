package db

import (
	"fmt"
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

func TestIterate(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	coll, _ := db.Collection("test")

	coll.Set("key1", []byte("value 1"))
	coll.Set("key2", []byte("value 2"))
	coll.Set("key3", []byte("value 3"))

	it := Iterator{coll: coll}
	keys, _ := it.Iterate()

	if len(keys) != 3 {
		t.Errorf("Expected to get %d keys, got %d", 3, len(keys))
	}
}

func TestLoadIndexFile(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	coll, _ := db.Collection("test")

	indexes, err := LoadIndexFile(coll)
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	offset, _, _ := coll.Set("key1", []byte("value 1"))

	err = indexes.Add("key1", []byte("value 1"), uint64(offset))
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	idx, _ := indexes.Get("key1")

	if idx == nil        { t.Errorf("Index for the given key wasn't find.") }
	if idx.BucketId != 1 { t.Errorf("Expected bucketId to be %d, was %d", 1, idx.BucketId) }
	if idx.Size != 7     { t.Errorf("Expected Size to be %d, was %d", 7, idx.Size) }
	if idx.Offset != 0   { t.Errorf("Expected Size to be %d, was %d", 0, idx.Offset) }
}

// func TestSetConcurrent(t *testing.T) {
// 	defer func() {
// 		if r := recover(); r != nil {
// 				fmt.Println("Recovered from panic:", r)
// 		}
// 	}()

// 	db, _ := Open("./db")
// 	defer db.Delete()

// 	coll, _ := db.Collection("test")

// 	// We must truncate file !!!
// 	coll.file.Truncate(17_000_000)

// 	for i := 0; i < 500_000; i++ {
// 		go func() {
// 			coll.Set("key1", []byte("value 1"))
// 			coll.Set("key2", []byte("value 2"))
// 			coll.Set("key3", []byte("value 3"))
// 		}()
// 	}

// 	fmt.Printf("OFFSET: %d\n", coll.offset.Load())
// }
