package db

import (
	"bytes"
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
	_, err := os.Stat("./db/collections/test/1/1.bucket")
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

func TestSet(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	coll, _ := db.Collection("test")

	coll.Set("key1", []byte("value 1"))
	coll.Set("key2", []byte("value 2"))
	coll.Set("key3", []byte("value 3"))
}

func TestSetGet(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	coll, _ := db.Collection("test")

	val1 := []byte("value 1")
	val2 := []byte("value 2")
	val3 := []byte("value 3")

	coll.Set("key1", val1)
	coll.Set("key2", val2)
	coll.Set("key3", val3)

	got1, _ := coll.Get("key1")
	got2, _ := coll.Get("key2")
	got3, _ := coll.Get("key3")

	if !bytes.Equal(got1, val1) { t.Errorf("Expected %s, got %s", val1, got1) }
	if !bytes.Equal(got2, val2) { t.Errorf("Expected %s, got %s", val2, got2) }
	if !bytes.Equal(got3, val3) { t.Errorf("Expected %s, got %s", val3, got3) }
}

func TestIterate(t *testing.T) {
	db, _ := Open("./db")
	// defer db.Delete()

	coll, _ := db.Collection("test")

	coll.Set("key1", []byte("value 1"))
	coll.Set("key2", []byte("value 2"))
	coll.Set("key3", []byte("value 3"))

	it := Iterator{bucket: coll.bucket}
	keys, size, _ := it.Iterate()


	if len(keys) != 3 {
		t.Errorf("Expected to get %d keys, got %d", 3, len(keys))
	}

	if size != 33 {
		t.Errorf("Expected size to be %d keys, got %d", 33, size)
	}
}

func TestLoadIndexFile(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	coll, _ := db.Collection("test")

	indexes, err := LoadIndexFile(coll.root)
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
}

func TestDel(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	coll, _ := db.Collection("test")
	coll.Set("key1", []byte("value1"))

	coll.Del("key1")
	res, _ := coll.Get("key1")

	if !bytes.Equal(res, []byte{}) { 
		t.Errorf("Key should be nil, instead we got %v", res) 
	}
}	

func TestUpdate(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	coll, _ := db.Collection("test")
	coll.Set("key1", []byte("value1"))

	coll.Update("key1", []byte("value2"))
	res, _ := coll.Get("key1")

	if !bytes.Equal(res, []byte("value2")) {
		t.Errorf("Expected to get %s, got %s", []byte("value2"), res)
	}
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
