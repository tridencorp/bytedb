package db

import (
	"bytes"
	"os"
	"testing"
)

func TestOpenBucket(t *testing.T) {
	db1, _ := Open("./db")
	db1.Delete()

	coll, _:= db1.Collection("test")

	bck, _ := OpenBucket(coll.root + "/1.bucket", 10, 5)
	if bck == nil {
		t.Errorf("Expected Bucket object, got nil")
	}
}

func TestBucketWrite(t *testing.T) {
	db1, _ := Open("./db")
	db1.Delete()

	coll, _ := db1.Collection("test")
	bck,  _ := OpenBucket(coll.root + "/1.bucket", 10, 5)

	data := []byte("value1")
	_, size, _ := bck.Write(data)
	if size != int64(len(data)) {
		t.Errorf("Expected %d bytes to be written, got %d.", int64(len(data)), size)
	}
}

func TestBucketRead(t *testing.T) {
	db1, _ := Open("./db")
	db1.Delete()

	coll, _ := db1.Collection("test")
	bck,  _ := OpenBucket(coll.root + "/1.bucket", 10, 5)

	data1 := []byte("value1")
	bck.Write(data1)

	data2, _ := bck.Read(0, 6)

	if !bytes.Equal(data1, data2) {
		t.Errorf("Expected read to return %s, got %s.", data1, data2)
	}
}

func TestBucketResize(t *testing.T) {
	db1, _ := Open("./db")
	db1.Delete()

	coll, _ := db1.Collection("test")
	bck,  _ := OpenBucket(coll.root + "/1.bucket", 10, 5)

	data := []byte("value")
	for i := 0; i < 10; i++ {
		bck.Write(data)
	}

	if bck.offset.Load() != 50 {
		t.Errorf("Expected offset to be %d, got %d.", 50, bck.offset.Load())
	}

	if bck.sizeLimit != 80 {
		t.Errorf("Expected size limit to be %d, got %d.", 80, bck.sizeLimit)
	}
}

func TestFindLastBucket(t *testing.T) {
	path1 := "./db/collections/test/1/1.bucket"
	path2 := "./db/collections/test/2/20.bucket"
	path3 := "./db/collections/test/11/200.bucket"

	os.MkdirAll(path1, 0755)
	os.MkdirAll(path2, 0755)
	os.MkdirAll(path3, 0755)
	defer os.RemoveAll("./db")

	expected := "./db/collections/test/11/200.bucket"
	id, path := findLastBucket("./db/collections/test")

	if id != 200 {
		t.Errorf("Expected ID to be %d, got %d.", 200, id)
	}

	if path != "./db/collections/test/11/200.bucket" {
		t.Errorf("Expected path to be %s, got %s.", expected, path)
	}
}