package db

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenBucket(t *testing.T) {
	db1, _ := Open("./db")
	db1.Delete()

	coll, _:= db1.Collection("test")

	bck, _ := OpenBucket(coll.root, 10, 5, 2)
	if bck == nil {
		t.Errorf("Expected Bucket object, got nil")
	}
}

func TestBucketWrite(t *testing.T) {
	db1, _ := Open("./db")
	db1.Delete()

	coll, _ := db1.Collection("test")
	bck, _ := OpenBucket(coll.root, 10, 5, 2)

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
	bck,  _ := OpenBucket(coll.root, 10, 5, 2)

	data1 := []byte("value1")
	bck.Write(data1)

	data2, _ := bck.Read(0, 6)

	if !bytes.Equal(data1, data2) {
		t.Errorf("Expected read to return %s, got %s.", data1, data2)
	}
}

func TestBucketResize(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	coll, _ := db.Collection("test")
	bck,  _ := OpenBucket(coll.root, 100, 5, 2)

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

func TestGetLastBucket(t *testing.T) {
	path1 := "./db/collections/test/1/10.bucket"
	path2 := "./db/collections/test/2/300.bucket"
	path3 := "./db/collections/test/11/300.bucket"
	path4 := "./db/collections/test/12/100.bucket"

	dir1 := filepath.Dir(path1)
	os.MkdirAll(dir1, 0755)
	os.Create(path1)

	dir2 := filepath.Dir(path2)
	os.MkdirAll(dir2, 0755)
	os.Create(path2)

	dir3 := filepath.Dir(path3)
	os.MkdirAll(dir3, 0755)
	os.Create(path3)

	dir4 := filepath.Dir(path4)
	os.MkdirAll(dir4, 0755)
	os.Create(path4)

	defer os.RemoveAll("./db")

	expected := "./db/collections/test/12/100.bucket"
	file, _ := getLastBucket("./db/collections/test")

	if file.Name() != expected {
		t.Errorf("Expected path to be %s, got %s.", expected, file.Name())
	}
}

func TestNextBucket(t *testing.T) {
	bucketsPerDir := 2

	// 1. Dir is full.
	path1 := "./db/collections/test/1/1.bucket"
	path2 := "./db/collections/test/1/2.bucket"


	dir1 := filepath.Dir(path1)
	os.MkdirAll(dir1, 0755)
	os.Create(path1)

	dir2 := filepath.Dir(path2)
	os.MkdirAll(dir2, 0755)
	os.Create(path2)

	defer os.RemoveAll("./db")

	bucket := Bucket{
		ID: 2,
		Dir: "./db/collections/test/",
		bucketsPerDir: int16(bucketsPerDir),
	}

	bucket.nextBucket()

	if bucket.ID != 3 {
		t.Errorf("Expected bucket ID to be %d, got %d.", 3, bucket.ID)
	}

	expected := "./db/collections/test/2/3.bucket"
	if bucket.file.Name() != expected {
		t.Errorf("Expected file to be %s, got %s.", expected, bucket.file.Name())
	}

	// 2. Dir have space.
	bucket.nextBucket()

	if bucket.ID != 4 {
		t.Errorf("Expected bucket ID to be %d, got %d.", 4, bucket.ID)
	}

	expected = "./db/collections/test/2/4.bucket"
	if bucket.file.Name() != expected {
		t.Errorf("Expected file to be %s, got %s.", expected, bucket.file.Name())
	}
}