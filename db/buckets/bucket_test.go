package buckets

import (
	"os"
	"path/filepath"
	"testing"
)

var conf = Config{MaxKeys: 2, MaxSize: 10, MaxPerDir: 2}

// func TestOpenBucket(t *testing.T) {
// 	db1, _ := Open("./db")
// 	db1.Delete()

// 	coll, _:= db1.Collection("test", conf)
// 	bck, _ := OpenBucket(coll.root, conf)
// 	if bck == nil {
// 		t.Errorf("Expected Bucket object, got nil")
// 	}
// }

// func TestBucketWrite(t *testing.T) {
// 	db, _ := Open("./db")
// 	defer db.Delete()

// 	coll, _ := db.Collection("test", conf)
// 	bck, _ := OpenBucket(coll.root, conf)

// 	data := []byte("value_1")
// 	size := int64(0)

// 	for i := 0; i < 100; i++ {
// 		_, len, _, _ := bck.Write(data)
// 		size += len
// 	}

// 	if size != int64(800) {
// 		t.Errorf("Expected %d bytes to be written, got %d.", 800, size)
// 	}
// }

// func TestBucketRead(t *testing.T) {
// 	db, _ := Open("./db")
// 	defer db.Delete()

// 	coll, _ := db.Collection("test", conf)
// 	bck,  _ := OpenBucket(coll.root, conf)

// 	data1 := []byte("value1")
// 	bck.Write(data1)

// 	data2, _ := bck.Read(0, 6)

// 	if !bytes.Equal(data1, data2) {
// 		t.Errorf("Expected read to return %s, got %s.", data1, data2)
// 	}
// }

// func TestBucketResize(t *testing.T) {
// 	db, _ := Open("./db")
// 	defer db.Delete()

// 	conf = Config{MaxKeys: 100, MaxSize: 5, MaxPerDir: 2}
// 	coll, _ := db.Collection("test", conf)
// 	bck,  _ := OpenBucket(coll.root, conf)

// 	data := []byte("value")
// 	for i := 0; i < 10; i++ {
// 		bck.Write(data)
// 	}

// 	file := bck.file.Load()
// 	if file.offset.Load() != 50 {
// 		t.Errorf("Expected offset to be %d, got %d.", 50, file.offset.Load())
// 	}

// 	if file.sizeLimit != 80 {
// 		t.Errorf("Expected size limit to be %d, got %d.", 80, file.sizeLimit)
// 	}
// }

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

	expected := "db/collections/test/12/100.bucket"
	file, _ := GetLastBucket("./db/collections/test")

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

	// defer os.RemoveAll("./db")

	bucket := Bucket{
		ID: 2,
		Dir: "./db/collections/test/",
		bucketsPerDir: int16(bucketsPerDir),
	}

	bucket.nextBucket()
	file := bucket.file

	if bucket.ID != 3 {
		t.Errorf("Expected bucket ID to be %d, got %d.", 3, bucket.ID)
	}

	expected := "db/collections/test/2/3.bucket"
	if file.Name() != expected {
		t.Errorf("Expected file to be %s, got %s.", expected, file.Name())
	}

	// 2. Dir have space.
	bucket.nextBucket()
	file = bucket.file

	if bucket.ID != 4 {
		t.Errorf("Expected bucket ID to be %d, got %d.", 4, bucket.ID)
	}

	expected = "db/collections/test/2/4.bucket"
	if file.Name() != expected {
		t.Errorf("Expected file to be %s, got %s.", expected, file.Name())
	}
}

// func TestGetOffset(t *testing.T) {
// 	db, _ := Open("./db")
// 	defer db.Delete()

// 	coll, _ := db.Collection("test", conf)
// 	bck,  _ := OpenBucket(coll.root, conf)

// 	coll.Set("key1",[]byte("value_1"))
// 	coll.Set("key2",[]byte("value_1"))

// 	off := getOffset(bck)

// 	if off != 24 {
// 		t.Errorf("Expected offset to be %d, got %d.", 24, off)
// 	}
// }
