package db

import (
	"testing"
)

func TestOpens(t *testing.T) {
	db1, _ := Open("./db")
	db1.Delete()

	coll, _:= db1.Collection("test")

	bck, _ := OpenBucket(coll.root + "/1.bucket")
	if bck == nil {
		t.Errorf("Expected Bucket object, got nil")
	}
}
