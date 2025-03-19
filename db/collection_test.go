package db

import (
	"bucketdb/db/buckets"
	"bucketdb/tests"
	"os"
	"testing"
)

func TestCollectionSet(t *testing.T) {
	conf := buckets.Config{ MaxKeys: 100, MaxSize: 1_000_000, MaxPerDir: 10, MaxOpened: 100 }
	col, _ := newCollection("./db/collections/test", conf)
	defer os.RemoveAll("./db")

	tests.RunConcurrently(10_000, func(){
		FillCollection(col, 100, 200)
	})
}
