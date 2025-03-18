package db

import (
	"bucketdb/db/buckets"
	"bucketdb/tests"
	"os"
	"testing"
)

func TestNewCollection(t *testing.T) {
	conf := buckets.Config{100, 1_000_000, 2, 100}
	col, _ := newCollection("./db/collections/test", conf)
	defer os.RemoveAll("./db")

	tests.Assert(t, col.buckets.Last().ID, 1)
}
