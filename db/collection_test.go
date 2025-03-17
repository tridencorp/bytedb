package db

import (
	"bucketdb/tests"
	"testing"
)

func TestNewCollection(t *testing.T) {
	conf := Config{100, 1_000_000, 2, 100}
	col, _ := newCollection("./db/collections/test", conf)

	tests.Assert(t, col.buckets.Last().ID, 1)
}
