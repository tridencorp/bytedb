package db

import (
	"bucketdb/tests"
	"testing"
)

func TestNewCollection(t *testing.T) {
	conf := Config{}
	col, _ := newCollection("./db/collections/test", conf)

	tests.Assert(t, col.buckets.Last().ID, 1)
}
