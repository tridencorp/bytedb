package db

import (
	"bucketdb/tests"
	"testing"
)

func TestOpenBuckets(t *testing.T) {
	conf := Config{2, 1_000_000, 2, 100}

	_, err := OpenBuckets("./test", conf)
	tests.Assert(t, err, nil)
}
