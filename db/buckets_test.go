package db

import (
	"testing"
)

func TestOpenBuckets(t *testing.T) {
	conf := Config{KeysLimit: 2, SizeLimit: 1_000_000, BucketsPerDir: 2}
	_, err := OpenBuckets("./test", 100, conf)
	if err != nil {
		t.Errorf("Error when opening buckets: %s", err)
	}
}
