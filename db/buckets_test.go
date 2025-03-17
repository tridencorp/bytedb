package db

import (
	"testing"
)

func TestOpenBuckets(t *testing.T) {
	conf := Config{MaxKeys: 2, MaxSize: 1_000_000, MaxPerDir: 2}
	_, err := OpenBuckets("./test", 100, conf)
	if err != nil {
		t.Errorf("Error when opening buckets: %s", err)
	}
}
