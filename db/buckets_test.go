package db

import (
	"testing"
)

func TestOpenBuckets(t *testing.T) {
	conf := Config{2, 1_000_000, 2, 100}
	_, err := OpenBuckets("./test", conf)
	if err != nil {
		t.Errorf("Error when opening buckets: %s", err)
	}
}
