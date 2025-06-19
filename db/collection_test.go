package db

import (
	"bucketdb/tests"
	"os"
	"testing"
)

func TestCollectionSetGet(t *testing.T) {
	c := OpenCollection("test", "./test")
	defer os.RemoveAll("./test")

	c.Set([]byte("key"), []byte("Hello World"))
	val, _ := c.Get([]byte("key"))

	tests.Assert(t, "Hello World", string(val))
}
