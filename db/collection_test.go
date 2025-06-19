package db

import (
	"os"
	"testing"
)

func TestCollectionSetGet(t *testing.T) {
	c := OpenCollection("test", "./test")
	defer os.RemoveAll("./test")

	c.Set([]byte("key"), []byte("val"))
	// c.Get([]byte("key"))
}
