package db

import (
	"bytedb/collection"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCollectionAdd(t *testing.T) {
	coll := OpenCollection("test")
	defer os.RemoveAll("./test")

	k := collection.NewKey([]byte("Key_1"))
	v := []byte("Val_1")

	coll.Add(k, v)

	fmt.Println(filepath.Clean(coll.Dir))
}
