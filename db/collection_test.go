package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCollection(t *testing.T) {
	coll := OpenCollection("test")
	defer os.RemoveAll("./test")

	fmt.Println(filepath.Clean(coll.Dir))
}
