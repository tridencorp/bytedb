package db

import (
	"fmt"
	"os"
	"testing"
)

func TestIndexSet(t *testing.T) {
	file, _ := LoadIndexFile(".")
	defer os.Remove("./index.idx")

	for i:=0; i < 5_000; i++ {
		key := fmt.Sprintf("key_%d", i)
		file.Set([]byte(key), 10, 10, 1)
	}

}
