package db

import (
	"fmt"
	"os"
	"testing"
)

func TestIndexSet(t *testing.T) {
	file, _ := LoadIndexFile(".", 5_000)
	defer os.Remove("./index.idx")

	key := fmt.Sprintf("key_%d", 1)
	file.Set([]byte(key), 10, 10, 1)

	fmt.Println("keys: ", len(file.Keys))
	fmt.Println("collisions: ", len(file.Collisions))

	
}
