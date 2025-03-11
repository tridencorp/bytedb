package index

import (
	"fmt"
	"os"
	"testing"
)

func TestIndexSet(t *testing.T) {
	num := 100_000
	file, _ := Load(".", uint64(num))
	defer os.Remove("./index.idx")

	for i:=0; i < num; i++ {
		key := fmt.Sprintf("key_%d", i)
		file.Set([]byte(key), 10, 10, 1)	
	}

	for i:=0; i < num; i++ {
		key  := fmt.Sprintf("key_%d", i)
		i, _ := file.Get([]byte(key))

		want := HashKey([]byte(key))
		got  := i.Hash

		if want != got { 
			t.Errorf("Expected %d, got %d", want, got) 
		}
	}
}

func TestLoader(t *testing.T) {
	num := 100_000
	file, _ := Load(".", uint64(num))
	defer os.Remove("./index.idx")

	for i:=0; i < num; i++ {
		key := fmt.Sprintf("key_%d", i)
		file.Set([]byte(key), 10, 10, 1)	
	}

	file.Keys       = make([]Key, 0)
	file.Collisions = make([]Key, 0)

	file.LoadIndexes(1024*1024*1)

	for i:=0; i < num; i++ {
		key := fmt.Sprintf("key_%d", i)
		i, _ := file.Get([]byte(key))

		want := HashKey([]byte(key))
		got  := i.Hash

		if want != got { 
			t.Errorf("Expected %s: %d, got %d", key, want, got) 
		}
	}
}
