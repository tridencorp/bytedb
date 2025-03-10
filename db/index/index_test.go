package index

import (
	"bytes"
	"fmt"
	"testing"
)

func TestIndexSet(t *testing.T) {
	num := 100_000
	file, _ := Load(".", uint64(num))
	// defer os.Remove("./index.idx")

	for i:=0; i < num; i++ {
		key := fmt.Sprintf("key_%d", i)
		file.Set([]byte(key), 10, 10, 1)	
	}

	for i:=0; i < num; i++ {
		key := fmt.Sprintf("key_%d", i)
		i, err := file.Get([]byte(key))
		if err != nil {
			fmt.Println(err)
		}
		
		expected := [20]byte{}
		copy(expected[:20], []byte(key))

		if !bytes.Equal(i.Key[:20], expected[:]) { 
			t.Errorf("Expected %s, got %s", expected, i.Key[:20]) 
		}
	}
}

func TestLoader(t *testing.T) {
	num := 100_000
	file, _ := Load(".", uint64(num))
	file.LoadIndexes(1024*1024*1)
}