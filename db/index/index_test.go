package index

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestIndexSet(t *testing.T) {
	num := 5_000

	file, _ := Load(".", uint64(num))
	defer os.Remove("./index.idx")

	hash := HashKey([]byte("key_4997"))
	off  := hash % file.indexesPerFile
	fmt.Println("off 1:", off)

	hash = HashKey([]byte("key_3080"))
	off2 := hash % file.indexesPerFile
	fmt.Println("off 2:", off2)

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
			t.Errorf("Expected %s, got %v", expected, i.Key[:20]) 
		}
	}

	index, _ := file.Get([]byte("key_4997"))
	fmt.Println("index: ", index)
}
