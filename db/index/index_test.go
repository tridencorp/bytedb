package index

import (
	"fmt"
	"os"
	"testing"
)

func TestIndexSet(t *testing.T) {
	num := 5_000

	file, _ := Load(".", uint64(num))
	defer os.Remove("./index.idx")

	for i:=0; i < num; i++ {
		key := fmt.Sprintf("key_%d", i)
		file.Set([]byte(key), 10, 10, 1)	
	}

	count := 0
	for _, key := range file.Keys {
		if !key.Empty() {
			// fmt.Println(key)
			count++
		}
	} 

	count2 := 0
	for _, key := range file.Collisions {
		if !key.Empty() {
			fmt.Println(key)
			count2++
		}
	} 

	fmt.Println("keys: ", count)
	fmt.Println("collisions: ", count2)
}
