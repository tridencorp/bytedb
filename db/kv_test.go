package db

import (
	"fmt"
	"os"
	"testing"
)

func TestKVSetGet(t *testing.T) {
	index, _ := OpenIndex(".index.idx", 1000)
	defer os.Remove(".index.idx")

	kv, _ := OpenKV(".data.kv", index)
	defer os.Remove(".data.kv")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%d", i)
		kv.Set([]byte(key), []byte("value"))
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%d", i)
		val, _ := kv.Get([]byte(key))
		fmt.Println(val)
	}
}
