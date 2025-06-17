package db

import (
	"fmt"
	"os"
	"testing"
)

func TestKVSetGet(t *testing.T) {
	kv, _ := OpenKV(".data.kv")
	defer os.Remove(".data.kv")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%d", i)
		off, _ := kv.Set([]byte(key), []byte("value"))
		fmt.Println(off)
	}

	// for i := 0; i < 10; i++ {
	// 	key := fmt.Sprintf("key_%d", i)
	// 	val, _ := kv.Get([]byte(key))
	// 	fmt.Println(val)
	// }
}
