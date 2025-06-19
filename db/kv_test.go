package db

import (
	"bucketdb/tests"
	"fmt"
	"os"
	"testing"
)

func TestKVSetGet(t *testing.T) {
	index, _ := OpenIndex(".index.idx", 1000)
	defer os.Remove(".index.idx")

	dataDir := Dir("./test", 10, "bin")
	defer os.RemoveAll("./test")

	kv, _ := OpenKV(".data.kv", dataDir, index)
	defer os.Remove(".data.kv")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)

		kv.Set([]byte(key), []byte(val))
	}

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key_%d", i)
		v := fmt.Sprintf("val_%d", i)

		b, _ := kv.Get([]byte(k))

		key := [5]byte{}
		val := [5]byte{}

		Decode2(b, key[:], val[:])

		tests.Assert(t, k, string(key[:]))
		tests.Assert(t, v, string(val[:]))
	}
}
