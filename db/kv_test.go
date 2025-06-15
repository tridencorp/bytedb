package db

import (
	"fmt"
	"os"
	"testing"
)

func TestKVWriteRead(t *testing.T) {
	kv, _ := OpenKV(".data.kv")
	defer os.Remove(".data.kv")

	fmt.Println(kv)
}
