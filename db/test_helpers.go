package db

import (
	"bucketdb/db/buckets"
	"fmt"

	"golang.org/x/exp/rand"
)

func CreateCollection(name string, conf buckets.Config) (*DB, *Collection) {
	database, _ := Open("./testdb")
	coll, _ := database.Collection("test", conf)

	return database, coll
}

// Write the specified number of keys to the collection, with each key having the given size in Bytes.
func FillCollection(coll *Collection, numOfKeys, sizeOfKey uint32) (int64, map[string][]byte) {
	kv := map[string][]byte{}
	written := int64(0)

	for i:=1; i <= int(numOfKeys); i++ {
		val := make([]byte, sizeOfKey)

		// Fill value with random bytes.
		for i := range val {
			val[i] = byte(rand.Intn(255))
		}

		key := fmt.Sprintf("key_%d", i)
		_, size, _ := coll.Set(key, val)
		written += size

		kv[key] = val
	}

	return written, kv
}