package db

import (
	"fmt"

	"golang.org/x/exp/rand"
)

func CreateCollection(name string, keysLimit uint32, sizeLimit int64, bucketsPerDir int32) (*DB, *Collection) {
	database, _ := Open("./testdb")

	conf    := Config{KeysLimit: 2, SizeLimit: 10, BucketsPerDir: 2}
	coll, _ := database.Collection("test", conf)

	return database, coll
}

// Write the specified number of keys to the collection, with each key having the given size in Bytes.
func FillCollection(coll *Collection, numOfKeys, sizeOfKey uint32) (written int64, values [][]byte) {
	for i:=0; i < int(numOfKeys); i++ {
		val := make([]byte, sizeOfKey)

		// Fill it with random bytes.
		for i := range val {
			val[i] = byte(rand.Intn(255))
		}

		key := fmt.Sprintf("key_%d", i)
		_, size, _ := coll.Set(key, val)

		written += size
		values = append(values, val)
	}

	return written, values
}