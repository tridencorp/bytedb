package collection

import "hash/fnv"

type Key struct {
	Collection uint64
	Namespace  uint64
	Prefix     uint64
	Hash       uint64
}

func NewKey(name []byte) *Key {
	return &Key{Hash: Hash(name)}
}

// Compute 64bit hash
func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))

	return h.Sum64()
}
