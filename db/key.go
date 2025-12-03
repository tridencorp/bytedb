package db

import "hash/fnv"

type Key struct {
	// Collection
	Collection uint32
	Namespace  uint32

	// Directory
	Dir1 uint8
	Dir2 uint8

	// File
	Prefix uint32
	Hash   uint64

	Name  []byte
	Value []byte
}

func NewKey(key, val []byte) *Key {
	return &Key{Value: val}
}

// Compute 64 bit hash
func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))

	return h.Sum64()
}
