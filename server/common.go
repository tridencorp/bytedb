package server

import "hash/fnv"

// Compute hash
func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}
