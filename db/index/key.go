package index

import (
	"bytes"
	"encoding/binary"
)

// Key
//
// [0:20]  - first 20 bytes are keyval name.
// [20:24] - next 4 bytes are index to next slot in Collisions table.
// [24:32] - last 8 bytes are index offset in file.
type Key [32]byte

func (k *Key) Empty() bool {
	return *k == *new(Key)
}

// Set key name (kv name).
func (k *Key) Set(key []byte) int {
	return copy(k[:], key)
}

// Check if bytes 20:24 are set. If they are, this indicates that
// the index for the next key is set, meaning we have a collision.
func (k *Key) HasCollision() bool {
	return !bytes.Equal(k[20:24], []byte{0, 0, 0, 0})
}

// Set key slot.
func (k *Key) SetSlot(index uint32) {
	binary.BigEndian.PutUint32(k[20:], index)
}

// Set key offset.
func (k *Key) SetOffset(offset uint64) {
	binary.BigEndian.PutUint64(k[24:], offset)
}

// Return key offset in index file.
func (k *Key) Offset() int64 {
	return int64(binary.BigEndian.Uint64(k[24:]))
}
