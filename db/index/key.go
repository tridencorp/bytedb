package index

import (
	"bytes"
	"encoding/binary"
)

// Key
// Why not structs or slices? Because each one of them
// has overhead of 24 bytes.
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
	return copy(k[:20], key)
}

// Get key name.
func (k *Key) Name() string {
	return string(k[:20])
}

// Check if bytes 20:24 are set. If they are, this indicates that
// the index for the next key is set, meaning we have a collision.
func (k *Key) HasCollision() bool {
	return !bytes.Equal(k[20:24], []byte{0, 0, 0, 0})
}

// Set key slot.
func (k *Key) SetSlot(index uint32) {
	binary.BigEndian.PutUint32(k[20:24], index)
}

// Set key offset.
func (k *Key) SetOffset(offset uint64) {
	binary.BigEndian.PutUint64(k[24:], offset)
}

// Return key offset in index file.
func (k *Key) Offset() int64 {
	return int64(binary.BigEndian.Uint64(k[24:]))
}

// Return key slot in Collisions table.
func (k *Key) Slot() uint32 {
	return binary.BigEndian.Uint32(k[20:])
}

// Check if kv name is egual to key. 
// TODO: Can be simplified.
func (k *Key) Equal(kv []byte) bool {
	tmp := Key{}
	copy(tmp[:20], kv)

	return bytes.Equal(k[:20], tmp[:20])
}
