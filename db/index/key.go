package index

import (
	"bytes"
	"encoding/binary"
)

// Key
//
// Why not struct or slice? Because each one of them
// has 24 bytes of overhead.
//
// [0:8]   - first 8 bytes are key hash.
// [8:12]  - next 4 bytes are index to next position in collision table.
// [12:20] - last 8 bytes are index offset in file.
type Key [20]byte

func (k *Key) Empty() bool {
	return *k == *new(Key)
}

// Set key hash.
func (k *Key) SetHash(hash uint64) {
	binary.BigEndian.PutUint64(k[0:], hash)
}

// Get key hash.
func (k *Key) Hash() uint64 {
	return binary.BigEndian.Uint64(k[0:])
}

// Check if bytes 8:12 are set. If they are, this indicates that
// the index for the next key is set, meaning we have a collision.
func (k *Key) HasCollision() bool {
	return !bytes.Equal(k[8:12], []byte{0, 0, 0, 0})
}

// Set key position.
func (k *Key) SetPosition(pos uint32) {
	binary.BigEndian.PutUint32(k[8:], pos)
}

// Set key offset.
func (k *Key) SetOffset(offset uint64) {
	binary.BigEndian.PutUint64(k[12:], offset)
}

// Return key offset in index file.
func (k *Key) Offset() int64 {
	return int64(binary.BigEndian.Uint64(k[12:]))
}

// Return key position in collision table.
func (k *Key) Position() uint32 {
	return binary.BigEndian.Uint32(k[8:])
}

// Check if hashes match key name.
func (k *Key) Equal(name []byte) bool {
	return k.Hash() == HashKey(name)
}
