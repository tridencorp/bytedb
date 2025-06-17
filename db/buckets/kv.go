package buckets

import (
	"bytes"
	"encoding/binary"
)

// KV structure that represents each key/value stored in collection.
// TODO: Key will be hashed key, uint64.
type KV struct {
	Key []byte
	Val []byte
}

func NewKV(key string, val []byte) *KV {
	return &KV{[]byte(key), val}
}

// Read kv data from bytes.
func (kv *KV) FromBytes(raw []byte) {
	buf := bytes.NewBuffer(raw)

	// Decode size and data. We don't need size at this point
	// so we can just skip next 4 bytes (since size is uint32)
	buf.Next(4)

	kv.Val = buf.Bytes()
}

// Encode kv to bytes.
func (kv *KV) Bytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Encode size. We need this in case we will have to re-create
	// keys without indexes.
	err := binary.Write(buf, binary.BigEndian, uint32(len(kv.Val)))
	if err != nil {
		return nil, err
	}

	// Write value.
	_, err = buf.Write(kv.Val)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
