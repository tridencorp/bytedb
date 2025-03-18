package buckets

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Iterator is iterating all keys in collection.
type Iterator struct {
	Bucket *Bucket
}

func (it *Iterator) Iterate() ([]*KV, int64, error) {
	// Read the whole file.
	// TODO: Read it in chunks.
	data, err := io.ReadAll(it.Bucket.file)
	if err != nil {
		return nil, 0, err
	}

	keys := []*KV{}
	buf  := bytes.NewReader(data)

	totalSize := int64(0)

	for {
		key := new(KV)

		// Read key size.
		size := uint32(0)
		err = binary.Read(buf, binary.BigEndian, &size)
		if err != nil {
			return nil, totalSize, err
		}

		// Size 0 means that there is no more data to read.
		// 
		// TODO: This is wrong because we could have
		// situation where we have blank spots between
		// records, so we will have to change this condition
		// in near future. It's temporary.
		if size == 0 {
			break
		}

		// We are adding 4 for size itself (uint32 - 4 bytes).
		totalSize += int64(size + 4)

		// Read key value based on size that we got.
		key.Val = make([]byte, size)

		err = binary.Read(buf, binary.BigEndian, &key.Val)
		if err != nil {
			return nil, totalSize, err
		}

		keys = append(keys, key)

		if buf.Len() == 0 {
			break
		}
	}

	return keys, totalSize, nil
}
