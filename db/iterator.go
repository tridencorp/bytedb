package db

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Iterator is iterating all keys in collection.
type Iterator struct {
	bucket *Bucket
}

func (it *Iterator) Iterate() ([]*Key, int64, error) {
	// Read the whole file.
	// TODO: Read it in chunks.
	file := it.bucket.file.Load()

	data, err := io.ReadAll(file.fd)
	if err != nil {
		return nil, 0, err
	}

	keys := []*Key{}
	buf  := bytes.NewReader(data)

	totalSize := int64(0)

	for {
		key := new(Key)

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

		// Read key data based on size that we got.
		key.data = make([]byte, size)
		key.size = size

		err = binary.Read(buf, binary.BigEndian, &key.data)
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
