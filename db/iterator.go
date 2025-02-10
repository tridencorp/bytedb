package db

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Iterator is iterating all keys in collection.
type Iterator struct {
	coll *Collection
}

func (it *Iterator) Iterate() ([]*Key, error) {
	// Read whole file.
	data, err := io.ReadAll(it.coll.file)
	if err != nil {
		return nil, err
	}

	keys := []*Key{}
	buf  := bytes.NewReader(data)

	for {
		key := new(Key)

		// Read key size
		size := uint32(0)
		err = binary.Read(buf, binary.LittleEndian, &size)
		if err != nil {
			return nil, err
		}

		// Read key data based on size that we got
		key.data = make([]byte, size)
		key.size = size

		err = binary.Read(buf, binary.LittleEndian, &key.data)
		if err != nil {
			return nil, err
		}

		keys = append(keys, key)

		if buf.Len() == 0 {
			break
		}
	}

	return keys, nil
}
