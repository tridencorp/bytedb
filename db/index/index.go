package index

import (
	"bucketdb/db/file"
	"bytes"
	"hash/fnv"
	"os"
	"unsafe"
)

// key stores information about key-value position in database files.
type key struct {
	Offset uint32  // 4 bytes
	Bucket uint32  // 4 bytes
	Size   uint16  // 2 bytes
	Hash   [6]byte // 6 bytes
}

type Index struct {
	file        *file.File
	keys        []key
	keysPerFile int64
}

// Open and load indexes. It creates a new index file
// if it doesn't already exist.
func Open(dir string, keysPerFile int64) (*Index, error) {
	f, err := file.Open(dir, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, nil
	}

	i := &Index{file: f, keysPerFile: keysPerFile}
	i.Prealloc(keysPerFile)

	return i, nil
}

// Preallocate space for max number of keys in index file.
// It's also adding extra space for collisions.
func (i *Index) Prealloc(keys int64) (int64, error) {
	// Calculate required space for all keys and additional 30% for collisions.
	size := keys * int64(unsafe.Sizeof(key{}))
	size = (size * 130) / 100

	if i.file.Size() < size {
		err := i.file.Resize(size)
		if err != nil {
			return 0, err
		}
	}

	return size, nil
}

// Set index for the given kv and stores it in the index file.
func (i *Index) Set(key []byte) error {
	h := Hash(key)

	// Find proper block number for key.
	n := int64(h % uint64(i.file.BlockCount()))

	i.file.WriteBlock(n, key)
	return nil
}

// Get index.
func (i *Index) Get(key []byte) ([]byte, error) {
	h := Hash(key)

	// Find proper block number for key.
	n := int64(h % uint64(i.file.BlockCount()))

	// Read block.
	b, err := i.file.ReadBlock(n)
	if err != nil {
		return nil, err
	}

	// Find our key.
	// TODO: We know the fixed  size, so we can make it quicker than Index - probably 🤞
	index := bytes.Index(b, key)
	if index == -1 {
		return nil, nil
	}

	return b[index : index+len(key)], nil
}

// Hash given key.
func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}
