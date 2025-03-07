package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"sync/atomic"
)

const (
  TypeKv   = 0
	TypeHash = 1 
)

// Index size in bytes.
const IndexSize = 37 
const BlockSize = IndexSize * 6

// Index will represent key in our database.
type Index struct {
	Key [20]byte // 20 bytes

  // Next collision key.
  next uint64

  // KeyVal
	Deleted    bool  // 1 byte
	BucketId uint32  // 4 bytes
	Size     uint32  // 4 bytes
	Offset   uint64  // 8 bytes
}

// Key
//
// [0:20]  - first 20 bytes are keyval name.
// [20:24] - next 4 bytes are index to next collision key.
// [24:32] - last 8 bytes are index offset in file.
type Key [32]byte

func (k *Key) Empty() bool {
	return *k == *new(Key)
}

func (k *Key) Set(key []byte) int {
	return copy(k[:], key)	
}

// Check if bytes 20:24 are set. If they are, this indicates that
// the index for the next key is set, meaning we have a collision.
func (k *Key) HasCollision() bool {
	return !bytes.Equal(k[20:24], []byte{0, 0, 0, 0})
}

type IndexFile struct {
  fd *os.File

  // Keeping key/collision offsets in memory.
  Keys       []Key
  Collisions []Key

  // Offset of next collision slot in index file.
  CollisionOffset atomic.Uint64

	// Number of indexes file can handle.
	indexesPerFile uint64
}

// Load index file from given directory. 
func Load(dir string, indexesPerFile uint64) (*IndexFile, error) {
	// TODO: Only temporary and will be replaced by proper index file.
	dir += "/index.idx"

	file, err := os.OpenFile(dir, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

  f := &IndexFile{fd: file, indexesPerFile: indexesPerFile}
	f.Keys = make([]Key, f.indexesPerFile)

	size := uint64(math.Ceil(float64(30.0*float64(f.indexesPerFile)/100))) 
	f.Collisions = make([]Key, size)

	return f, nil
}


// Create an index for the given key/value and store it in the index file.
// This will allow us for faster lookups.
func (f *IndexFile) Set(keyName []byte, size int, keyOffset uint64, bucketID uint32) error {	
	hash := HashKey(keyName)
	off  := hash % f.indexesPerFile

	// Find key in Keys.
	key := &f.Keys[off]
	fmt.Println(key)

	if key.Empty() {
		key.Set(keyName)
	}

	if key.HasCollision() {
		fmt.Println("xxxxxx")
	}

	fmt.Println(f.Keys[off])

  // idx := Index{BucketId: 1, Size: uint32(size), Offset: keyOffset}
  // /opy(idx.Key[:], keyName)
	
	// buf := new(bytes.Buffer)
	// err := binary.Write(buf, binary.BigEndian, block)
	// if err != nil {
		//   return err
		// }
		
		// off := file.offset(hash)
		// _, err = file.fd.WriteAt(buf.Bytes(), int64(off))
		// if err != nil {
			// 	return err
			// }
			
		return nil
	}
		
// Load index file.
func LoadIndexFile(path string, indexesPerFile uint64) (*IndexFile, error) {
	path += "/index.idx"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil
	}

	f := &IndexFile{fd: file, indexesPerFile: indexesPerFile}

	// ~30% of keys size.
	// size := uint64(math.Ceil(float64(30.0*float64(f.indexesPerFile)/100))) 
	return f, nil
}

// Calculate index offset for new key.
// Also checks for hash collisions 
// and update the offset accordingly.
func (file *IndexFile) offset(hash uint64) uint64 {
  return hash % file.indexesPerFile * BlockSize
}

// Read index for given key.
func (file *IndexFile) Get(key []byte) (*Index, error) {
	// Find index position
	offset := file.offset(HashKey(key))
	data := make([]byte, IndexSize)

	file.fd.ReadAt(data, int64(offset))
	idx := Index{}

	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &idx)
	if err != nil {
		return nil, err
	}

	if idx.Deleted {
		return nil, fmt.Errorf("Key was %s deleted", key)
	}

	return &idx, nil
}

// Delete index for given key.
func (file *IndexFile) Del(key []byte) error {
	// Find index offset.
	offset := file.offset(HashKey(key))

	// If we know the position of index, we can just
	// set it's second byte to 1.
	// TODO: struct changed, this won't work anymore.
	_, err := file.fd.WriteAt([]byte{1}, int64(offset + 1))
	return err
}

// Hash the key.
func HashKey(key []byte) uint64 {
 	h := fnv.New64()
	h.Write(key)
	return h.Sum64()
}
