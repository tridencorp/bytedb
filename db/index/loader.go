package index

import (
	"fmt"
	"os"
)

// Number of indexes (bytes) to read in each chunk.
const size = 10_000 * IndexSize

type Iterator struct {
	file 			*os.File
	buf 		 	[]byte
	batchSize int
}

func NewIterator(file *os.File, batchSize int) *Iterator {
	return &Iterator{file: file, buf: []byte{}, batchSize: batchSize}
}

func (f *File) LoadIndexes() {
	buff   := make([]byte, size)
	start  := 0
	offset := uint64(0)
	count  := 0

	size := 1024*1024*10 // 10 MB
	it := NewIterator(f.fd, size)

	stat, _   := f.fd.Stat()
	totalCount := stat.Size() / IndexSize

	collisionCount := totalCount - int64(f.indexesPerFile)

	fmt.Println("expected: ", totalCount)

	f.Keys       = make([]Key, f.indexesPerFile)
	f.Collisions = make([]Key, collisionCount)

	// Read keys.
	limit := f.indexesPerFile
	for i:=uint64(0); i < limit; i++ {

	}

	limit = uint64(collisionCount)
	// Read collisions.

	for {
		start = 0

		_, err := f.fd.Read(buff)
		if err != nil {
			fmt.Println("ERROR: ", err)
			break
		}

		for {
			index := buff[start:start+IndexSize]

			key := Key{}
			key.Set(index[:20])
			key.SetOffset(offset)

			// fmt.Println(key)

			offset += IndexSize
			start  += IndexSize

			if count < int(totalCount) {
				count++
			}

			// Break and start reading collision keys.
			if count == int(f.indexesPerFile) {
				break;
			}

			if start == size {
				break
			}
		}
	}

	fmt.Println(count)
}

func readKeys(count int32) {

}
