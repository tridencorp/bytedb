package index

import (
	"fmt"
	"os"
)

// Number of indexes (bytes) to read in each chunk.
const size = 10_000 * IndexSize

type Iterator struct {
	file 	  *os.File
	buf 		[]byte
	offset 	int 
}

func NewIterator(file *os.File, batchSize int) *Iterator {
	return &Iterator{file: file, buf: make([]byte, batchSize)}
}

func (it *Iterator) Read() (int, error) {
	n, err := it.file.Read(it.buf)
	if err != nil {
		fmt.Println("ERROR: ", err)
		return n, err
	}

	it.offset = 0
	return n, nil
}

// func (it *Iterator) Next(num int) []byte {
// 	// We don't have enough data, read next batch.
// 	if it.offset + num > len(it.buf) {
// 		// Read what's left.
// 		tmp := it.buf[it.offset:num]
// 		remaining := num - len(tmp)

// 		// Read next batch from file.
// 		it.Read()
// 	}
// }

func (f *File) LoadIndexes() {
	// buff   := make([]byte, size)
	// start  := 0
	// offset := uint64(0)
	count  := 0

	size := 1024*1024*1 // 10 MB
	it := NewIterator(f.fd, size)

	it.Read()
	fmt.Println(it.buf)

	stat, _    := f.fd.Stat()
	totalCount := stat.Size() / IndexSize

	collisionCount := totalCount - int64(f.indexesPerFile)

	f.Keys       = make([]Key, f.indexesPerFile)
	f.Collisions = make([]Key, collisionCount)

	// Read keys.
	// limit := f.indexesPerFile
	// for i:=uint64(0); i < limit; i++ {
	// 	key := it.Next(IndexSize)
	// }

	// for {
	// 	start = 0

	// 	_, err := f.fd.Read(buff)
	// 	if err != nil {
	// 		fmt.Println("ERROR: ", err)
	// 		break
	// 	}

	// 	for {
	// 		index := buff[start:start+IndexSize]

	// 		key := Key{}
	// 		key.Set(index[:20])
	// 		key.SetOffset(offset)

	// 		// fmt.Println(key)

	// 		offset += IndexSize
	// 		start  += IndexSize

	// 		if count < int(totalCount) {
	// 			count++
	// 		}

	// 		// Break and start reading collision keys.
	// 		if count == int(f.indexesPerFile) {
	// 			break;
	// 		}

	// 		if start == size {
	// 			break
	// 		}
	// 	}
	// }

	fmt.Println(count)
}
