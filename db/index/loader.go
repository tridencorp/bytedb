package index

import (
	"fmt"
	"os"
)

// Number of indexes (bytes) to read in each chunk.
const size = 10_000 * IndexSize

type Iterator struct {
	file 	  *os.File
	offset 	int 

	buf 		  []byte
	batchSize int
}

func NewIterator(file *os.File, batchSize int) *Iterator {
	it := &Iterator{file: file, batchSize: batchSize}
	it.Read()

	return it
}

func (it *Iterator) Read() (int, error) {
	it.buf = make([]byte, it.batchSize)

	n, err := it.file.Read(it.buf)
	if err != nil {
		return n, err
	}

	it.offset = 0
	return n, nil
}

func (it *Iterator) Next(num int) []byte {
	// We have enough data in buffer.
	if it.offset + num <= len(it.buf) {
		off := it.offset
		it.offset += num
		return it.buf[off:it.offset]
	}

	// We don't have enough data in buffer, read what's left 
	// and then read next batch.
	tmp := it.buf[it.offset:]

	// Read next batch from file.
	n, _ := it.Read()
	if n == 0 {
		return tmp
	}

	// Read remaining data from fresh buffer.
	remaining := num - len(tmp)
	tmp = append(tmp, it.Next(remaining)...)

	it.offset += remaining
	return tmp
}

func (f *File) LoadIndexes() {
	// buff   := make([]byte, size)
	// start  := 0
	// offset := uint64(0)
	count  := 0

	size := 1024*1024*1 // 10 MB
	it := NewIterator(f.fd, size)

	stat, _    := f.fd.Stat()
	totalCount := stat.Size() / IndexSize

	collisionCount := totalCount - int64(f.indexesPerFile)

	f.Keys       = make([]Key, f.indexesPerFile)
	f.Collisions = make([]Key, collisionCount)

	// Read keys.
	limit := f.indexesPerFile
	for i:=uint64(0); i < limit; i++ {
		key := it.Next(IndexSize)
		fmt.Println(key)
	}

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
