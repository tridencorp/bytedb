package index

import (
	"fmt"
	"os"
)

type Iterator struct {
	file 	  	*os.File
	offset 		int 
	buf 		  []byte
	batchSize int
}

func NewIterator(file *os.File, batchSize int) *Iterator {
	it := &Iterator{offset: 0, file: file, batchSize: batchSize}
	return it
}

func (i *Iterator) Read() (int, error) {
	i.buf = make([]byte, i.batchSize)
	i.offset = 0

	return i.file.Read(i.buf)
}

func (i *Iterator) Next(num int) []byte {
	// We have enough data in buffer.
	if i.offset + num <= len(i.buf) {
		data := i.buf[i.offset:i.offset+num]
		i.offset += num
		
		return data
	}

	// We don't have enough data in buffer, read what's left 
	// and then read next batch.
	tmp := i.buf[i.offset:]

	// Read next batch from file.
	n, _ := i.Read()
	if n == 0 {
		return tmp
	}

	// Read remaining data.
	remaining := num - len(tmp)
	return append(tmp, i.Next(remaining)...)
}

func (f *File) LoadIndexes(num int) {
	count := 0
	it := NewIterator(f.fd, num)

	stat, _    := f.fd.Stat()
	totalCount := stat.Size() / IndexSize

	keys 			 := f.indexesPerFile
	collisions := totalCount - int64(keys)

	f.Keys       = make([]Key, keys)
	f.Collisions = make([]Key, collisions)

	// Read keys.
	for i:=uint64(0); i < keys; i++ {
		data := it.Next(IndexSize)

		key := Key{}
		key.Set(data[:20])

		if !key.Empty() {
			count++
			// fmt.Println(key.Name())
		}
	}

	// Read collisions.
	for i:=int64(0); i < collisions; i++ {
		data := it.Next(IndexSize)
		
		key := Key{}
		key.Set(data[:20])

		if !key.Empty() {
			count++
			fmt.Println("XXXXXXX")
		}
	}

	fmt.Println(count)
}
