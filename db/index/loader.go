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
		data := i.buf[i.offset:i.offset + num]

		i.offset += num
		return data
	}

	// We don't have enough data in buffer, read what's left 
	// and then read next batch.
	rest := i.buf[i.offset:]

	// Read next batch from file.
	n, err := i.Read()
	if n == 0 {
		fmt.Println("err: ", err)
		return rest
	}

	// Read remaining data.
	remaining := num - len(rest)
	return append(rest, i.Next(remaining)...)
}

func (f *File) LoadIndexes(num int) {
	it := NewIterator(f.fd, num)

	stat, _ := f.fd.Stat()
	total   := stat.Size() / IndexSize

	collisions := uint64(total) - f.capacity

	f.Keys			 = make([]Key, f.capacity)
	f.Collisions = make([]Key, collisions)

	// Read keys.
	for i:=0; i < len(f.Keys); i++ {
		data := it.Next(IndexSize)

		key := Key{}
		key.SetHash(HashKey(data[:20]))
		f.Keys[i] = key
	}

	// Read collisions.
	for i:=0; i < len(f.Collisions); i++ {
		data := it.Next(IndexSize)
	
		key := Key{}
		key.SetHash(HashKey(data[:20]))
		f.Collisions[i] = key
	}
}
