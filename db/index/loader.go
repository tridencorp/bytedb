package index

import "fmt"

// Number of indexes (bytes) to read in each chunk.
const size = 10_000 * IndexSize

func (f *File) LoadIndexes() {
	buff  := make([]byte, size)
	start  := 0
	offset := uint64(0)
	count  := 0

	stat, _   := f.fd.Stat()
	totalSize := stat.Size() / IndexSize

	f.Keys       = make([]Key, f.indexesPerFile)
	f.Collisions = make([]Key, totalSize - int64(f.indexesPerFile))

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

			fmt.Println(key)

			offset += IndexSize
			start  += IndexSize
			count++

			if start == size {
				break
			}
		}
		
		fmt.Println(count)
	}
}
