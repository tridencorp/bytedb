package index

import (
	"fmt"
	"os"
	"testing"
)

func TestIndexSet(t *testing.T) {
	num := 500_000
	file, _ := Load("./index.idx", uint64(num))
	defer os.Remove("./index.idx")

	for i:=0; i < num; i++ {
		key := fmt.Sprintf("key_%d", i)
		file.Set([]byte(key), 10, 10, 1)	
	}

	for i:=0; i < num; i++ {
		key  := fmt.Sprintf("key_%d", i)
		i, _ := file.Get([]byte(key))

		want := HashKey([]byte(key))
		got  := i.Hash
		
		if want != got { 
			t.Errorf("Expected %d, got %d", want, got) 
		}
	}
}

func TestLoader(t *testing.T) {
	num := 100_000
	file, _ := Load("./index.idx", uint64(num))
	defer os.Remove("./index.idx")

	for i:=0; i < num; i++ {
		key := fmt.Sprintf("key_%d", i)
		file.Set([]byte(key), 10, 10, 1)	
	}

	file.Keys       = make([]Key, 0)
	file.Collisions = make([]Key, 0)

	oneMB := 1024*1024*1
	file.LoadIndexes(oneMB)

	for i:=0; i < num; i++ {
		key := fmt.Sprintf("key_%d", i)
		i, _ := file.Get([]byte(key))

		want := HashKey([]byte(key))
		got  := i.Hash

		if want != got { 
			t.Errorf("Expected %s: %d, got %d", key, want, got) 
		}
	}
}

func TestWrites(t *testing.T) {
	// num := 200_000
	// file, _  := Load("index.idx", uint64(num))
	// file2, _ := Load("index1.idx", uint64(num))
	// file3, _ := Load("index2.idx", uint64(num))

	// _ = []*File{file, file2, file3}

	// defer os.Remove("./index.idx")
	// defer os.Remove("./index1.idx")
	// defer os.Remove("./index2.idx")

	// for i:=0; i < 1_000_000; i++ {
	// 	key := fmt.Sprintf("key_%d", i)
	// 	file.Set([]byte(key), 10, 10, 1)
	// }

	// filename := "./mmap_test.bin"
	// size := int64(1024 * 1024 * 50)

	// // Open or create file
	// file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer file.Close()

	// Resize file to required size
	// if err := syscall.Ftruncate(int(file.Fd()), size); err != nil {
	// 	log.Fatal(err)
	// }

	// Memory-map the file
	// data, err := unix.Mmap(int(file.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// Write to memory-mapped filealloc_space

	// Flush changes to disk

	// file1, _ := os.OpenFile("./index.idx", os.O_WRONLY | os.O_CREATE, 0644)                                                 
	// offset := int64(0) 

	// _, err := file.WriteAt(data, offset)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	for i:=0; i < 1_000_000; i++ {
		// total += copy(data[offset:], fmt.Sprintf("%d\n", i))

		// _, err := file1.Write(buf)
		// if err != nil {
		// 	fmt.Println(err)
		// }
		// offset += 8
	}

	// if err := unix.Msync(data, unix.MS_ASYNC); err != nil {
	// 	log.Fatal(err)
	// }

	// tests.RunConcurrently(2, func(){
	// 	for i:=0; i < 500_000; i++ {
	// 		key := fmt.Sprintf("key_%d", i)
	// 		num := rand.Intn(2)
	// 		files[num].Set([]byte(key), 10, 10, 1)
	// 	}
	// })
}