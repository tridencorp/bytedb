package index

import (
	"bucketdb/tests"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"testing"
	"time"
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
	f, err := os.Create("cpu_profile.out")
	if err != nil {
			log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()

	// Start CPU profiling
	err = pprof.StartCPUProfile(f)
	if err != nil {
			log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()
		
	num := 1_000_000
	file, _  := Load("index.idx", uint64(num))
	defer os.Remove("./index.idx")

	tests.RunConcurrently(1, func() {
		for i:=0; i < 1_000_000; i++ {
			key := fmt.Sprintf("key_%d_%d", i, time.Now().UnixMicro())
			file.Set([]byte(key), 10, 10, 1)
		}
	})

	fmt.Println("collisions: ", file.nextCollision.Load())
	fmt.Println("collisions: ", len(file.Collisions))
}
