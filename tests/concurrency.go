package tests

import (
	"sync"
)

func RunConcurrently(num int, fn func()) {
	var wg sync.WaitGroup

	for i:=0; i < num; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}

	wg.Wait()
}
