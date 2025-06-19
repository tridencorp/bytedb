package db

import (
	"bucketdb/tests"
	"fmt"
	"testing"
)

func TestDirectoryGet(t *testing.T) {
	d := OpenDirectory("./test", 3, "idx")

	// Test subdir 1, ex:  root/1/1.idx
	for i := 1; i <= 3; i++ {
		f, _ := d.Get(i)
		tests.Assert(t, fmt.Sprintf("./test/1/%d.idx", i), f.file.Name())
	}

	// Test subdir 2, ex: root/2/4.idx
	for i := 4; i <= 6; i++ {
		f, _ := d.Get(i)
		tests.Assert(t, fmt.Sprintf("./test/2/%d.idx", i), f.file.Name())
	}
}
