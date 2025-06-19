package db

import (
	"bucketdb/tests"
	"fmt"
	"os"
	"testing"
)

func TestDirGet(t *testing.T) {
	d := Dir("./test", 3, "idx")
	defer os.RemoveAll("./test")

	// Test subdir 1, ex: root/1/1.idx
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

func TestDirMax(t *testing.T) {
	d := Dir("./test", 3, "idx")
	defer os.RemoveAll("./test")

	// Make some subdirs and files.
	for i := 1; i <= 13; i++ {
		d.Get(i)
	}

	tests.Assert(t, 13, d.Max())
}
