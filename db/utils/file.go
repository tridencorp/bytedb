package utils

import (
	"os"
	"path/filepath"
)

// Open file within given path. It will create the path
// if it doesn't exist.
func OpenPath(path string) (*os.File, error) {
	dir := filepath.Dir(path)

	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	
	return file, nil
}

// Sort given directory and return max file/dir.
func MaxEntry(path string, fn func(i, j os.DirEntry) bool) os.DirEntry {
	entries, _ := os.ReadDir(path)
	if len(entries) == 0 {
		return nil
	}

	max := entries[0]

	for i:=0; i < len(entries); i++ {
		j := i + 1
		if j >= len(entries) { j = i }

		if fn(entries[i], entries[j]) {
			max = entries[j]
		}
	}

	return max
}
