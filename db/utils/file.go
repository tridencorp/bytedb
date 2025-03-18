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
