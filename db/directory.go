package db

import (
	"fmt"
	"os"
)

// Manage files and subdirectories.s
type Directory struct {
	Root   string
	Ext    string
	PerDir int
}

func OpenDirectory(root string, perDir int, extension string) *Directory {
	return &Directory{Root: root, PerDir: perDir, Ext: extension}
}

// Get file from directory. Create it if it doesn't already exist.
func (d *Directory) Get(id int) (*File, error) {
	// Get subdir based on id using ceil technique.
	subdir := (d.PerDir + id - 1) / d.PerDir

	// Build path for given id:
	// 	* root/subdir/id.ext
	path := fmt.Sprintf("%s/%d/%d.%s", d.Root, subdir, id, d.Ext)

	return OpenPath(path, os.O_RDWR|os.O_CREATE)
}
