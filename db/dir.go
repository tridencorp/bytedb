package db

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Manage files and subdirectories.
type Directory struct {
	Root   string
	Ext    string
	PerDir int

	// Get last file (with highest id) from directory.
	// In most cases this will be the file we are currently writing to.
	Last *File
}

func Dir(root string, perDir int, extension string) *Directory {
	d := &Directory{Root: root, PerDir: perDir, Ext: extension}
	id := d.Max()

	// Dir is empty.
	if id == 0 {
		id = 1
	}

	f, err := d.Get(id)
	if err != nil {
		return nil
	}

	d.Last = f
	return d
}

// Get file from directory. Create it if it doesn't already exist.
func (d *Directory) Get(id int) (*File, error) {
	// Get subdir based on id using ceil technique.
	subdir := (d.PerDir + id - 1) / d.PerDir

	// Build path for given id:
	// 	* root/subdir/id.ext
	path := fmt.Sprintf("%s/%d/%d.%s", d.Root, subdir, id, d.Ext)

	// Open file id.
	f, err := OpenPath(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}

	if d.Last == nil || (d.Last != nil && d.Last.ID < id) {
		d.Last = f
	}

	f.ID = id
	return f, nil
}

// Search in subdirectories and find max file id.
func (d *Directory) Max() int {
	max := 0

	// Find last subdir.
	subdirs, _ := os.ReadDir(d.Root)

	for _, subdir := range subdirs {
		id, _ := strconv.Atoi(subdir.Name())
		if id > max {
			max = id
		}
	}

	// Dir is empty, no subdirs.
	if max == 0 {
		return max
	}

	// Find last file id.
	path := fmt.Sprintf("%s/%d/", d.Root, max)
	files, _ := os.ReadDir(path)

	max = 0
	for _, f := range files {
		// Split file name, don't care about extension.
		id, err := strconv.Atoi(strings.Split(f.Name(), ".")[0])
		if err != nil {
			return max
		}

		if id > max {
			max = id
		}
	}

	return max
}
