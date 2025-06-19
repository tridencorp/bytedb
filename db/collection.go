package db

import (
	"fmt"
	"path/filepath"
)

type Collection struct {
	name string
	root string

	keys *Keys
}

func OpenCollection(name string, root string) *Collection {
	c := &Collection{name: name, root: root}

	c.keys, _ = OpenKeys(
		Dir(filepath.Join(root, "keys", "data"), 10_000, "bin"),
		Dir(filepath.Join(root, "keys", "index"), 10_000, "bin"),
	)

	return c
}

// Set key.
func (c *Collection) Set(key, val []byte) {
	off, _ := c.keys.Set(key, val)
	fmt.Println(off)
}
