package collection

type Collection struct {
	name string
}

// Add key-value to collection namespace.
//
// Keys are added to files based on their prefix, so keys
// with the same prefix will end up in the same file.
func (c *Collection) Add(key *Key, val []byte) {
	// Files('coll_blocks')   :
	//
	// Files('coll_indexes')  :

}
