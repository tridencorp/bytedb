package db

// Bucket file
type Bucket struct {
	*File
	index *Index
}

func (b *Bucket) Write(key *Key) (int, error) {
	return 0, nil
}
