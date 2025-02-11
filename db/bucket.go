package db

import "os"

type Bucket struct {
	ID 		uint32
	Dir   string
	file *os.File
}

func OpenBucket(file string) (*Bucket, error) {
	// Open bucket file.
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// TODO: Temporary values untill we have proper bucket management.
	bck := &Bucket{ID:1, Dir: "", file: f}
	return bck, nil;
}