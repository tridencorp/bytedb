package db

import (
	"bucketdb/db/index"
	"os"
	"path/filepath"
	"sync/atomic"
)

type Collection struct {
	bucket  *Bucket
	buckets *Buckets

	indexes *index.File
	config  Config

	// Collection root directory.
	root string

	// We will be using atomic.Swap() for each key.
	// In combination with WriteAt, it should give
	// us the ultimate concurrent writes.
	offset atomic.Int64
}

type Config struct {
	KeysLimit     uint32
	SizeLimit     int64
	BucketsPerDir int32
}

// Open the collection. If it doesn't exist,
// create it with default values.
func (db *DB) Collection(name string, conf Config) (*Collection, error) {
	// Build collection path.
	path := db.root + CollectionsPath + name
	return newCollection(path, conf)
}

func newCollection(path string, conf Config) (*Collection, error) {
	// Build collection path.
	dir := filepath.Dir(path)

	// Create directory structure. Do nothing if it already exist.
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	// Open buckets.	
	buckets, err := OpenBuckets(path, 100, conf)
	if err != nil {
		return nil, err
	}

	indexes, err := index.Load(dir, 100_000)
	if err != nil {
		return nil, err
	}

	coll := &Collection {
		buckets:  buckets, 
		root:    path,
		indexes: indexes,
		config:  conf,
	}

	// TODO: because of file truncation we should track current 
	// data size and set our initial offset based on it.
	coll.offset.Store(0)

	return coll, nil
}

// Open or create new hash.
func (coll *Collection) Hash(name string) (*Hash, error) {
	root := coll.root + "/hashes/"
	keys, err := newCollection(root, coll.config)
	if err != nil {
		return nil, err
	}

	return &Hash{root: root, keys: keys}, nil
}

// Store key in collection.
func (c *Collection) Set(key string, val []byte) (int64, int64, error) {
	data, err := NewKV(key, val).Bytes()

	bucket := c.buckets.Latest()
	off, size, id, err := bucket.Write(data)

	// bucket := c.buckets.Get(id)
	// c.buckets.Puy(bucket)

	// Index new key.
	err = c.indexes.Set([]byte(key), len(data), uint64(off), id)
	if err != nil {
		return 0, 0, err
	}

	return off, size, err
}

// Get key from collection.
func (coll *Collection) Get(key string) ([]byte, error) {
	idx, err := coll.indexes.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	// TODO: Based on index we need to pick proper bucket.
	raw, err := coll.bucket.Read(int64(idx.Offset), int64(idx.Size))
	if err != nil {
		return nil, err
	}

	kv := new(KV)
	kv.FromBytes(raw)
	return kv.val, err
}

// Delete key from collection.
// TODO: Set Deleted flag for Key.
func (coll *Collection) Del(key string) error {
	return coll.indexes.Del([]byte(key))	
}

// Update key from collection.
func (coll *Collection) Update(key string, val []byte) error {
	_, _, err := coll.Set(key, val)
	if err != nil {
		return err
	}

	return nil
}
