package db

func CreateCollection(name string, keysLimit uint32, sizeLimit int64, bucketsPerDir int32) (*DB, *Collection) {
	database, _ := Open("./testdb")

	conf := Config{KeysLimit: 2, SizeLimit: 10, BucketsPerDir: 2}
	coll, _:= database.Collection("test", conf)

	return database, coll
}