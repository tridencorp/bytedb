package db

// TODO: Move this to test setup
// var num = flag.Int64("num", 100_000, "number of iterations")

// func TestIndexPrealloc(t *testing.T) {
// 	flag.Parse()

// 	i, _ := OpenIndex(Dir("./test", 10, "bin"), *num)
// 	defer os.RemoveAll("./test")

// 	prealloc := int64(2800000) // keys + collisions
// 	tests.AssertEqual(t, prealloc, i.files.Last.Size())
// }

// func TestIndexSetGet(t *testing.T) {
// 	flag.Parse()

// 	idx, _ := OpenIndex(Dir("./test", 10, "bin"), *num)
// 	defer os.RemoveAll("./test")

// 	for i := 0; i < int(*num); i++ {
// 		key := fmt.Sprintf("key_%d", i)
// 		off := &Offset{Start: uint32(i), Size: 10}

// 		err := idx.Set([]byte(key), off)
// 		tests.Assert(t, nil, err)
// 	}

// 	for i := 0; i < int(*num); i++ {
// 		key := fmt.Sprintf("key_%d", i)
// 		off, _ := idx.Get([]byte(key))

// 		tests.Assert(t, i, int(off.Start))
// 	}
// }
