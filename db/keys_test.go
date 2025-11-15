package db

// func TestKeysSetGet(t *testing.T) {
// 	index := Dir("./test/index", 10, "bin")
// 	dataDir := Dir("./test", 10, "bin")
// 	defer os.RemoveAll("./test")

// 	kv, _ := OpenKeys(dataDir, index)

// 	for i := 0; i < 10; i++ {
// 		key := fmt.Sprintf("key_%d", i)
// 		val := fmt.Sprintf("val_%d", i)

// 		kv.Set([]byte(key), []byte(val))
// 	}

// 	for i := 0; i < 10; i++ {
// 		k := fmt.Sprintf("key_%d", i)
// 		v := fmt.Sprintf("val_%d", i)

// 		b, _ := kv.Get([]byte(k))
// 		val := [5]byte{}

// 		Decode2(b, val[:])
// 		tests.Assert(t, v, string(val[:]))
// 	}
// }
