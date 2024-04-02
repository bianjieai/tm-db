package db

//var pdAddr = []string{"127.0.0.1:2379"}

//func TestTikvDBNewTikvDB(t *testing.T) {
//	name := "application"
//	dir := fmt.Sprintf("testdir%x", randStr(12))
//
//	// Test we can't open the db twice for writing
//	wr1, err := NewTikvDBWithOpts(name, dir, pdAddr)
//	require.Nil(t, err)
//	_, err = NewTikvDBWithOpts(name, dir, pdAddr)
//	require.NotNil(t, err)
//	wr1.Close() // Close the db to release the lock
//}

//func BenchmarkTikvDBRandomReadsWrites(b *testing.B) {
//	name := fmt.Sprintf("testname%x", randStr(12))
//	dir := fmt.Sprintf("testdir%x", randStr(12))
//
//	db, err := NewTikvDBWithOpts(name, dir, pdAddr)
//	if err != nil {
//		b.Fatal(err)
//	}
//	defer db.Close()
//
//	benchmarkRandomReadsWrites(b, db)
//}

//// 迭代器测试
//func TestTikvIter(t *testing.T) {
//	name := fmt.Sprintf("testname%x", randStr(12))
//	dir := fmt.Sprintf("testdir%x", randStr(12))
//
//	db, err := NewTikvDBWithOpts(name, dir, pdAddr)
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer db.Close()
//
//	num := int64(10)
//	var keys [][]byte
//
//	for i := int64(0); i < num; i++ {
//		bytes := int642Bytes(i)
//		err := db.Set(bytes, bytes)
//		if err != nil {
//			// require.NoError() is very expensive (according to profiler), so check manually
//			t.Fatal(t, err)
//		}
//		keys = append(keys, bytes)
//	}
//
//	for _, key := range keys {
//		get, err := db.Get(key)
//		require.NoError(t, err)
//		fmt.Printf("[%X]:\t[%X]\n", key, get)
//	}
//
//	iter, err := db.Iterator(int642Bytes(2), int642Bytes(6))
//	require.NoError(t, err)
//	count := 0
//	for ; iter.Valid(); iter.Next() {
//		fmt.Printf("key: %v, data: %v\n", string(iter.Key()), iter.Value())
//		count++
//	}
//	iter.Close()
//	//require.EqualValues(t, num, count)
//}
//
//// 迭代器批量数据自动测试
//func BenchmarkTikvDBRangeScans1M(b *testing.B) {
//	name := fmt.Sprintf("testname%x", randStr(12))
//	dir := fmt.Sprintf("testdir%x", randStr(12))
//
//	db, err := NewTikvDBWithOpts(name, dir, pdAddr)
//	if err != nil {
//		b.Fatal(err)
//	}
//	defer db.Close()
//
//	benchmarkRangeScans(b, db, int64(10001))
//}

//// 反迭代器测试
//func TestTikvReverseIter(t *testing.T) {
//	name := fmt.Sprintf("testname%x", randStr(12))
//	dir := fmt.Sprintf("testdir%x", randStr(12))
//
//	db, err := NewTikvDBWithOpts(name, dir, pdAddr)
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer db.Close()
//
//	num := int64(10)
//	var keys [][]byte
//
//	for i := int64(0); i < num; i++ {
//		bytes := int642Bytes(i)
//		err := db.Set(bytes, bytes)
//		if err != nil {
//			// require.NoError() is very expensive (according to profiler), so check manually
//			t.Fatal(t, err)
//		}
//		keys = append(keys, bytes)
//	}
//
//	for _, key := range keys {
//		get, err := db.Get(key)
//		require.NoError(t, err)
//		fmt.Printf("[%X]:\t[%X]\n", key, get)
//	}
//
//	iter, err := db.ReverseIterator(int642Bytes(2), int642Bytes(6))
//	require.NoError(t, err)
//	count := 0
//	for ; iter.Valid(); iter.Next() {
//		fmt.Printf("key: %v, data: %v\n", string(iter.Key()), iter.Value())
//		count++
//	}
//	iter.Close()
//	//require.EqualValues(t, num, count)
//}

//func TestTikvIterScan(t *testing.T) {
//	txnClient, err := txnkv.NewClient(pdAddr)
//	require.NoError(t, err)
//
//	txn, err := txnClient.Begin()
//	require.NoError(t, err)
//	defer func() {
//		err2 := txn.Commit(context.Background())
//		require.NoError(t, err2)
//	}()
//
//	iter, err := txn.Iter(nil, nil)
//	require.NoError(t, err)
//
//	fmt.Println("start")
//	count := 0
//	for ; iter.Valid(); iter.Next() {
//		fmt.Printf("key: %v, data: %v\n", string(iter.Key()), iter.Value())
//		count++
//	}
//	iter.Close()
//	fmt.Printf("count: %v\n", count)
//	fmt.Println("end")
//}
