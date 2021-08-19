package deep6

import (
	"os"
	"testing"

	dbset "github.com/digisan/data-block/store/db"
)

func Test_runIngestWithReader(t *testing.T) {
	f, err := os.Open("./mixed.json")
	if err != nil {
		panic(err)
	}

	db, err := dbset.NewBadgerDB("./data/badger")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	runIngestWithReader(f, db, "./")

	// for k, v := range *(kv.KVs[store.IdxM].(*impl.M)) {
	// 	fmt.Println(k, v)
	// }

	// kv.KVs[store.IdxM].(*impl.M).FlushToBadger(db)

	// fmt.Println("----------------------------------------")

	// fdBuf := impl.NewM()

	// fdBuf, _ := db.BadgerSearchByPrefix(fdBuf, db, "spo|82656FA0-17B6-42BF-9915-487360FDF361|", func(v interface{}) bool { return v.(int64) != 0 })
	// for k, v := range fdBuf {
	// 	fmt.Println(k, v)
	// }

	// // real [remove] should only be allowed by cmd,
	// // here invoke only for test
	// if err := db.RemoveToBadger(fdBuf, db); err != nil {
	// 	panic(err)
	// }

	// fmt.Println("----------------------------------------")

	// fdBuf, _ = db.BadgerSearchByPrefix(db, "spo|82656FA0-17B6-42BF-9915-487360FDF361|", func(v interface{}) bool { return v.(int64) != 0 })
	// for k, v := range fdBuf {
	// 	fmt.Println(k, v)
	// }

	// fmt.Println("----------------------------------------")
}
