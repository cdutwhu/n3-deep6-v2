package deep6

import (
	"fmt"
	"os"
	"testing"

	"github.com/digisan/data-block/store"
	"github.com/digisan/data-block/store/db"
	"github.com/digisan/data-block/store/impl"
)

func Test_runIngestWithReader(t *testing.T) {
	f, err := os.Open("./mixed.json")
	if err != nil {
		panic(err)
	}

	badgerDB, err := db.NewBadgerDB("./data/badger")
	if err != nil {
		panic(err)
	}
	defer badgerDB.Close()

	kv := store.NewKV(true, true)

	runIngestWithReader(f, kv, badgerDB, "./")
	// for k, v := range *(kv.KVs[store.IdxM].(*impl.M)) {
	// 	fmt.Println(k, v)
	// }

	kv.KVs[store.IdxM].(*impl.M).FlushToBadger(badgerDB)

	fmt.Println("----------------------------------------")

	fdBuf := impl.NewM()

	db.SyncFromBadgerByPrefix(fdBuf, badgerDB, "spo|82656FA0-17B6-42BF-9915-487360FDF361|", func(v interface{}) bool { return v.(int64) != 0 })
	for k, v := range *fdBuf {
		fmt.Println(k, v)
	}

	// real [remove] should only be allowed by cmd,
	// here invoke only for test
	if err := db.RemoveToBadger(fdBuf, badgerDB); err != nil {
		panic(err)
	}

	fmt.Println("----------------------------------------")

	db.SyncFromBadgerByPrefix(fdBuf, badgerDB, "spo|82656FA0-17B6-42BF-9915-487360FDF361|", func(v interface{}) bool { return v.(int64) != 0 })
	for k, v := range *fdBuf {
		fmt.Println(k, v)
	}

	fmt.Println("----------------------------------------")

}
