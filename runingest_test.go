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
	f, e := os.Open("./mixed.json")
	if e != nil {
		panic(e)
	}

	badgerDB, err := db.NewBadgerDB("./data/badger")
	if err != nil {
		panic(err)
	}
	defer badgerDB.Close()

	kv := store.NewKV(true, true)
	runIngestWithReader(f, kv, "./")
	for k, v := range *(kv.KVs[store.IdxM].(*impl.M)) {
		fmt.Sprintln(k, v)
	}

	kv.KVs[store.IdxM].(*impl.M).FlushToBadger(badgerDB)
}
