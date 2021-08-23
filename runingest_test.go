package deep6

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cdutwhu/n3-deep6-v2/helper"
	pl "github.com/cdutwhu/n3-deep6-v2/pipeline"
	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	dbset "github.com/digisan/data-block/store/db"
)

func Test_runIngestWithReader(t *testing.T) {

	wp.SetWorkPath("./")

	// impl.SetPrint(true)

	f, err := os.Open("./mixed.json")
	if err != nil {
		panic(err)
	}

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close() // cancel pipeline first, then close database

	// set up a context to manage ingest pipeline
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancel pipeline first, then close database

	RunIngestWithReader(ctx, f, db)

	cOut, cErr := RunIngestWithReader(ctx, f, db)
	go func() {
		I := 1
		for igd := range cOut {
			if igd != nil {
				igd.Print(I, "Triples", "RawData", "LinkCandidates", "RawBytes")
				I++
			}
		}
	}()
	if err := <-cErr; err != nil {
		panic(err)
	}

	fmt.Println("\n--- object id list: ---")
	mIdVer, err := helper.MapAllId(db)
	if err != nil {
		panic(err)
	}
	for id, ver := range mIdVer {
		fmt.Println(id, "@", ver)
	}

	fmt.Println("\n--- Update Link Candidates: ---")
	pl.UpdateLinkCandidates(db)

	fmt.Println()

	// for k, v := range *(kv.KVs[store.IdxM].(*impl.M)) {
	// 	fmt.Println(k, v)
	// }

	// kv.KVs[store.IdxM].(*impl.M).FlushToBadger(db)

	// fmt.Println("----------------------------------------")

	// fdBuf := impl.NewM()

	// fdBuf, _ := db.BadgerSearchByPrefix(fdBuf, db, "spo|82656FA0-17B6-42BF-9915-487360FDF361|", fVerValid)
	// for k, v := range fdBuf {
	// 	fmt.Println(k, v)
	// }

	// // real [remove] should only be allowed by cmd,
	// // here invoke only for test
	// if err := db.RemoveToBadger(fdBuf, db); err != nil {
	// 	panic(err)
	// }

	// fmt.Println("----------------------------------------")

	// fdBuf, _ = db.BadgerSearchByPrefix(db, "spo|82656FA0-17B6-42BF-9915-487360FDF361|", fVerValid)
	// for k, v := range fdBuf {
	// 	fmt.Println(k, v)
	// }

	// fmt.Println("----------------------------------------")
}
