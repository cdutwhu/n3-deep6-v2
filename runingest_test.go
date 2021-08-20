package deep6

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cdutwhu/n3-deep6-v2/helper"
	pl "github.com/cdutwhu/n3-deep6-v2/pipeline"
	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	dbset "github.com/digisan/data-block/store/db"
)

// func TestIngestDataFromDB(t *testing.T) {

// 	wp.SetWorkPath("./")

// 	// set up a context to manage ingest pipeline
// 	ctx, cancelFunc := context.WithCancel(context.Background())
// 	defer cancelFunc()

// 	// monitor all error channels
// 	var cErrList []<-chan error

// 	db, err := dbset.NewBadgerDB(wp.DBP())
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer db.Close()

// 	cIGD, cErr, _ := pl.IngestDataFromDB(ctx, db, "4947ED1F-1E94-4850-8B8F-35C653F51E9C")
// 	cErrList = append(cErrList, cErr)

// 	go func() {
// 		for igd := range cIGD {
// 			fmt.Println(jt.Fmt(string(igd.RawBytes), "  "))
// 			igd.Print("")
// 		}
// 	}()

// 	err = pl.WaitForPipeline(cErrList...)
// 	if err != nil {
// 		panic(err)
// 	}

// 	time.Sleep(10 * time.Millisecond)
// }

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
	defer db.Close()

	runIngestWithReader(f, db)

	time.Sleep(10 * time.Millisecond)

	fmt.Println("\n--- object id list: ---")
	mIdVer, err := helper.MapAllID(db)
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
