package deep6

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cdutwhu/n3-deep6-v2/helper"
	pl "github.com/cdutwhu/n3-deep6-v2/pipeline"
	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	dbset "github.com/digisan/data-block/store/db"
)

func Test_RunIngestWithReader(t *testing.T) {

	AuditStep = 2
	wp.SetWorkPath("./")
	// impl.SetPrint(true)

	for i := 0; i < 100; i++ {
		func() {
			f, err := os.Open("./mixed.json")
			if err != nil {
				panic(err)
			}
			defer f.Close()

			db, err := dbset.NewBadgerDB(wp.DBP())
			if err != nil {
				panic(err)
			}
			defer func() { time.Sleep(10 * time.Millisecond); db.Close() }() // cancel pipeline first, then close database

			// set up a context to manage ingest pipeline
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel() // cancel pipeline first, then close database

			if err := RunIngest(ctx, f, db); err != nil {
				fmt.Println(err)
			}
		}()
	}
}

func TestMapAllId(t *testing.T) {

	wp.SetWorkPath("./")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("\n------------------------ object id list:         ------------------------")

	mIdVer, err := helper.MapAllId(db)
	if err != nil {
		panic(err)
	}
	for id, ver := range mIdVer {
		fmt.Println(id, "@", ver)
	}

	fmt.Println("\n------------------------ Update Link Candidates: ------------------------")
	pl.LinkBuilder(db)
}

func TestLinkScan(t *testing.T) {

	wp.SetWorkPath("./")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	prefix := fmt.Sprintf("l-spo|")
	fdBuf, _ := dbset.BadgerSearchByPrefix(db, prefix, helper.FnVerActive)
	for k, v := range fdBuf {
		fmt.Println(k, v)
	}
}
