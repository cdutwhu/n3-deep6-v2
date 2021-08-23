package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/cdutwhu/n3-deep6-v2/helper"
	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	dbset "github.com/digisan/data-block/store/db"
	jt "github.com/digisan/json-tool"
)

func TestJsonFromDB(t *testing.T) {
	wp.SetWorkPath("../")

	// set up a context to manage ingest pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// monitor all error channels
	var cErrList []<-chan error

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cJson, cErr, err := JsonFromDB(ctx, db,
		"fake",
		"4947ED1F-1E94-4850-8B8F-35C653F51E9C",
		"0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9",
		"fake",
		"fake",
	)
	if err != nil {
		panic(err)
	}
	cErrList = append(cErrList, cErr)

	go func() {
		for js := range cJson {
			fmt.Println(jt.TryFmtStr(js, "\t"))
			fmt.Println("-------------------------------------------------------------------------------------------------")
		}
	}()

	// monitor progress
	helper.WaitForPipeline(cErrList...)
}

func TestIngestDataFromDB(t *testing.T) {
	wp.SetWorkPath("../")

	// set up a context to manage ingest pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// monitor all error channels
	var cErrList []<-chan error

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cIgd, cErr, err := IngestDataFromDB(ctx, db,
		"fake",
		"4947ED1F-1E94-4850-8B8F-35C653F51E9C",
		"0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9",
	)
	if err != nil {
		panic(err)
	}
	cErrList = append(cErrList, cErr)

	go func() {
		for igd := range cIgd {
			if igd != nil {
				igd.Print("")
				// fmt.Println(igd)
			} else {
				fmt.Println("\nnil igd")
			}
		}
		fmt.Println("\n ----------------------------------------------------------------- ")
	}()

	// monitor progress
	helper.WaitForPipeline(cErrList...)
}