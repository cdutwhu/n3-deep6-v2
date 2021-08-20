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

	cJson, cErr, _ := JsonFromDB(ctx, db, "4947ED1F-1E94-4850-8B8F-35C653F51E9C", "def", "0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9", "abc")
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
