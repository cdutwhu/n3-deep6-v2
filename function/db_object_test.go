package function

import (
	"context"
	"fmt"
	"testing"

	"github.com/cdutwhu/n3-deep6-v2/dbset"
	"github.com/cdutwhu/n3-deep6-v2/helper"
	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	"github.com/dgraph-io/badger/v3"
	jt "github.com/digisan/json-tool"
)

func TestJsonFromDB(t *testing.T) {
	wp.SetWorkPath("../")

	// monitor all error channels
	var cErrList []<-chan error

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// set up a context to manage ingest pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

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

	// monitor all error channels
	var cErrList []<-chan error

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// set up a context to manage ingest pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

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
				igd.Print("", "RawBytes1", "RawData1", "Triples1", "LinkCandidates1")
			} else {
				fmt.Println("\nnil igd")
			}
		}
		fmt.Println("\n ----------------------------------------------------------------- ")
	}()

	// monitor progress
	helper.WaitForPipeline(cErrList...)
}

func TestGetIDbyX(t *testing.T) {
	wp.SetWorkPath("../")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	type args struct {
		arg   string
		db    *badger.DB
		types []string
	}
	tests := []struct {
		name string
		args args
		want map[string][]string
	}{
		// TODO: Add test cases.
		{
			name: "GetIDsByType",
			args: args{
				arg: "Type",
				db:  db,
				types: []string{
					"XAPI",
					"StudentPersonal",
					"GradingAssignment",
					"TeachingGroup",
					"Syllabus",
					"JSON",
				},
			},
		},
		{
			name: "GetIDsByValue",
			args: args{
				arg: "Value",
				db:  db,
				types: []string{
					"marjorie45@trashymail.com",
					"LGL",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := GetIDbyX(tt.args.arg, tt.args.db, tt.args.types...)
			for k, v := range m {
				fmt.Println(k)
				for _, id := range v {
					fmt.Printf("  %v\n", id)
				}
			}
			fmt.Println("-------------------------------------------------------")
		})
	}
}
