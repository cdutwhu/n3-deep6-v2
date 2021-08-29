package deep6

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	dbset "github.com/digisan/data-block/store/db"
)

func TestRunIngestWithReader(t *testing.T) {

	AuditStep = 2
	wp.SetWorkPath("./")
	// impl.SetPrint(true)

	sampleDataPaths := []string{
		// "./test_data/naplan/sif.json",
		"./test_data/sif/sif.json",
		"./test_data/xapi/xapi.json",
		"./test_data/subjects/subjects.json",
		"./test_data/lessons/lessons.json",
		"./test_data/curriculum/overview.json",
		"./test_data/curriculum/content.json",
		"./test_data/otf/mapping1.json",
		"./test_data/otf/mapping2.json",
		"./test_data/mixed.json",
	}

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer func() { time.Sleep(10 * time.Millisecond); db.Close() }() // cancel pipeline first, then close database

	// ingest all original files
	for _, datafile := range sampleDataPaths {
		func() {
			f, err := os.Open(datafile)
			if err != nil {
				panic(err)
			}
			defer f.Close()

			// set up a context to manage ingest pipeline
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel() // cancel pipeline first, then close database

			if err := RunIngest(ctx, f, db); err != nil {
				fmt.Println(err)
			}
		}()
	}
}

func TestLinkBuilder(t *testing.T) {
	wp.SetWorkPath("./")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	RunLinkBuilder(db)
}
