package pipeline

import (
	"context"
	"fmt"

	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
	"github.com/cdutwhu/n3-deep6-v2/helper"
	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
	jt "github.com/digisan/json-tool"
	"github.com/pkg/errors"
)

func JsonFromDB(ctx context.Context, db *badger.DB, ids ...string) (
	<-chan string,
	<-chan error,
	error) {

	cOut := make(chan string)
	cErr := make(chan error, 1)

	go func() {
		defer close(cOut)
		defer close(cErr)

		mIdVer, err := helper.MapAllId(db)
		if err != nil {
			cErr <- err
			return
		}

		for _, id := range ids {
			prefix := fmt.Sprintf("spo|%s|", id)
			m, err := dbset.BadgerSearchByPrefix(db, prefix, func(v interface{}) bool { return v.(int64) == mIdVer[id] })
			if err != nil {
				cErr <- err
				continue
			}
			if len(m) == 0 { // if id was not found, inflate empty string
				cOut <- ""
				continue
			}

			m4com := make(map[string]interface{})
			for k := range m {
				t := dd.ParseTriple(k.(string))
				m4com[t.P] = t.O
			}

			select {
			case cOut <- jt.Composite(m4com): // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return cOut, cErr, nil
}

func IngestDataFromDB(ctx context.Context, db *badger.DB, ids ...string) (
	<-chan *dd.IngestData,
	<-chan error, // emits errors encountered to the pipeline
	error) { // any error encountered when creating this component

	// monitor all error channels
	var cErrList []<-chan error

	cJson, cErr, err := JsonFromDB(ctx, db, ids...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error: cannot load json from database")
	}
	cErrList = append(cErrList, cErr)

	cOut, cErr, err := ObjectClassifier(ctx, cJson)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error: cannot create object-classifier")
	}
	cErrList = append(cErrList, cErr)

	cOut, cErr, err = TupleGenerator(ctx, cOut)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error: cannot create tuple-generator component: ")
	}
	cErrList = append(cErrList, cErr)

	cOut, cErr, err = LinkParser(ctx, nil, cOut)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error: cannot create link-parser component: ")
	}
	cErrList = append(cErrList, cErr)

	return cOut, helper.MergeErrors(cErrList...), nil
}