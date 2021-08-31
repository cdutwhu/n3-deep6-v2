package function

import (
	"context"
	"fmt"

	. "github.com/cdutwhu/n3-deep6-v2/basic"
	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
	"github.com/cdutwhu/n3-deep6-v2/dbset"
	"github.com/cdutwhu/n3-deep6-v2/helper"
	pl "github.com/cdutwhu/n3-deep6-v2/pipeline"
	"github.com/dgraph-io/badger/v3"
	jt "github.com/digisan/json-tool"
	"github.com/pkg/errors"
)

func JsonFromDB(ctx context.Context, db *badger.DB, ids ...string) (
	<-chan string,
	<-chan error,
	error) {

	cOut := make(chan string)
	cErr := make(chan error, 1)
	var err error

	go func() {
		defer close(cOut)
		defer close(cErr)

		mIdVer, e := MapAllId(db, false)
		if err != nil {
			err = e
			return
		}

		for _, id := range ids {

			ver, _ := mIdVer.Get(id)
			prefix := fmt.Sprintf("spo|%s|", id)
			m, err := dbset.BadgerSearchByPrefix(db, prefix, func(k string, v int64) bool {
				return v == ver
			})
			if err != nil {
				cErr <- err
				continue
			}
			if len(m) == 0 { // if tuple not found, inflate empty string
				cOut <- ""
				continue
			}

			m4com := make(map[string]interface{})
			for k := range m {
				t := dd.ParseTripleData(k)
				m4com[t.P] = t.O
			}

			select {
			case cOut <- jt.Composite(m4com): // pass data to next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return cOut, cErr, err
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

	cOut, cErr, err := pl.ObjectClassifier(ctx, cJson)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error: cannot create object-classifier")
	}
	cErrList = append(cErrList, cErr)

	cOut, cErr, err = pl.TupleGenerator(ctx, cOut)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error: cannot create tuple-generator component: ")
	}
	cErrList = append(cErrList, cErr)

	cOut, cErr, err = pl.LinkCandidateWriter(ctx, db, nil, nil, cOut)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error: cannot create link-parser component: ")
	}
	cErrList = append(cErrList, cErr)

	return cOut, helper.MergeErrors(cErrList...), nil
}
