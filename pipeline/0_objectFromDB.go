package pipeline

import (
	"context"
	"fmt"

	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
	"github.com/cdutwhu/n3-deep6-v2/helper"
	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
	jt "github.com/digisan/json-tool"
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

		mIdVer, err := helper.MapAllID(db)
		if err != nil {
			cErr <- err
			return
		}

		for _, id := range ids {
			prefix := fmt.Sprintf("spo|%s|", id)
			m, err := dbset.BadgerSearchByPrefix(db, prefix, func(v interface{}) bool { return v.(int64) == mIdVer[id] })
			if err != nil {
				cErr <- err
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
	<-chan dd.IngestData,
	<-chan error, // emits errors encountered to the pipeline
	error) { // any error encountered when creating this component

	cOut := make(chan dd.IngestData)
	cErr := make(chan error, 1)

	// cJson, cErr, err := JsonFromDB(ctx, db, ids...)

	// go func() {
	// 	defer close(cOut)
	// 	defer close(cErr)

	// 	mIdVer, err := MapAllID(db)
	// 	if err != nil {
	// 		cErr <- err
	// 		return
	// 	}

	// 	cComJson := make(chan string)

	// 	for _, id := range ids {

	// 		prefix := fmt.Sprintf("spo|%s|", id)
	// 		m, err := dbset.BadgerSearchByPrefix(db, prefix, func(v interface{}) bool { return v.(int64) == mIdVer[id] })
	// 		if err != nil {
	// 			cErr <- err
	// 		}
	// 		if len(m) == 0 { // if id was not found, do not inflate igd
	// 			continue
	// 		}

	// 		m4com := make(map[string]interface{})
	// 		for k := range m {
	// 			t := dd.ParseTriple(k.(string))
	// 			m4com[t.P] = t.O
	// 		}
	// 		cComJson <- jt.Composite(m4com)

	// 		go func() {

	// 			// cClassOut, cErr, err := ObjectClassifier(ctx, folderPath, cComJson) // ------------------------1)
	// 			// if err != nil {
	// 			// 	return errors.Wrap(err, "Error: cannot create object-classifier component: ")
	// 			// }
	// 			// cErrList = append(cErrList, cErr)

	// 		}()

	// 		select {
	// 		case cOut <- igd: // pass the data on to the next stage
	// 		case <-ctx.Done(): // listen for pipeline shutdown
	// 			return
	// 		}
	// 	}
	// }()

	return cOut, cErr, nil
}
