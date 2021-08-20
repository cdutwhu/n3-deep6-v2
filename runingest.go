package deep6

import (
	"context"
	"io"

	"github.com/cdutwhu/n3-deep6-v2/helper"
	pl "github.com/cdutwhu/n3-deep6-v2/pipeline"
	"github.com/dgraph-io/badger/v3"
	jt "github.com/digisan/json-tool"
	"github.com/pkg/errors"
)

//
// Runs the ingest pipeline to consume json data.
//
// Needs db and wb as these are presumed to be in use
// by other pipelines or application features.
//
// eg. db/wb are used here for loading data
// but db can also be in use to support queries in parallel.
//
// db - instance of a badger db
// wb - badger.WriteBatch, a fast write manager provided by the db
// sbf - bloom filter used to capture required graph links as data traverses the pipeline
// r - the io.Reader (file, http body etc.) to be ingested
// auditLevel - one of: none, basic, high
//
func runIngestWithReader(r io.Reader, db *badger.DB) error {

	// set up a context to manage ingest pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// monitor all error channels
	var cErrList []<-chan error

	//
	// build the pipeline by connecting all stages
	//

	cJsonOut, cErr, err := jt.ScanObjectInArray(ctx, r, true)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create json-reader source component: ")
	}
	cErrList = append(cErrList, cErr)

	cClassOut, cErr, err := pl.ObjectClassifier(ctx, cJsonOut) // ------------------------1)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create object-classifier component: ")
	}
	cErrList = append(cErrList, cErr)

	// remObjOut, errc, err := objectRemover(ctx, db, wb, sbf, auditLevel, folderPath, classOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create object-remover component: ")
	// }
	// errcList = append(errcList, errc)

	cTripleOut, cErr, err := pl.TupleGenerator(ctx, cClassOut) // ------------------------------------2)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create tuple-generator component: ")
	}
	cErrList = append(cErrList, cErr)

	cWriterOut, cErr, err := pl.TripleWriter(ctx, db, cTripleOut) // ---------------------------------3)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create triple-writer component: ")
	}
	cErrList = append(cErrList, cErr)

	cLinkerOut, cErr, err := pl.LinkParser(ctx, db, cWriterOut) // ---------------------------------------4)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create link-parser component: ")
	}
	cErrList = append(cErrList, cErr)

	// cReverselinkerOut, cErr, err := pl.LinkReverseChecker(ctx, db, cLinkerOut) // ------------------- 5)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create reverse-link-checker component: ")
	// }
	// cErrList = append(cErrList, cErr)

	// cBuilderOut, cErr, err := pl.LinkBuilder(ctx, db, cReverselinkerOut) // --------------------------6)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create link-builder component: ")
	// }
	// cErrList = append(cErrList, cErr)

	go func() {
		I := 1
		for c := range cLinkerOut {
			c.Print(I)
			I++
		}
	}()

	// lwriterOut, errc, err := linkWriter(ctx, wb, builderOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create link-writer component: ")
	// }
	// errcList = append(errcList, errc)

	// errc, err = ingestAuditSink(ctx, auditLevel, lwriterOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create audit-sink component: ")
	// }
	// errcList = append(errcList, errc)

	// monitor progress
	return helper.WaitForPipeline(cErrList...)
}
