package deep6

import (
	"context"
	"io"

	"github.com/cdutwhu/n3-deep6-v2/datadef"
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
func RunIngestWithReader(ctx context.Context, r io.Reader, db *badger.DB) (cOut <-chan *datadef.IngestData, cErr <-chan error) {

	ce := make(chan error)

	// monitor all error channels
	var cErrList []<-chan error

	//
	// build the pipeline by connecting all stages
	//
	cJsonOut, cErr, err := jt.ScanObjectInArray(ctx, r, true)
	if err != nil {
		ce <- errors.Wrap(err, "Error: cannot create json-reader source component: ")
		return nil, ce
	}
	cErrList = append(cErrList, cErr)

	cOut, cErr, err = pl.ObjectClassifier(ctx, cJsonOut) // ------------------------1)
	if err != nil {
		ce <- errors.Wrap(err, "Error: cannot create object-classifier component: ")
		return nil, ce
	}
	cErrList = append(cErrList, cErr)

	/////

	// remObjOut, errc, err := objectRemover(ctx, db, wb, sbf, auditLevel, folderPath, classOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create object-remover component: ")
	// }
	// errcList = append(errcList, errc)

	/////

	cOut, cErr, err = pl.TupleGenerator(ctx, cOut) // ------------------------------------2)
	if err != nil {
		ce <- errors.Wrap(err, "Error: cannot create tuple-generator component: ")
		return nil, ce
	}
	cErrList = append(cErrList, cErr)

	cOut, cErr, err = pl.TripleWriter(ctx, db, cOut) // ---------------------------------3)
	if err != nil {
		ce <- errors.Wrap(err, "Error: cannot create triple-writer component: ")
		return nil, ce
	}
	cErrList = append(cErrList, cErr)

	cOut, cErr, err = pl.LinkParser(ctx, db, cOut) // ---------------------------------------4)
	if err != nil {
		ce <- errors.Wrap(err, "Error: cannot create link-parser component: ")
		return nil, ce
	}
	cErrList = append(cErrList, cErr)

	/////

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
	return cOut, helper.MergeErrors(cErrList...) // helper.WaitForPipeline(cErrList...)
}
