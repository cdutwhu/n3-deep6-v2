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

var (
	AuditStep = 0
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
func RunIngest(ctx context.Context, r io.Reader, db *badger.DB) (err error) {

	// monitor all error channels
	var cErrList []<-chan error

	cJsonOut, cErr, err := jt.ScanObjectInArray(ctx, r, true)
	if err != nil {
		return errors.Wrap(err, "Error: ScanObjectInArray ")
	}
	cErrList = append(cErrList, cErr)

	// --------------------------------------------------------------------------------- 1)

	cOut, cErr, err := pl.ObjectClassifier(ctx, cJsonOut)
	if err != nil {
		return errors.Wrap(err, "Error: ObjectClassifier ")
	}
	cErrList = append(cErrList, cErr)

	// --------------------------------------------------------------------------------- 2)

	cOut, cErr, err = pl.TupleGenerator(ctx, cOut)
	if err != nil {
		return errors.Wrap(err, "Error: TupleGenerator ")
	}
	cErrList = append(cErrList, cErr)

	// --------------------------------------------------------------------------------- 3)

	cOut, cErr, err = pl.TripleWriter(ctx, db, cOut)
	if err != nil {
		return errors.Wrap(err, "Error: TripleWriter ")
	}
	cErrList = append(cErrList, cErr)

	// --------------------------------------------------------------------------------- 4)

	cOut, cErr, err = pl.LinkCandidateWriter(ctx, db, cOut)
	if err != nil {
		return errors.Wrap(err, "Error: LinkCandidateWriter ")
	}
	cErrList = append(cErrList, cErr)

	// --------------------------------------------------------------------------------- Audit)

	cOut, cErr, err = pl.Audit(ctx, cOut)
	if err != nil {
		return errors.Wrap(err, "Error: Audit ")
	}
	cErrList = append(cErrList, cErr)

	go func() {
		for range cOut {
		}
		pl.LinkBuilder(db) // update database for creating linkage
	}()

	// monitor progress
	return helper.WaitForPipeline(cErrList...)
}
