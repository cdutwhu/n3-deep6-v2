package deep6

import (
	"context"
	"io"

	"github.com/cdutwhu/n3-deep6-v2/basic"
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
// r - the io.Reader (file, http body etc.) to be ingested
// auditLevel - one of: none, basic, high
//
func RunIngest(ctx context.Context, r io.Reader, db *badger.DB) (err error) {

	// speed up version-functions
	mIdVer, err := basic.MapAllId(db, true)
	if err != nil {
		return err
	}

	wb := db.NewWriteBatch() // Flush action at bottom

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

	cOut, cErr, err = pl.TripleWriter(ctx, db, mIdVer, wb, cOut)
	if err != nil {
		return errors.Wrap(err, "Error: TripleWriter ")
	}
	cErrList = append(cErrList, cErr)

	// --------------------------------------------------------------------------------- 4)

	cOut, cErr, err = pl.LinkCandidateWriter(ctx, db, mIdVer, wb, cOut)
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
		if err := wb.Flush(); err != nil { // save 'triples' & 'link-candidates' into badger
			panic("*** write batch flush panic ***")
		}
	}()

	// monitor progress
	return helper.WaitForPipeline(cErrList...)
}

func RunLinkBuilder(db *badger.DB) {
	wb := db.NewWriteBatch() // reset write batch
	defer wb.Flush()         // save 'links' into badger
	pl.LinkBuilder(db, wb)   // update database for creating linkage
}
