package pipeline

import (
	"context"
	"strings"

	. "github.com/cdutwhu/n3-deep6-v2/basic"
	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
	"github.com/dgraph-io/badger/v3"
	"github.com/digisan/data-block/store/impl"
)

//
// parses inbound object for candidate properties to link into the graph
//
// ctx - pipeline management context
// in - channel providing IngestData objects
//
func LinkCandidateWriter(ctx context.Context, db *badger.DB, mIdVer *impl.SM, wb *badger.WriteBatch, in <-chan *dd.IngestData) (
	<-chan *dd.IngestData, // new list of triples also containing links
	<-chan error, // emits errors encountered to the pipeline
	error) { // returns any errors when creating this component

	cOut := make(chan *dd.IngestData)
	cErr := make(chan error, 1)
	var err error

	go func() {
		defer close(cOut)
		defer close(cErr)

		if mIdVer == nil {
			if mIdVer, err = MapAllId(db, false); err != nil {
				return
			}
		}

		for igd := range in {

			if igd == nil {
				cOut <- igd
				continue
			}

			ver, err := CurVer(igd.N3id, mIdVer, db)
			if err != nil {
				cErr <- err
				continue
			}

			// at the first ingest for a new id, db has no record; we set it to 1.
			// DO NOT modify mIdVer for map safety reason
			if ver == int64(0) {
				ver = int64(1)
			}

			func() {
				m := impl.NewM()
				if wb != nil {
					defer m.SyncToBadgerWriteBatch(wb)
				} else {
					defer m.FlushToBadger(db)
				}

				for _, t := range igd.Triples {
					for _, s := range igd.LinkSpecs {
						if strings.Contains(t.P, s) { // select link-spec tuples
							igd.LinkCandidates = append(igd.LinkCandidates, t)
							t.Cache2LinkCandidate(m, ver)
						}
					}
					if t.O == igd.Unique {
						t.Cache2LinkCandidate(m, ver)
					}
				}
			}()

			select {
			case cOut <- igd: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return cOut, cErr, err
}
