package deep6

import (
	"context"

	st "github.com/cdutwhu/n3-deep6-v2/struct"
	"github.com/dgraph-io/badger/v3"
	"github.com/digisan/data-block/store/impl"
)

//
// commits triples to datastore so can be used in
// lookups by later pipeline stages.
//
// ctx - context for pipeline management
// kv - badger.WriteBatch which manages very fast writing to the
// datastore
// in - channel providing IngestData objects
//
func TripleWriter(ctx context.Context, db *badger.DB, in <-chan st.IngestData) (
	<-chan st.IngestData, // pass on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) { // returns any error encountered creating this component

	cOut := make(chan st.IngestData)
	cErr := make(chan error, 1)

	go func() {
		defer close(cOut)
		defer close(cErr)

		for igd := range in {

			m := impl.NewM()

			ver, err := nextVer(igd.N3id, db)
			if err != nil {
				cErr <- err
			}
			setVer(igd.N3id, ver, m)

			// Save triple content
			for _, t := range igd.Triples {
				for _, hexa := range t.HexaTuple() { // turn each tuple into hexastore entries
					m.Set(hexa, ver)
				}
			}

			m.FlushToBadger(db)

			select {
			case cOut <- igd: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return cOut, cErr, nil
}
