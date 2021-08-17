// triplewriter.go

package deep6

import (
	"context"

	st "github.com/cdutwhu/n3-deep6-v2/struct"
	"github.com/dgraph-io/badger/v3"
	"github.com/digisan/data-block/store"
	"github.com/digisan/data-block/store/db"
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
func TripleWriter(ctx context.Context, kv *store.KVStorage, bagerdb *badger.DB, in <-chan st.IngestData) (
	<-chan st.IngestData, // pass on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) { // returns any error encountered creating this component

	cOut := make(chan st.IngestData)
	cErr := make(chan error, 1)

	go func() {
		defer close(cOut)
		defer close(cErr)

		for igd := range in {

			// Save updated ID(object) version
			var NewID int64 = 1
			idBuf := impl.NewSM()
			db.SyncFromBadgerByKey(idBuf, bagerdb, igd.N3id, nil)
			if id, ok := idBuf.Get(igd.N3id); ok {
				if id.(int64) == 0 {
					return // if version is 0, which means already deleted, ignore this ID for saving
				}
				NewID = id.(int64) + 1
			}
			kv.Save(igd.N3id, NewID)

			// Save triple content
			for _, t := range igd.Triples {
				for _, hexa := range t.HexaTuple("|") { // turn each tuple into hexastore entries
					kv.Save(hexa, NewID)
				}
			}
			select {
			case cOut <- igd: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return cOut, cErr, nil
}
