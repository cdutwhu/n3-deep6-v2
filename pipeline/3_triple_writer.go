package pipeline

import (
	"context"
	"log"

	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
	"github.com/cdutwhu/n3-deep6-v2/helper"
	"github.com/dgraph-io/badger/v3"
	"github.com/digisan/data-block/store/impl"
)

//
// commits triples to datastore so can be used in
// lookups by later pipeline stages.
//
// ctx - context for pipeline management
// kv - badger.WriteBatch which manages very fast writing to the datastore
// in - channel providing IngestData objects
//
func TripleWriter(ctx context.Context, db *badger.DB, in <-chan *dd.IngestData) (
	<-chan *dd.IngestData, // pass on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) { // returns any error encountered creating this component

	cOut := make(chan *dd.IngestData)
	cErr := make(chan error, 1)

	go func() {
		defer close(cOut)
		defer close(cErr)

		for igd := range in {

			m := impl.NewM()

			ver, err := helper.NewVer(igd.N3id, db)
			if err != nil {
				log.Fatalf("NewVer for %s error\n", igd.N3id)
				cOut <- nil
				continue
			}
			helper.SetVer(igd.N3id, ver, m) // save id & version into database
			igd.Version = ver

			// Save triple content
			for _, t := range igd.Triples {
				for _, hexa := range t.HexaTuple() { // turn each tuple into hexastore entries
					m.Set(hexa, ver) // save triples(spo,...) & version into database
				}
			}

			// flush to database
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
