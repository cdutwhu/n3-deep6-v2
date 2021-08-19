package pipeline

import (
	"context"
	"fmt"

	ds "github.com/cdutwhu/n3-deep6-v2/datastruct"
	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
	"github.com/pkg/errors"
)

//
// Given a set of unfulfilled candidate links for an object, checks the hexastore to
// find matches that need linking to.
// Used for matching data that arrived historically, and did not register
// a feature of interest, e.g. from a different data-model; SIF will link
// internally backwards & forwards, but if xapi data arrives after SIF data
// this back-check is required as xapi will not be looking for
// refids or sif local-ids, but will be looking for particular
// values (.Object) properties.
//
// ctx - pipeline management context
// db - badger db used for lookups of objects to link to
// in - channel providing IngestData objects
//
func LinkReverseChecker(ctx context.Context, db *badger.DB, in <-chan ds.IngestData) (
	<-chan ds.IngestData, // pass data on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) {

	cOut := make(chan ds.IngestData)
	cErr := make(chan error, 1)

	go func() {
		defer close(cOut)
		defer close(cErr)

		for igd := range in {

			linksTo := make(map[string]interface{})

			// first see if anything reverse links
			// by checking for the presence of the object member
			//
			for _, candidate := range igd.LinkCandidates {
				if len(candidate.O) > 0 { // don't link to empty content
					prefix := fmt.Sprintf("ops|%s|", candidate.O)
					fdBuf, err := dbset.BadgerSearchByPrefix(db, prefix, func(v interface{}) bool { return v.(int64) > 0 })
					if err != nil {
						cErr <- errors.Wrap(err, "LinkReverseChecker() database search error:")
					}
					for k := range fdBuf {
						t := ds.ParseTriple(k.(string))
						linksTo[t.S] = struct{}{}
					}
				}
			}
			//

			// add any reverse links to the list of viable candidates
			for k := range linksTo {
				// ignore self reverse link
				if k == igd.N3id {
					continue
				}
				reverseLinkTriple := ds.Triple{
					S: "reverse",
					P: "link",
					O: k,
				}
				igd.LinkCandidates = append(igd.LinkCandidates, reverseLinkTriple)
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
