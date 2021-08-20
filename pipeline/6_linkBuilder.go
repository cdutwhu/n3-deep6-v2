package pipeline

import (
	"context"
	"fmt"

	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
	"github.com/cdutwhu/n3-deep6-v2/helper"
	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
	"github.com/digisan/data-block/store/impl"
	"github.com/pkg/errors"
)

//
// Given a set of candidate links for an object, checks the hexastore to
// find matches that need linking to.
//
// ctx - pipeline management context
// db - badger db used for lookups of objects to link to
// wb - badger.Writebatch for fast writing of new link objects
// in - channel providing IngestData objects
//
func LinkBuilder(ctx context.Context, db *badger.DB, in <-chan dd.IngestData) (
	<-chan dd.IngestData, // pass data on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) {

	cOut := make(chan dd.IngestData)
	cErr := make(chan error, 1)

	go func() {
		defer close(cOut)
		defer close(cErr)

		for igd := range in {
			linksTo := make(map[string]interface{})

			// get current igd version, "s|id" for key
			ver, err := helper.CurrVer(igd.N3id, db)
			if err != nil {
				cErr <- err
			}

			// first see if anything links
			//
			for _, candidate := range igd.LinkCandidates {
				prefix := fmt.Sprintf("spo|%s|is-a|", candidate.O)
				fdBuf, err := dbset.BadgerSearchByPrefix(db, prefix, helper.FnVerValid)
				if err != nil {
					cErr <- errors.Wrap(err, "Linkbuilder database search error:")
				}
				for k := range fdBuf {
					t := dd.ParseTriple(k.(string))
					linksTo[t.S] = struct{}{}
				}
			}
			//

			// buffer for update badger database
			m := impl.NewM()
			fmt.Println()

			//
			// if not all specified links are satisfied, then a new link
			// entity needs to be created, may be temporary as will resolve
			// to 'real' objects as more data arrives.
			//
			if len(linksTo) < len(igd.LinkCandidates) {
				for _, candidate := range igd.LinkCandidates {
					if _, ok := linksTo[candidate.O]; ok { // only build a link if needed
						continue
					}
					if candidate.O == "" { // ignore empty links
						continue
					}
					if candidate.O == igd.Unique { // ignore the pseudo-unique link here
						continue
					}
					// if needed generate new triple and store in the db
					propertyLinkTriple := dd.Triple{
						S: candidate.O,
						P: "is-a",
						O: "Property.Link",
					}
					for _, t := range propertyLinkTriple.HexaTuple() {
						m.Set(t, ver)
						linksTo[propertyLinkTriple.S] = struct{}{} // add new link
					}
				}
			}

			//
			// now create links to any declared psuedo-unique properties;
			// forces a property link to exist that creates a unique identity
			// for the object if its own data has no available discrimination
			//
			if len(igd.Unique) > 0 {
				uniqueLinkTriple := dd.Triple{
					S: igd.Unique,
					P: "is-a",
					O: "Unique.Link",
				}
				for _, t := range uniqueLinkTriple.HexaTuple() {
					m.Set(t, ver)
					linksTo[uniqueLinkTriple.S] = struct{}{} // add new link
				}
			}

			// update badger database
			m.FlushToBadger(db)

			// convert all known links into link triples
			linkTriples := make([]dd.Triple, 0)
			for l := range linksTo {
				if l == igd.N3id {
					continue // don't self link
				}
				t := dd.Triple{
					S: igd.N3id,
					P: "references",
					O: l,
				}
				linkTriples = append(linkTriples, t)
			}
			igd.LinkTriples = linkTriples

			select {
			case cOut <- igd: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}

		} // end of for
	}()

	return cOut, cErr, nil
}
