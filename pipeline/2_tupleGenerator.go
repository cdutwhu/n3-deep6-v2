package deep6

import (
	"context"
	"fmt"

	st "github.com/cdutwhu/n3-deep6-v2/struct"
	jt "github.com/digisan/json-tool"
)

//
// Turns the original data into a list of
// subject:predicate:object Triples
//
// ctx - context used for pipeline management
// in - channel providing IngestData objects
//
func TupleGenerator(ctx context.Context, in <-chan st.IngestData) (
	<-chan st.IngestData,
	<-chan error, // emits errors encountered to the pipeline
	error) { // any error encountered when creating this component

	cOut := make(chan st.IngestData)
	cErr := make(chan error, 1)

	go func() {
		defer close(cOut)
		defer close(cErr)

		for igd := range in {

			var err error
			igd.RawData, err = jt.Flatten(igd.RawBytes) // turn json m into predicate:object pairs
			if err != nil {
				cErr <- err
			}

			// re-append additional info like 'is-a', 'unique' from previous step
			igd.RawData["is-a"] = igd.Type
			igd.RawData["unique"] = igd.Unique

			// create list of subject:predicate:object triples
			tuples := make([]st.Triple, 0)
			for k, v := range igd.RawData {
				t := st.Triple{
					S: igd.N3id,
					P: k,
					O: fmt.Sprintf("%v", v),
				}
				tuples = append(tuples, t)
			}

			igd.Triples = tuples

			select {
			case cOut <- igd: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return cOut, cErr, nil
}
