package pipeline

import (
	"context"

	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
)

func Audit(ctx context.Context, in <-chan *dd.IngestData) (
	<-chan *dd.IngestData, // new list of triples also containing links
	<-chan error, // emits errors encountered to the pipeline
	error) { // returns any errors when creating this component

	cOut := make(chan *dd.IngestData)
	cErr := make(chan error, 1)

	go func() {
		defer close(cOut)
		defer close(cErr)

		I := 1
		for igd := range in {
			if igd != nil {
				igd.Print(I, "RawBytes", "RawData", "Triples", "LinkCandidates", "LinkTriples")
				I++

				select {
				case cOut <- igd: // pass the data on to the next stage
				case <-ctx.Done(): // listen for pipeline shutdown
					return
				}
			}
		}
	}()

	return cOut, cErr, nil
}
