// jsonreader.go

package deep6

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/pkg/errors"
)

func Test_jsonReaderSource(t *testing.T) {

	// set up a context to manage ingest pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	r, err := os.Open("./mixed.json")
	if err != nil {
		panic(err)
	}

	// monitor all error channels
	var errcList []<-chan error

	//
	// build the pipeline by connecting all stages
	//
	jsonOut, errc, err := jsonReaderSource(ctx, r)
	if err != nil {
		panic(errors.Wrap(err, "Error: cannot create json-reader source component: "))
	}
	errcList = append(errcList, errc)

	for json := range jsonOut {
		for k, v := range json {
			fmt.Println(k)
			fmt.Println(v)
		}
		fmt.Println("---------------------------------------------")
	}
}
