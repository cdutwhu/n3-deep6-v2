package deep6

import (
	"os"
	"testing"
)

func Test_runIngestWithReader(t *testing.T) {
	f, e := os.Open("./mixed.json")
	if e != nil {
		panic(e)
	}
	runIngestWithReader(f, "./")
}
