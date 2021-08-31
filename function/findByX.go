package function

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	jt "github.com/digisan/json-tool"
)

func FindByX(byWhat string, db *badger.DB, arg string) string {

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	cJson, cErr, err := JsonFromDBbyX(byWhat, ctx, db, arg)
	defer func() {
		if err != nil {
			fmt.Println(err)
		}
	}()

	select {
	case err = <-cErr:
		return ""
	case ret := <-cJson:
		return jt.TryFmtStr(ret, "\t")
	}
}
