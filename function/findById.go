package function

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	jt "github.com/digisan/json-tool"
)

func FindById(db *badger.DB, id string) string {

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	cJson, cErr, err := JsonFromDB(ctx, db, id)
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
