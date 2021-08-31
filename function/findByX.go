package function

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	jt "github.com/digisan/json-tool"
)

func FindByX(byWhat string, db *badger.DB, arg string) (ret []string) {

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	cJson, cErr, err := JsonFromDBbyX(byWhat, ctx, db, arg)
	defer func() {
		if err != nil {
			fmt.Println(err)
		}
	}()

ERR_CHK:
	for {
		select {
		case err := <-cErr:
			fmt.Println(err)
			return nil
		default:
			break ERR_CHK
		}
	}

	for j := range cJson {
		ret = append(ret, jt.TryFmtStr(j, "\t"))
	}

	return
}
