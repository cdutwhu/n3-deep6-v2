package pipeline

import (
	"fmt"

	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
	"github.com/cdutwhu/n3-deep6-v2/helper"
	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
	"github.com/digisan/data-block/store/impl"
)

func LinkBuilder(db *badger.DB) {

	mIdVer, err := helper.MapAllId(db)
	if err != nil {
		panic(err)
	}

	m := impl.NewM()
	defer m.FlushToBadger(db)

	for id, ver := range mIdVer {
		fmt.Println("\nID:", id)

		prefix := fmt.Sprintf("lc-spo|%s|", id)
		fdBuf, _ := dbset.BadgerSearchByPrefix(db, prefix, func(v interface{}) bool { return v.(int64) == ver })

		for k := range fdBuf {
			t := dd.ParseTripleLinkCandidate(k.(string))
			// fmt.Printf("Link Value: %s\n", t.O)

			if foreignKeyVal := t.O; len(foreignKeyVal) > 0 {
				prefix := fmt.Sprintf("ops|%s|", foreignKeyVal)
				fdBuf, _ := dbset.BadgerSearchByPrefix(db, prefix, helper.FnVerActive)

				for k := range fdBuf {
					t := dd.ParseTripleData(k.(string))
					if t.S != id {
						// fmt.Printf("foreign ID [%s] takes [%s]\n", t.S, foreignKeyVal)
						link := dd.Triple{
							S: id,            // who
							P: foreignKeyVal, // which value
							O: t.S,           // referred by whom
						}
						link.Cache2Link(m, ver)
					}
				}
			}
		}
	}
}
