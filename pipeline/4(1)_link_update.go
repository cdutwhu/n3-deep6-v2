package pipeline

import (
	"fmt"

	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
	"github.com/cdutwhu/n3-deep6-v2/helper"
	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
)

func UpdateLinkCandidates(db *badger.DB) {
	mIdVer, err := helper.MapAllId(db)
	if err != nil {
		panic(err)
	}
	for id, ver := range mIdVer {
		fmt.Println("\nID:", id)
		prefix := fmt.Sprintf("lc-spo|%s|", id)
		fdBuf, _ := dbset.BadgerSearchByPrefix(db, prefix, func(v interface{}) bool { return v.(int64) == ver })
		for k := range fdBuf {
			t := dd.ParseTripleLC(k.(string))
			// fmt.Printf("Link Value: %s\n", t.O)
			lkVal := t.O

			if len(lkVal) > 0 {
				prefix := fmt.Sprintf("ops|%s|", lkVal)
				fdBuf, _ := dbset.BadgerSearchByPrefix(db, prefix, helper.FnVerActive)
				for k := range fdBuf {
					t := dd.ParseTriple(k.(string))
					if t.S != id {
						fmt.Printf("foreign ID [%s] takes [%s]\n", t.S, lkVal)
					}
				}
			}
		}
	}
}
