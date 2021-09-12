package pipeline

import (
	"fmt"

	. "github.com/cdutwhu/n3-deep6-v2/basic"
	dd "github.com/cdutwhu/n3-deep6-v2/datadef"
	"github.com/cdutwhu/n3-deep6-v2/dbset"
	"github.com/cdutwhu/n3-deep6-v2/impl"
	"github.com/dgraph-io/badger/v3"
)

func LinkBuilder(db *badger.DB, wb *badger.WriteBatch) {

	mIdVer, err := MapAllId(db, false)
	if err != nil {
		panic(err)
	}

	m := impl.NewM()
	if wb != nil {
		defer m.SyncToBadgerWriteBatch(wb) // wb.Flush in out caller
	} else {
		defer m.FlushToBadger(db)
	}

	mIdVer.Range(func(id string, ver int64) bool {
		// fmt.Println("\nID:", id)

		pfx := fmt.Sprintf("lc-spo|%s|", id)
		fdBuf, _ := dbset.BadgerSearchByPfx(db, pfx, func(k string, v int64) bool { return v == ver })

		for k := range fdBuf {
			t := dd.ParseTripleLinkCandidate(k)
			// fmt.Printf("Link Value: %s\n", t.O)

			if foreignKeyVal := t.O; len(foreignKeyVal) > 0 {
				pfx := fmt.Sprintf("ops|%s|", foreignKeyVal)
				fdBuf, _ := dbset.BadgerSearchByPfx(db, pfx, FnVerActive)

				for k := range fdBuf {
					t := dd.ParseTripleData(k)
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
		return true
	})
}
