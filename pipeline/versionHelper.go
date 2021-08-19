package deep6

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
	"github.com/digisan/data-block/store/impl"
)

const sep = "|"

func id4v(id string) string {
	return fmt.Sprintf("s%[1]s%[2]s", sep, id)
}

func setVer(id string, ver int64, m *impl.M) *impl.M {
	m.Set(id4v(id), ver)
	return m
}

func currVer(id string, db *badger.DB) (int64, error) {
	key := id4v(id)
	verBuf, err := dbset.BadgerSearchByKey(db, key, func(v interface{}) bool { return v.(int64) > 0 })
	if err != nil {
		return -1, err
	}
	if ver, ok := verBuf[key]; ok {
		return ver.(int64), nil
	}
	return 0, nil
}

func nextVer(id string, db *badger.DB) (int64, error) {
	cv, err := currVer(id, db)
	if err != nil {
		return -1, err
	}
	return cv + 1, nil
}
