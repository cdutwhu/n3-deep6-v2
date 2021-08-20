package helper

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
	"github.com/digisan/data-block/store/impl"
)

// const sep = "|"
const idPrefix = "s|"

var FnVerValid = func(v interface{}) bool { return v.(int64) > 0 }

// func idPrefix() string {
// 	return fmt.Sprintf("s%s", sep)
// }

func id4v(id string) string {
	return fmt.Sprintf("%s%s", idPrefix, id)
}

func SetVer(id string, ver int64, m *impl.M) *impl.M {
	m.Set(id4v(id), ver)
	return m
}

func CurrVer(id string, db *badger.DB) (int64, error) {
	key := id4v(id)
	verBuf, err := dbset.BadgerSearchByKey(db, key, FnVerValid)
	if err != nil {
		return -1, err
	}
	if ver, ok := verBuf[key]; ok {
		return ver.(int64), nil
	}
	return 0, nil
}

func NextVer(id string, db *badger.DB) (int64, error) {
	cv, err := CurrVer(id, db)
	if err != nil {
		return -1, err
	}
	return cv + 1, nil
}

func MapAllID(db *badger.DB) (mIdVer map[string]int64, err error) {
	m, err := dbset.BadgerSearchByPrefix(db, idPrefix, FnVerValid)
	if err != nil {
		return nil, err
	}
	mIdVer = make(map[string]int64)
	i := len(idPrefix)
	for k, v := range m {
		mIdVer[k.(string)[i:]] = v.(int64)
	}
	return
}
