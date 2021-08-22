package helper

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
	"github.com/digisan/data-block/store/impl"
)

type sta byte

const (
	None     sta = 0
	Active   sta = 1
	Inactive sta = 2
	Unknown  sta = 3
)

// const sep = "|"
const idPrefix = "s|"

var FnVerActive = func(v interface{}) bool { return v.(int64) > 0 }
var FnVerInactive = func(v interface{}) bool { return v.(int64) == int64(0) }

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

func MarkDelete(id string, m *impl.M) *impl.M {
	return SetVer(id, 0, m)
}

func MarkErase(id string, m *impl.M) *impl.M {
	m.Set(id4v(id), struct{}{})
	return m
}

func CurVer(id string, db *badger.DB) (int64, error) {
	key := id4v(id)
	verBuf, err := dbset.BadgerSearchByKey(db, key, FnVerActive) // active version
	if err != nil {
		return -1, err
	}
	if ver, ok := verBuf[key]; ok {
		return ver.(int64), nil
	}
	return 0, nil
}

func InactiveCheck(id string, db *badger.DB) bool {
	key := id4v(id)
	verBuf, err := dbset.BadgerSearchByKey(db, key, FnVerInactive) // inactive version
	if err == nil {
		if _, ok := verBuf[key]; ok {
			return true
		}
	}
	return false
}

func IdStatus(id string, db *badger.DB) sta {
	ver, err := CurVer(id, db)
	switch {
	case err != nil:
		return Unknown
	case ver > 0:
		return Active
	case InactiveCheck(id, db):
		return Inactive
	case ver == int64(0):
		return None
	default:
		return Unknown
	}
}

func NewVer(id string, db *badger.DB) (int64, error) {
	sta := IdStatus(id, db)
	if sta == None || sta == Active {
		cv, err := CurVer(id, db)
		if err != nil {
			return -1, err
		}
		return cv + 1, nil
	}
	return -1, fmt.Errorf("%s is inactive, cannot be set a new version", id)
}

func MapAllId(db *badger.DB) (mIdVer map[string]int64, err error) {
	m, err := dbset.BadgerSearchByPrefix(db, idPrefix, FnVerActive) // active version
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

func DelObj(db *badger.DB, ids ...string) error {
	m := impl.NewM()
	for _, id := range ids {
		if IdStatus(id, db) == Active {
			MarkDelete(id, m)
		}
	}
	return m.FlushToBadger(db)
}

func EraseObj(db *badger.DB, ids ...string) error {
	m := impl.NewM()
	for _, id := range ids {
		if IdStatus(id, db) == Inactive {
			MarkErase(id, m)
		}
	}
	return dbset.RemoveToBadger(m, db)
}
