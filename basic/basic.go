package basic

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
	"github.com/digisan/data-block/store/impl"
	"github.com/pkg/errors"
)

type status byte

const (
	None     status = 0
	Active   status = 1
	Inactive status = 2
	Unknown  status = 3
)

const (
	verDeleted = int64(0)  // Mark '0' means deleted or inactive
	verErased  = int64(-1) // Mark '-1' means erased
)

const (
	sep       = "|"
	prefixId  = "s" + sep
	prefixSPO = "spo" + sep
	prefixSOP = "sop" + sep
	prefixPSO = "pso" + sep
	prefixPOS = "pos" + sep
	prefixOSP = "osp" + sep
	prefixOPS = "ops" + sep
)

var prefixData = []string{
	prefixSPO,
	prefixSOP,
	prefixPSO,
	prefixPOS,
	prefixOSP,
	prefixOPS,
}

func lcPrefixWrap(prefix ...string) (plcGrp []string) {
	for _, p := range prefix {
		plcGrp = append(plcGrp, "lc-"+p)
	}
	return
}

func lPrefixWrap(prefix ...string) (plGrp []string) {
	for _, p := range prefix {
		plGrp = append(plGrp, "l-"+p)
	}
	return
}

var (
	FnVerActive   = func(k, v interface{}) bool { return v.(int64) > 0 }
	FnVerInactive = func(k, v interface{}) bool { return v.(int64) == int64(0) }
	FnVerToErase  = func(k, v interface{}) bool { return v.(int64) < 0 }
)

func id4v(id string) string {
	return fmt.Sprintf("%s%s", prefixId, id)
}

func SetVer(id string, ver int64, m *impl.M) *impl.M {
	m.Set(id4v(id), ver)
	return m
}

// Mark '0' means deleted or inactive, could be erased in future
func MarkDelete(id string, m *impl.M) *impl.M {
	return SetVer(id, verDeleted, m)
}

// Mark '-1' means erased, could be real erased in future
func MarkErase(id string, m *impl.M) *impl.M {
	return SetVer(id, verErased, m)
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

func IdStatus(id string, db *badger.DB) status {
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

func MapAllId(db *badger.DB, inclInactive bool) (mIdVer map[string]int64, err error) {

	filter := FnVerActive // active version
	if inclInactive {
		filter = nil // all version
	}

	m, err := dbset.BadgerSearchByPrefix(db, prefixId, filter)
	if err != nil {
		return nil, err
	}

	mIdVer = make(map[string]int64)
	i := len(prefixId)
	for k, v := range m {
		mIdVer[k.(string)[i:]] = v.(int64)
	}
	return
}

func DeleteObj(db *badger.DB, ids ...string) error {
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
	return m.FlushToBadger(db) //
}

func CleanupErased(db *badger.DB) error {
	m, err := MapAllId(db, true)
	if err != nil {
		return errors.Wrap(err, "@CleanupErased")
	}

	mErased := make(map[string]struct{})
	for k, v := range m {
		if v == verErased {
			mErased[k] = struct{}{}
		}
	}

	prefixAll := append([]string{prefixId}, prefixData...)
	prefixAll = append(prefixAll, lcPrefixWrap(prefixData...)...)
	prefixAll = append(prefixAll, lPrefixWrap(prefixData...)...)

	mErasedDB := impl.NewM()
	for id := range mErased {
		fmt.Println("\nCould be real erased in database:", id)

		for _, prefix := range prefixAll {
			verBuf, err := dbset.BadgerSearchByPrefix(db, prefix, func(k, v interface{}) bool {
				return strings.Contains(k.(string), "|"+id)
			})
			if err != nil {
				fmt.Println(err)
			}
			for k := range verBuf {
				mErasedDB.Set(k, struct{}{})
				// fmt.Println("deleted:", k)
			}
		}
	}

	fmt.Printf("[%05d] raw tuples will be erased", len(*mErasedDB))
	return dbset.RemoveToBadger(mErasedDB, db)
}
