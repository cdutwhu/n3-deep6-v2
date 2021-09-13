package basic

import (
	"fmt"
	"strings"

	"github.com/cdutwhu/n3-deep6-v2/dbset"
	"github.com/cdutwhu/n3-deep6-v2/impl"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

type status byte

const (
	None     status = 0
	Active   status = 1
	Inactive status = 2
	Unknown  status = 3
)

func (sta status) String() string {
	switch sta {
	case None:
		return "None"
	case Active:
		return "Active"
	case Inactive:
		return "Inactive"
	case Unknown:
		fallthrough
	default:
		return "Unknown"
	}
}

const (
	verDeleted = int64(0)  // Mark '0' means deleted or inactive
	verErased  = int64(-1) // Mark '-1' means erased
)

const (
	sep    = "|"
	pfxId  = "s" + sep
	pfxSPO = "spo" + sep
	pfxSOP = "sop" + sep
	pfxPSO = "pso" + sep
	pfxPOS = "pos" + sep
	pfxOSP = "osp" + sep
	pfxOPS = "ops" + sep
)

var pfxData = []string{
	pfxSPO,
	pfxSOP,
	pfxPSO,
	pfxPOS,
	pfxOSP,
	pfxOPS,
}

func lcPfxWrap(prefix ...string) (plcGrp []string) {
	for _, p := range prefix {
		plcGrp = append(plcGrp, "lc-"+p)
	}
	return
}

func lPfxWrap(prefix ...string) (plGrp []string) {
	for _, p := range prefix {
		plGrp = append(plGrp, "l-"+p)
	}
	return
}

var (
	FnVerActive   = func(k string, v int64) bool { return v > 0 }
	FnVerInactive = func(k string, v int64) bool { return v == int64(0) }
	FnVerToErase  = func(k string, v int64) bool { return v < 0 }
)

func id4v(id string) string {
	return fmt.Sprintf("%s%s", pfxId, id)
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

func MapAllId(db *badger.DB, inclInactive bool) (mIdVer *impl.SM, err error) {

	filter := FnVerActive // active version
	if inclInactive {
		filter = nil // all version
	}

	m, err := dbset.BadgerFindByPfx(db, pfxId, filter)
	if err != nil {
		return nil, err
	}

	mIdVer = impl.NewSM()
	i := len(pfxId)
	for k, v := range m {
		mIdVer.Set(k[i:], v)
	}
	return
}

func NewVer(id string, mIdVer *impl.SM, db *badger.DB) (int64, error) {
	var err error
	if mIdVer == nil {
		if mIdVer, err = MapAllId(db, true); err != nil {
			panic(errors.Wrap(err, "@NewVer"))
		}
	}

	sta := IdStatus(id, mIdVer, db)
	if sta == None || sta == Active {
		cv, err := CurVer(id, mIdVer, db)
		if err != nil {
			return -1, err
		}
		mIdVer.Set(id, cv+1) // mIdVer must be updated for following pipeline use
		return cv + 1, nil
	}
	return -1, fmt.Errorf("%s is inactive, cannot be set a new version", id)
}

func CurVer(id string, mIdVer *impl.SM, db *badger.DB) (int64, error) {
	if mIdVer != nil {
		if ver, ok := mIdVer.Get(id); ok && ver > 0 { // active version
			return ver, nil
		}
		return 0, nil
	}

	key := id4v(id)
	mIdVerBuf, err := dbset.BadgerFindByKey(db, key, FnVerActive) // active version
	if err != nil {
		return -1, err
	}
	if ver, ok := mIdVerBuf[key]; ok {
		return ver, nil
	}
	return 0, nil
}

func InactiveCheck(id string, mIdVer *impl.SM, db *badger.DB) bool {
	if mIdVer != nil {
		if ver, ok := mIdVer.Get(id); ok && ver == int64(0) { // inactive version
			return true
		}
		return false
	}

	key := id4v(id)
	mIdVerBuf, err := dbset.BadgerFindByKey(db, key, FnVerInactive) // inactive version
	if err == nil {
		if _, ok := mIdVerBuf[key]; ok {
			return true
		}
	}
	return false
}

func IdStatus(id string, mIdVer *impl.SM, db *badger.DB) status {
	var err error
	if mIdVer == nil {
		if mIdVer, err = MapAllId(db, true); err != nil {
			panic(errors.Wrap(err, "@IdStatus"))
		}
	}

	ver, err := CurVer(id, mIdVer, db)
	switch {
	case err != nil:
		return Unknown
	case ver > 0:
		return Active
	case InactiveCheck(id, mIdVer, db):
		return Inactive
	case ver == int64(0):
		return None
	default:
		return Unknown
	}
}

func DeleteObj(mIdVer *impl.SM, db *badger.DB, ids ...string) error {
	var err error
	if mIdVer == nil {
		if mIdVer, err = MapAllId(db, true); err != nil {
			panic(errors.Wrap(err, "@DeleteObj"))
		}
	}

	m := impl.NewM()
	for _, id := range ids {
		if IdStatus(id, mIdVer, db) == Active {
			MarkDelete(id, m)
		}
	}
	return m.FlushToBadger(db)
}

func EraseObj(mIdVer *impl.SM, db *badger.DB, ids ...string) error {
	var err error
	if mIdVer == nil {
		if mIdVer, err = MapAllId(db, true); err != nil {
			panic(errors.Wrap(err, "@EraseObj"))
		}
	}

	m := impl.NewM()
	for _, id := range ids {
		if IdStatus(id, mIdVer, db) == Inactive {
			MarkErase(id, m)
		}
	}
	return m.FlushToBadger(db)
}

func CleanupErased(db *badger.DB) error {
	m, err := MapAllId(db, true)
	if err != nil {
		return errors.Wrap(err, "@CleanupErased")
	}

	mErased := make(map[string]struct{})
	m.Range(func(k string, v int64) bool {
		if v == verErased {
			mErased[k] = struct{}{}
		}
		return true
	})

	pfxAll := append([]string{pfxId}, pfxData...)
	pfxAll = append(pfxAll, lcPfxWrap(pfxData...)...)
	pfxAll = append(pfxAll, lPfxWrap(pfxData...)...)

	mErasedDB := impl.NewM()
	for id := range mErased {
		fmt.Println("\nCould be real erased in database:", id)

		for _, pfx := range pfxAll {
			mIdVerBuf, err := dbset.BadgerFindByPfx(db, pfx, func(k string, v int64) bool {
				return strings.Contains(k, "|"+id)
			})
			if err != nil {
				fmt.Println(err)
			}
			for k := range mIdVerBuf {
				mErasedDB.Set(k, int64(0))
				// fmt.Println("deleted:", k)
			}
		}
	}

	fmt.Printf("[%05d] raw tuples will be erased", len(*mErasedDB))
	return dbset.RemoveToBadger(mErasedDB, db)
}
