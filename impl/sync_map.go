package impl

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

type SM sync.Map

func NewSM() *SM {
	return &SM{}
}

func (sm *SM) Len() int {
	cnt := 0
	sm.Range(func(key string, value int64) bool {
		cnt++
		return true
	})
	return cnt
}

func (sm *SM) Set(key string, value int64) {
	((*sync.Map)(sm)).Store(key, value)
}

func (sm *SM) Get(key string) (int64, bool) {
	if value, ok := ((*sync.Map)(sm)).Load(key); ok {
		return value.(int64), ok
	}
	return int64(0), false
}

func (sm *SM) Remove(key string) {
	if _, ok := sm.Get(key); ok {
		((*sync.Map)(sm)).Delete(key)
	}
}

func (sm *SM) Range(f func(key string, value int64) bool) {
	((*sync.Map)(sm)).Range(func(k, v interface{}) bool {
		return f(k.(string), v.(int64))
	})
}

func (sm *SM) Clear() {
	keys := []string{}
	sm.Range(func(key string, value int64) bool {
		keys = append(keys, key)
		return true
	})
	for _, k := range keys {
		sm.Remove(k)
	}
}

func (sm *SM) OnConflict(f func(existing, coming int64) (bool, int64)) func(existing, coming int64) (bool, int64) {
	if f != nil {
		return f
	}
	return func(existing, coming int64) (bool, int64) {
		return true, coming
	}
}

func (sm *SM) IsPersistent() bool {
	return false
}

///////////////////////////////////////////////////////////////////

func (sm *SM) SyncToBadgerWriteBatch(wb *badger.WriteBatch) (err error) {
	if wb == nil {
		return fmt.Errorf("writebatch is nil, flushed nothing")
	}

	sm.Range(func(key string, value int64) bool {
		kp, e := DBPrefix(key)
		if e != nil {
			panic(errors.Wrap(e, "key type is not supported @ SM FlushToBadger"))
		}
		vp, e := DBPrefix(value)
		if e != nil {
			panic(errors.Wrap(e, "value type is not supported @ SM FlushToBadger"))
		}
		kBuf := append([]byte{kp}, []byte(fmt.Sprint(key))...)
		vBuf := append([]byte{vp}, []byte(fmt.Sprint(value))...)
		if err = wb.Set(kBuf, vBuf); err != nil {
			return false
		}
		return true
	})

	return err
}

func (sm *SM) FlushToBadger(db *badger.DB) (err error) {
	if db == nil {
		return fmt.Errorf("db is nil, flushed nothing")
	}

	wb := db.NewWriteBatch()
	defer wb.Flush()

	return sm.SyncToBadgerWriteBatch(wb)
}
