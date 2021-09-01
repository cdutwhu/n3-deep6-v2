package impl

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
)

type M map[string]int64

func NewM() *M {
	m := make(M)
	return &m
}

func (m *M) Len() int {
	return len(*(*map[string]int64)(m))
}

func (m *M) Set(key string, value int64) {
	(*m)[key] = value
}

func (m *M) Get(key string) (int64, bool) {
	value, ok := (*m)[key]
	return value, ok
}

func (m *M) Remove(key string) {
	delete(*m, key)
}

func (m *M) Range(f func(key string, value int64) bool) {
	for k, v := range *m {
		if !f(k, v) {
			break
		}
	}
}

func (m *M) Clear() {
	keys := []string{}
	for k := range *m {
		keys = append(keys, k)
	}
	for _, k := range keys {
		delete(*m, k)
	}
}

func (m *M) OnConflict(f func(existing, coming int64) (bool, int64)) func(existing, coming int64) (bool, int64) {
	if f != nil {
		return f
	}
	return func(existing, coming int64) (bool, int64) {
		return true, coming
	}
}

func (m *M) IsPersistent() bool {
	return false
}

///////////////////////////////////////////////////////////////////

func (m *M) SyncToBadgerWriteBatch(wb *badger.WriteBatch) (err error) {
	if wb == nil {
		return fmt.Errorf("writebatch is nil, flushed nothing")
	}

	m.Range(func(key string, value int64) bool {
		kBuf := []byte("s" + key)
		vBuf := []byte("i" + fmt.Sprint(value))
		if err = wb.Set(kBuf, vBuf); err != nil {
			return false
		}
		return true
	})
	return err
}

func (m *M) FlushToBadger(db *badger.DB) (err error) {
	if db == nil {
		return fmt.Errorf("db is nil, flushed nothing")
	}

	wb := db.NewWriteBatch()
	defer wb.Flush()

	return m.SyncToBadgerWriteBatch(wb)
}
