package impl

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
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

// for badger db storage for differentiate data type
func DBPrefix(input interface{}) (prefix byte, err error) {
	switch i := input.(type) {
	case string:
		prefix = 's'
	case bool:
		prefix = 'b'
	case int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint, uintptr:
		prefix = 'i'
	case float32, float64:
		prefix = 'f'
	case complex64, complex128:
		prefix = 'c'
	case nil:
		prefix = 'n'
	case struct{}:
		prefix = 'e'
	default:
		err = fmt.Errorf("%v is not supported for prefix", i)
	}
	return
}

func (m *M) SyncToBadgerWriteBatch(wb *badger.WriteBatch) (err error) {
	if wb == nil {
		return fmt.Errorf("writebatch is nil, flushed nothing")
	}

	for key, value := range *m {
		kp, e := DBPrefix(key)
		if e != nil {
			panic(errors.Wrap(e, "key type is not supported @ M FlushToBadger"))
		}
		vp, e := DBPrefix(value)
		if e != nil {
			panic(errors.Wrap(e, "value type is not supported @ M FlushToBadger"))
		}
		kBuf := append([]byte{kp}, []byte(fmt.Sprint(key))...)
		vBuf := append([]byte{vp}, []byte(fmt.Sprint(value))...)
		if err = wb.Set(kBuf, vBuf); err != nil {
			break
		}
	}

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
