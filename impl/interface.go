package impl

type Ikv interface {
	Len() int
	Set(key string, value int64)
	Get(key string) (int64, bool)
	Remove(key string)
	Range(f func(key string, value int64) bool)
	Clear()
	OnConflict(f func(existing, coming int64) (bool, int64)) func(existing, coming int64) (bool, int64)
	IsPersistent() bool
}
