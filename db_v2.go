package eventmgr

import (
	"bytes"

	"github.com/dgraph-io/badger/v3"
)

type DbAccessible interface {
	Key() []byte
	Marshal() (forKey []byte, forValue []byte)
	Unmarshal(dbKey []byte, dbVal []byte) error
	BadgerDB() *badger.DB
}

type PtrDbAccessible[T any] interface {
	DbAccessible
	*T
}

func GetOneObjectDB[V any, T PtrDbAccessible[V]](key []byte) (T, error) {
	var (
		rt  = T(new(V))
		err = rt.BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			if it.Seek(key); it.Valid() {
				if item := it.Item(); bytes.Equal(key, item.Key()) {
					if err := item.Value(func(val []byte) error {
						return rt.Unmarshal(key, val)
					}); err != nil {
						return err
					}
				}
			}
			return nil
		})
	)
	if len(rt.Key()) == 0 {
		return nil, err
	}
	return rt, err
}

// func GetAllObjectsDB[V any, T PtrDbAccessible[V]]() ([]T, error) {
// }

// func GetObjectsDB[V any, T PtrDbAccessible[V]](prefix []byte) ([]T, error) {
// 	rt := []T{}
// }

func SaveOneObjectDB[V any, T PtrDbAccessible[V]](object T) error {
	return object.BadgerDB().Update(func(txn *badger.Txn) error {
		return txn.Set(object.Marshal())
	})
}
