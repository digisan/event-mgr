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

// one object with fixed key
func GetOneObjectDB[V any, T PtrDbAccessible[V]](key []byte) (T, error) {
	var (
		rt  = T(new(V))
		err = rt.BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemproc := func(item *badger.Item) error {
				if bytes.Equal(key, item.Key()) {
					if err := item.Value(func(val []byte) error {
						return rt.Unmarshal(key, val)
					}); err != nil {
						return err
					}
				}
				return nil
			}

			if it.Seek(key); it.Valid() {
				return itemproc(it.Item())
			}

			return nil
		})
	)
	if len(rt.Key()) == 0 {
		return nil, err
	}
	return rt, err
}

// all objects if prefix is nil or empty
func GetObjectsDB[V any, T PtrDbAccessible[V]](prefix []byte) ([]T, error) {
	var (
		rt  = []T{}
		err = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemproc := func(item *badger.Item) error {
				return item.Value(func(val []byte) error {
					one := T(new(V))
					if err := one.Unmarshal(item.Key(), val); err != nil {
						return err
					}
					rt = append(rt, one)
					return nil
				})
			}

			if len(prefix) == 0 {
				for it.Rewind(); it.Valid(); it.Next() {
					if err := itemproc(it.Item()); err != nil {
						return err
					}
				}
			} else {
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					if err := itemproc(it.Item()); err != nil {
						return err
					}
				}
			}

			return nil
		})
	)
	return rt, err
}

// update or insert one object
func UpsertOneObjectDB[V any, T PtrDbAccessible[V]](object T) error {
	return object.BadgerDB().Update(func(txn *badger.Txn) error {
		return txn.Set(object.Marshal())
	})
}

// delete one object
func DeleteOneObjectDB[V any, T PtrDbAccessible[V]](key []byte) error {
	return T(new(V)).BadgerDB().Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Seek(key); it.Valid() {
			if item := it.Item(); bytes.Equal(key, item.Key()) {
				return txn.Delete(item.KeyCopy(nil))
			}
		}

		return nil
	})
}
