package eventmgr

import (
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v3"
	lk "github.com/digisan/logkit"
)

var once sync.Once

type EDB struct {
	sync.Mutex
	dbTmSpan *badger.DB
	dbID     *badger.DB
}

var eDB *EDB // global, for keeping single instance

func open(dir string) *badger.DB {
	opt := badger.DefaultOptions("").WithInMemory(true)
	if dir != "" {
		opt = badger.DefaultOptions(dir)
	}
	db, err := badger.Open(opt)
	lk.FailOnErr("%v", err)
	return db
}

func GetDB(dir string) *EDB {
	if eDB == nil {
		once.Do(func() {
			eDB = &EDB{
				dbTmSpan: open(filepath.Join(dir, "ts")),
				dbID:     open(filepath.Join(dir, "id")),
			}
		})
	}
	return eDB
}

func (db *EDB) Close() {
	db.Lock()
	defer db.Unlock()

	if db.dbTmSpan != nil {
		lk.FailOnErr("%v", db.dbTmSpan.Close())
		db.dbTmSpan = nil
	}

	if db.dbID != nil {
		lk.FailOnErr("%v", db.dbID.Close())
		db.dbID = nil
	}
}

///////////////////////////////////////////////////////////////

// only for update private using
// func (db *EDB) rmEventBlock(eb *EventBlock, lock bool) error {
// 	if lock {
// 		db.Lock()
// 		defer db.Unlock()
// 	}

// 	ids, _ := eb.MarshalIDsSpan()
// 	for _, id := range ids {
// 		if err := db.dbID.Update(func(txn *badger.Txn) error {
// 			it := txn.NewIterator(badger.DefaultIteratorOptions)
// 			defer it.Close()
// 			if it.Seek([]byte(id)); it.Valid() {
// 				if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
// 					lk.WarnOnErr("%v", err)
// 					return err
// 				}
// 			}
// 			return nil
// 		}); err != nil {
// 			return err
// 		}
// 	}

// 	return db.dbTmSpan.Update(func(txn *badger.Txn) error {
// 		it := txn.NewIterator(badger.DefaultIteratorOptions)
// 		defer it.Close()

// 		if it.Seek([]byte(eb.prevSpan)); it.Valid() {
// 			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
// 				lk.WarnOnErr("%v", err)
// 				return err
// 			}
// 		}
// 		return nil
// 	})
// }

func (db *EDB) AppendEventBlock(eb *EventBlock, lock bool) error {
	if lock {
		db.Lock()
		defer db.Unlock()
	}

	ids, spans := eb.MarshalIDsSpan()
	for i, id := range ids {
		if err := db.dbID.Update(func(txn *badger.Txn) error {
			if err := txn.Set(id, spans[i]); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return db.dbTmSpan.Update(func(txn *badger.Txn) error {
		return txn.Set(eb.MarshalSpanIDs())
	})
}

func (db *EDB) ListEventBlock() (eb *EventBlock, err error) {
	db.Lock()
	defer db.Unlock()

	eb = &EventBlock{}

	err = db.dbTmSpan.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			item.Value(func(v []byte) error {
				eb.UnmarshalSpanIDs(item.Key(), v)
				return nil
			})
		}
		return nil
	})
	return
}
