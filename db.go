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
	dbSpanIDs *badger.DB
	dbIDDesc  *badger.DB
	dbIDCmnt  *badger.DB
	dbIDAuth  *badger.DB
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
				dbSpanIDs: open(filepath.Join(dir, "span-id")),
				dbIDDesc:  open(filepath.Join(dir, "id-desc")),
				dbIDCmnt:  open(filepath.Join(dir, "id-cmnt")),
				dbIDAuth:  open(filepath.Join(dir, "id-auth")),
			}
		})
	}
	return eDB
}

func (db *EDB) Close() {
	db.Lock()
	defer db.Unlock()

	if db.dbSpanIDs != nil {
		lk.FailOnErr("%v", db.dbSpanIDs.Close())
		db.dbSpanIDs = nil
	}

	if db.dbIDDesc != nil {
		lk.FailOnErr("%v", db.dbIDDesc.Close())
		db.dbIDDesc = nil
	}

	if db.dbIDCmnt != nil {
		lk.FailOnErr("%v", db.dbIDCmnt.Close())
		db.dbIDCmnt = nil
	}

	if db.dbIDAuth != nil {
		lk.FailOnErr("%v", db.dbIDAuth.Close())
		db.dbIDAuth = nil
	}
}

///////////////////////////////////////////////////////////////

// only for update private using
// func (db *EDB) rmEventBlock(es *EventSpan, lock bool) error {
// 	if lock {
// 		db.Lock()
// 		defer db.Unlock()
// 	}

// 	ids, _ := es.MarshalIDsSpan()
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

// 		if it.Seek([]byte(es.prevSpan)); it.Valid() {
// 			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
// 				lk.WarnOnErr("%v", err)
// 				return err
// 			}
// 		}
// 		return nil
// 	})
// }

func (db *EDB) AppendEvtToSpan(es *EventSpan, lock bool) error {
	if lock {
		db.Lock()
		defer db.Unlock()
	}

	// ids, spans := es.MarshalIDsSpan()
	// for i, id := range ids {
	// 	if err := db.dbIDDesc.Update(func(txn *badger.Txn) error {
	// 		if err := txn.Set(id, spans[i]); err != nil {
	// 			return err
	// 		}
	// 		return nil
	// 	}); err != nil {
	// 		return err
	// 	}
	// }

	return db.dbSpanIDs.Update(func(txn *badger.Txn) error {
		return txn.Set(es.Marshal())
	})
}

func (db *EDB) ListEventBlock() (es *EventSpan, err error) {
	db.Lock()
	defer db.Unlock()

	es = &EventSpan{}

	err = db.dbSpanIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			item.Value(func(v []byte) error {
				es.Unmarshal(item.Key(), v)
				return nil
			})
		}
		return nil
	})
	return
}

///////////////////////////////////////////////////////////////
