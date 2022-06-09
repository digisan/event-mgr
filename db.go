package eventmgr

import (
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v3"
	lk "github.com/digisan/logkit"
)

var onceEDB sync.Once

type EDB struct {
	sync.Mutex
	dbSpanIDs  *badger.DB
	dbIDEvt    *badger.DB
	dbIDSubIDs *badger.DB
	dbOwnerIDs *badger.DB
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

// init global 'eDB'
func InitDB(dir string) *EDB {
	if eDB == nil {
		onceEDB.Do(func() {
			eDB = &EDB{
				dbSpanIDs:  open(filepath.Join(dir, "span-ids")),
				dbIDEvt:    open(filepath.Join(dir, "id-event")),
				dbIDSubIDs: open(filepath.Join(dir, "id-subs")),
				dbOwnerIDs: open(filepath.Join(dir, "owner-ids")),
			}
		})
	}
	return eDB
}

func CloseDB() {
	eDB.Lock()
	defer eDB.Unlock()

	if eDB.dbSpanIDs != nil {
		lk.FailOnErr("%v", eDB.dbSpanIDs.Close())
		eDB.dbSpanIDs = nil
	}

	if eDB.dbIDEvt != nil {
		lk.FailOnErr("%v", eDB.dbIDEvt.Close())
		eDB.dbIDEvt = nil
	}

	if eDB.dbIDSubIDs != nil {
		lk.FailOnErr("%v", eDB.dbIDSubIDs.Close())
		eDB.dbIDSubIDs = nil
	}

	if eDB.dbOwnerIDs != nil {
		lk.FailOnErr("%v", eDB.dbOwnerIDs.Close())
		eDB.dbOwnerIDs = nil
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

func SaveEvt(evt *Event) error {
	return eDB.dbIDEvt.Update(func(txn *badger.Txn) error {
		return txn.Set(evt.Marshal())
	})
}

func GetEvt(id string) (evt *Event, err error) {
	eDB.Lock()
	defer eDB.Unlock()

	evt = &Event{}

	return evt, eDB.dbIDEvt.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Seek([]byte(id)); it.Valid() {
			item := it.Item()
			item.Value(func(val []byte) error {
				return evt.Unmarshal(item.Key(), val)
			})
		}
		return nil
	})
}

func SaveEvtSpan() error {
	return eDB.dbSpanIDs.Update(func(txn *badger.Txn) error {
		return txn.Set(Marshal())
	})
}

func ListEvtSpan() error {
	eDB.Lock()
	defer eDB.Unlock()

	return eDB.dbSpanIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			item.Value(func(v []byte) error {
				return Unmarshal(item.Key(), v)
			})
		}
		return nil
	})
}

func FillEvtSpan(ts string) error {
	eDB.Lock()
	defer eDB.Unlock()

	return eDB.dbSpanIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(ts)
		if it.Seek(prefix); it.ValidForPrefix(prefix) {
			item := it.Item()
			item.Value(func(v []byte) error {
				return Unmarshal(item.Key(), v)
			})
		}
		return nil
	})
}

func SaveOwn(own *Own) error {
	return eDB.dbOwnerIDs.Update(func(txn *badger.Txn) error {
		return txn.Set(own.Marshal())
	})
}

func GetOwn(owner, yyyymm string) (own *Own, err error) {
	eDB.Lock()
	defer eDB.Unlock()

	own = &Own{}

	return own, eDB.dbOwnerIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Seek([]byte(owner + "@" + yyyymm)); it.Valid() {
			item := it.Item()
			item.Value(func(val []byte) error {
				return own.Unmarshal(item.Key(), val)
			})
		}
		return nil
	})
}
