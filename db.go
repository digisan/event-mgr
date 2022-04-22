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
	dbSpanIDs  *badger.DB
	dbIDEvt    *badger.DB
	dbIDSubIDs *badger.DB
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
				dbSpanIDs:  open(filepath.Join(dir, "span-ids")),
				dbIDEvt:    open(filepath.Join(dir, "id-event")),
				dbIDSubIDs: open(filepath.Join(dir, "id-subs")),
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

	if db.dbIDEvt != nil {
		lk.FailOnErr("%v", db.dbIDEvt.Close())
		db.dbIDEvt = nil
	}

	if db.dbIDSubIDs != nil {
		lk.FailOnErr("%v", db.dbIDSubIDs.Close())
		db.dbIDSubIDs = nil
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

func (db *EDB) SaveEvt(evt *Event, lock bool) error {
	if lock {
		db.Lock()
		defer db.Unlock()
	}
	return db.dbIDEvt.Update(func(txn *badger.Txn) error {
		return txn.Set(evt.Marshal())
	})
}

func (db *EDB) GetEvt(id string) (evt *Event, err error) {
	db.Lock()
	defer db.Unlock()

	evt = &Event{}

	return evt, db.dbIDEvt.View(func(txn *badger.Txn) error {
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

func (db *EDB) SaveEvtSpan(es *EventSpan, lock bool) error {
	if lock {
		db.Lock()
		defer db.Unlock()
	}
	return db.dbSpanIDs.Update(func(txn *badger.Txn) error {
		return txn.Set(es.Marshal())
	})
}

func (db *EDB) ListEvtSpan() (es *EventSpan, err error) {
	db.Lock()
	defer db.Unlock()

	es = &EventSpan{}

	return es, db.dbSpanIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			item.Value(func(v []byte) error {
				return es.Unmarshal(item.Key(), v)
			})
		}
		return nil
	})
}

func (db *EDB) GetEvtSpan(ts string) (es *EventSpan, err error) {
	db.Lock()
	defer db.Unlock()

	es = &EventSpan{}

	return es, db.dbSpanIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(ts)
		if it.Seek(prefix); it.ValidForPrefix(prefix) {
			item := it.Item()
			item.Value(func(v []byte) error {
				return es.Unmarshal(item.Key(), v)
			})
		}
		return nil
	})
}
