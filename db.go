package eventmgr

import (
	"path/filepath"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v3"
	. "github.com/digisan/go-generics/v2"
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

func SaveEvtDB(evt *Event) error {
	return eDB.dbIDEvt.Update(func(txn *badger.Txn) error {
		return txn.Set(evt.Marshal())
	})
}

func GetEvtDB(id string) (evt *Event, err error) {
	eDB.Lock()
	defer eDB.Unlock()

	evt = &Event{}

	return evt, eDB.dbIDEvt.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Seek([]byte(id)); it.Valid() {
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				return evt.Unmarshal(item.Key(), val)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func SaveEvtSpanDB(span string) error {
	return eDB.dbSpanIDs.Update(func(txn *badger.Txn) error {
		return txn.Set(MarshalAt(span))
	})
}

func GetSpanAllDB() ([]string, error) {
	eDB.Lock()
	defer eDB.Unlock()

	spans := []string{}
	return spans, eDB.dbSpanIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			spans = append(spans, string(item.Key()))
		}
		return nil
	})
}

func GetEvtIdAllDB() ([]string, error) {
	eDB.Lock()
	defer eDB.Unlock()

	var idsGrp [][]string
	err := eDB.dbSpanIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if err := item.Value(func(v []byte) error {
				if sv := string(v); strings.Contains(sv, SEP) {
					idsGrp = append(idsGrp, strings.Split(sv, SEP))
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return MergeArray(idsGrp...), err
}

func GetEvtIdRangeDB(ts string) ([]string, error) {
	eDB.Lock()
	defer eDB.Unlock()

	var ids []string

	return ids, eDB.dbSpanIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(ts)
		if it.Seek(prefix); it.ValidForPrefix(prefix) {
			item := it.Item()
			if err := item.Value(func(v []byte) error {
				if sv := string(v); strings.Contains(sv, SEP) {
					ids = strings.Split(sv, SEP)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func SaveOwnDB(own *Own) error {
	return eDB.dbOwnerIDs.Update(func(txn *badger.Txn) error {
		return txn.Set(own.Marshal())
	})
}

func GetOwnDB(key string) (*Own, error) {
	eDB.Lock()
	defer eDB.Unlock()

	rtOwn := &Own{}

	return rtOwn, eDB.dbOwnerIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Seek([]byte(key)); it.Valid() {
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				return rtOwn.Unmarshal(item.Key(), val)
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// return rtOwn, eDB.dbOwnerIDs.View(func(txn *badger.Txn) error {
	// 	opts := badger.DefaultIteratorOptions
	// 	it := txn.NewIterator(opts)
	// 	defer it.Close()

	// 	prefix := []byte(owner + "@" + yyyymm + "-")
	// 	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
	// 		one := &Own{}
	// 		item := it.Item()
	// 		if err := item.Value(func(val []byte) error {
	// 			return one.Unmarshal(item.Key(), val)
	// 		}); err != nil {
	// 			return err
	// 		}
	// 		// join each one.EventIDs
	// 		rtOwn.EventIDs = append(rtOwn.EventIDs, one.EventIDs...)
	// 	}
	// 	return nil
	// })
}

func GetOwnKeysDB(owner, yyyymm string) ([]string, error) {
	eDB.Lock()
	defer eDB.Unlock()

	rtSpans := []string{}

	return rtSpans, eDB.dbOwnerIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(owner + "@" + yyyymm + "-")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			rtSpans = append(rtSpans, string(it.Item().Key()))
		}
		return nil
	})
}
