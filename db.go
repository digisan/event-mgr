package eventmgr

import (
	"bytes"
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
	dbOwnerIDs *badger.DB
	dbIDFlwIDs *badger.DB
	dbIDPtps   *badger.DB
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
				dbOwnerIDs: open(filepath.Join(dir, "owner-ids")),
				dbIDFlwIDs: open(filepath.Join(dir, "id-flwids")),
				dbIDPtps:   open(filepath.Join(dir, "id-ptps")),
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
	if eDB.dbOwnerIDs != nil {
		lk.FailOnErr("%v", eDB.dbOwnerIDs.Close())
		eDB.dbOwnerIDs = nil
	}
	if eDB.dbIDFlwIDs != nil {
		lk.FailOnErr("%v", eDB.dbIDFlwIDs.Close())
		eDB.dbIDFlwIDs = nil
	}
	if eDB.dbIDPtps != nil {
		lk.FailOnErr("%v", eDB.dbIDPtps.Close())
		eDB.dbIDPtps = nil
	}
}

/////////////////////////////////////////////////////////////////////////

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
	eDB.Lock()
	defer eDB.Unlock()

	return eDB.dbIDEvt.Update(func(txn *badger.Txn) error {
		return txn.Set(evt.Marshal())
	})
}

func GetEvtDB(id string) (*Event, error) {
	eDB.Lock()
	defer eDB.Unlock()

	evt := &Event{}
	err := eDB.dbIDEvt.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		bytesKey := []byte(id)
		if it.Seek(bytesKey); it.Valid() {
			if item := it.Item(); bytes.Equal(bytesKey, item.Key()) {
				if err := item.Value(func(val []byte) error {
					return evt.Unmarshal(item.Key(), val)
				}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if len(evt.ID) == 0 {
		return nil, err
	}
	return evt, err
}

func SaveEvtSpanDB(span string) error {
	eDB.Lock()
	defer eDB.Unlock()

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
				if sv := string(v); len(sv) > 0 {
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
				if sv := string(v); len(sv) > 0 {
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
	eDB.Lock()
	defer eDB.Unlock()

	return eDB.dbOwnerIDs.Update(func(txn *badger.Txn) error {
		return txn.Set(own.Marshal())
	})
}

func GetOwnDB(ownerYMSpan string) (*Own, error) {
	eDB.Lock()
	defer eDB.Unlock()

	rtOwn := &Own{}
	err := eDB.dbOwnerIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		bytesKey := []byte(ownerYMSpan)
		if it.Seek(bytesKey); it.Valid() {
			if item := it.Item(); bytes.Equal(bytesKey, item.Key()) {
				if err := item.Value(func(val []byte) error {
					return rtOwn.Unmarshal(item.Key(), val)
				}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if len(rtOwn.OwnerYMSpan) == 0 {
		return nil, err
	}
	return rtOwn, err

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

func GetOwnSpanKeysDB(owner, yyyymm string) ([]string, error) {
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

func SaveFlwDB(ef *EventFollow) error {
	eDB.Lock()
	defer eDB.Unlock()

	return eDB.dbIDFlwIDs.Update(func(txn *badger.Txn) error {
		return txn.Set(ef.Marshal())
	})
}

func GetFlwDB(FlweeID string) (*EventFollow, error) {
	eDB.Lock()
	defer eDB.Unlock()

	rtEvtFlw := &EventFollow{}
	err := eDB.dbIDFlwIDs.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		bytesKey := []byte(FlweeID)
		if it.Seek(bytesKey); it.Valid() {
			if item := it.Item(); bytes.Equal(bytesKey, item.Key()) {
				if err := item.Value(func(val []byte) error {
					return rtEvtFlw.Unmarshal(item.Key(), val)
				}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if len(rtEvtFlw.evtFlwee) == 0 {
		return nil, err
	}
	return rtEvtFlw, err
}
