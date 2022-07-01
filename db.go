package eventmgr

import (
	"path/filepath"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v3"
	. "github.com/digisan/go-generics/v2"
	lk "github.com/digisan/logkit"
)

var (
	onceEDB sync.Once // do once
	eDB     *EDB      // global, for keeping single instance
)

type EDB struct {
	sync.Mutex
	dbSpanIDs  *badger.DB
	dbIDEvt    *badger.DB
	dbOwnerIDs *badger.DB
	dbIDFlwIDs *badger.DB
	dbIDPtps   *badger.DB
}

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
