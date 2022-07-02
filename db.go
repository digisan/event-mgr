package eventmgr

import (
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v3"
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
		opt.Logger = nil
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
