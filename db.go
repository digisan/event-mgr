package eventmgr

import (
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v3"
	lk "github.com/digisan/logkit"
)

type DBGrp struct {
	sync.Mutex
	SpanIDs  *badger.DB
	IDEvt    *badger.DB
	OwnerIDs *badger.DB
	IDFlwIDs *badger.DB
	IDPtps   *badger.DB
}

var (
	onceDB sync.Once // do once
	DbGrp  *DBGrp    // global, for keeping single instance
)

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
func InitDB(dir string) *DBGrp {
	if DbGrp == nil {
		onceDB.Do(func() {
			DbGrp = &DBGrp{
				SpanIDs:  open(filepath.Join(dir, "span-ids")),
				IDEvt:    open(filepath.Join(dir, "id-event")),
				OwnerIDs: open(filepath.Join(dir, "owner-ids")),
				IDFlwIDs: open(filepath.Join(dir, "id-flwids")),
				IDPtps:   open(filepath.Join(dir, "id-ptps")),
			}
		})
	}
	return DbGrp
}

func CloseDB() {
	DbGrp.Lock()
	defer DbGrp.Unlock()

	if DbGrp.SpanIDs != nil {
		lk.FailOnErr("%v", DbGrp.SpanIDs.Close())
		DbGrp.SpanIDs = nil
	}
	if DbGrp.IDEvt != nil {
		lk.FailOnErr("%v", DbGrp.IDEvt.Close())
		DbGrp.IDEvt = nil
	}
	if DbGrp.OwnerIDs != nil {
		lk.FailOnErr("%v", DbGrp.OwnerIDs.Close())
		DbGrp.OwnerIDs = nil
	}
	if DbGrp.IDFlwIDs != nil {
		lk.FailOnErr("%v", DbGrp.IDFlwIDs.Close())
		DbGrp.IDFlwIDs = nil
	}
	if DbGrp.IDPtps != nil {
		lk.FailOnErr("%v", DbGrp.IDPtps.Close())
		DbGrp.IDPtps = nil
	}
}
