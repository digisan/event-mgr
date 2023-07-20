package eventmgr

import (
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	lk "github.com/digisan/logkit"
)

type DBGrp struct {
	sync.Mutex
	SpanID     *badger.DB // span     : event ids
	IDEvt      *badger.DB // event id : event content
	MyID       *badger.DB // uname    : self own event ids
	BookmarkID *badger.DB // uname    : bookmarked event ids
	IDFlwID    *badger.DB // event id : follower event ids
	IDPtps     *badger.DB // event id : uname participants
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
				SpanID:     open(filepath.Join(dir, "span-ids")),
				IDEvt:      open(filepath.Join(dir, "id-event")),
				MyID:       open(filepath.Join(dir, "my-ids")),
				BookmarkID: open(filepath.Join(dir, "bookmark-ids")),
				IDFlwID:    open(filepath.Join(dir, "id-flwids")),
				IDPtps:     open(filepath.Join(dir, "id-ptps")),
			}
		})
	}
	return DbGrp
}

func CloseDB() {
	DbGrp.Lock()
	defer DbGrp.Unlock()

	if DbGrp.SpanID != nil {
		lk.FailOnErr("%v", DbGrp.SpanID.Close())
		DbGrp.SpanID = nil
	}
	if DbGrp.IDEvt != nil {
		lk.FailOnErr("%v", DbGrp.IDEvt.Close())
		DbGrp.IDEvt = nil
	}
	if DbGrp.MyID != nil {
		lk.FailOnErr("%v", DbGrp.MyID.Close())
		DbGrp.MyID = nil
	}
	if DbGrp.BookmarkID != nil {
		lk.FailOnErr("%v", DbGrp.BookmarkID.Close())
		DbGrp.BookmarkID = nil
	}
	if DbGrp.IDFlwID != nil {
		lk.FailOnErr("%v", DbGrp.IDFlwID.Close())
		DbGrp.IDFlwID = nil
	}
	if DbGrp.IDPtps != nil {
		lk.FailOnErr("%v", DbGrp.IDPtps.Close())
		DbGrp.IDPtps = nil
	}
}
