package eventmgr

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/dgraph-io/badger/v3"
	bh "github.com/digisan/db-helper/badger"
	. "github.com/digisan/go-generics/v2"
	"github.com/digisan/gotk/strs"
	lk "github.com/digisan/logkit"
)

// key:    User (uname)
// value:  EventIDs ([uuid])
type Bookmark struct {
	User       string                // uname
	EventIDTMs []string              // [uuid@event-time], picked from all events
	fnDbStore  func(*Bookmark) error // in db.go
}

func (bm Bookmark) String() string {
	sb := strings.Builder{}
	sb.WriteString(bm.User + "\n")
	for i, idtm := range bm.EventIDTMs {
		id := strs.SplitPart(idtm, "@", 0)
		sb.WriteString(fmt.Sprintf("\t%02d\t%s\n", i, id))
	}
	return sb.String()
}

func (bm *Bookmark) BadgerDB() *badger.DB {
	return DbGrp.BookmarkIDs
}

func (bm *Bookmark) Key() []byte {
	return []byte(bm.User)
}

func (bm *Bookmark) Marshal(at any) (forKey, forValue []byte) {
	lk.FailOnErrWhen(len(bm.User) == 0, "%v", errors.New("empty bookmark user"))
	forKey = bm.Key()
	forValue = []byte(fmt.Sprint(bm.EventIDTMs))
	return
}

func (bm *Bookmark) Unmarshal(dbKey, dbVal []byte) (any, error) {
	bm.User = string(dbKey)
	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	dbValStr = strings.TrimSpace(dbValStr)
	bm.EventIDTMs = IF(len(dbValStr) > 0, strings.Split(dbValStr, " "), []string{})
	bm.fnDbStore = bh.UpsertOneObjectDB[Bookmark]
	return bm, nil
}

func AddBookmark(user, evtId string) error {
	mtx.Lock()
	defer mtx.Unlock()

	bookmark, err := bh.GetOneObjectDB[Bookmark]([]byte(user))
	if err != nil {
		return err
	}

	evt, err := FetchEvent(true, evtId)
	if err != nil {
		return err
	}
	if evt == nil {
		return fmt.Errorf("event [%s] is not alive, cannot be bookmarked", evtId)
	}
	idtm := evtId + "@" + evt.Tm.Format("20060102150405")

	if bookmark == nil {
		bookmark = &Bookmark{
			User:       user,
			EventIDTMs: []string{idtm},
			fnDbStore:  bh.UpsertOneObjectDB[Bookmark],
		}
		return bookmark.fnDbStore(bookmark)
	}

	bookmark.EventIDTMs = append(bookmark.EventIDTMs, idtm)
	bookmark.EventIDTMs = Settify(bookmark.EventIDTMs...)
	return bookmark.fnDbStore(bookmark)
}

func RemoveBookmark(user, evtId string) (int, error) {
	mtx.Lock()
	defer mtx.Unlock()

	bookmark, err := bh.GetOneObjectDB[Bookmark]([]byte(user))
	if err != nil {
		return -1, err
	}
	if bookmark == nil {
		return 0, nil
	}

	prevN := len(bookmark.EventIDTMs)
	FilterFast(&bookmark.EventIDTMs, func(i int, e string) bool { return !strings.HasPrefix(e, evtId) })
	if err := bh.UpsertOneObjectDB(bookmark); err != nil {
		return -1, err
	}
	return prevN - len(bookmark.EventIDTMs), nil
}

func FetchBookmark(user, order string) ([]string, error) {
	bookmark, err := bh.GetOneObjectDB[Bookmark]([]byte(user))
	if err != nil {
		return nil, err
	}
	if bookmark == nil {
		return []string{}, nil
	}

	evtIDs := []string{}
	switch order {
	case "desc":
		sort.SliceStable(bookmark.EventIDTMs, func(i, j int) bool {
			left, right := bookmark.EventIDTMs[i], bookmark.EventIDTMs[j]
			leftTm, rightTm := strs.SplitPart(left, "@", 1), strs.SplitPart(right, "@", 1)
			return leftTm > rightTm
		})
	case "asc":
		sort.SliceStable(bookmark.EventIDTMs, func(i, j int) bool {
			left, right := bookmark.EventIDTMs[i], bookmark.EventIDTMs[j]
			leftTm, rightTm := strs.SplitPart(left, "@", 1), strs.SplitPart(right, "@", 1)
			return leftTm < rightTm
		})
	}
	for _, idtm := range bookmark.EventIDTMs {
		evtIDs = append(evtIDs, strs.SplitPart(idtm, "@", 0))
	}
	return evtIDs, nil
}
