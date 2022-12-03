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

func newBookmark(user string) *Bookmark {
	return &Bookmark{
		User:       user,
		EventIDTMs: []string{},
		fnDbStore:  bh.UpsertOneObject[Bookmark],
	}
}

func NewBookmark(user string, useExisting bool) (*Bookmark, error) {
	if bm, err := FetchBookmark(user); err == nil && bm != nil {
		if useExisting {
			return bm, err
		}
		return nil, fmt.Errorf("user <%s> is already existing, cannot be NewBookmark,", user)
	}
	return newBookmark(user), nil
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
	bm.fnDbStore = bh.UpsertOneObject[Bookmark]
	return bm, nil
}

/////////////////////////////////////////////////////////////////////////

func (bm *Bookmark) AddEvent(evtId string) error {
	mtx.Lock()
	defer mtx.Unlock()

	evt, err := FetchEvent(true, evtId)
	if err != nil {
		return err
	}
	if evt == nil {
		return fmt.Errorf("event [%s] is not alive, cannot be bookmarked", evtId)
	}
	idtm := evtId + "@" + evt.Tm.Format("20060102150405")

	bm.EventIDTMs = append(bm.EventIDTMs, idtm)
	bm.EventIDTMs = Settify(bm.EventIDTMs...)
	return bm.fnDbStore(bm)
}

func (bm *Bookmark) RemoveEvent(evtId string) (int, error) {
	mtx.Lock()
	defer mtx.Unlock()

	prevN := len(bm.EventIDTMs)
	FilterFast(&bm.EventIDTMs, func(i int, e string) bool { return !strings.HasPrefix(e, evtId) })
	if err := bm.fnDbStore(bm); err != nil {
		return -1, err
	}
	return prevN - len(bm.EventIDTMs), nil
}

// get all bookmarked events
func (bm *Bookmark) Bookmarks(order string) (bms []string) {
	switch order {
	case "desc":
		sort.SliceStable(bm.EventIDTMs, func(i, j int) bool {
			left, right := bm.EventIDTMs[i], bm.EventIDTMs[j]
			leftTm, rightTm := strs.SplitPart(left, "@", 1), strs.SplitPart(right, "@", 1)
			return leftTm > rightTm
		})
	case "asc":
		sort.SliceStable(bm.EventIDTMs, func(i, j int) bool {
			left, right := bm.EventIDTMs[i], bm.EventIDTMs[j]
			leftTm, rightTm := strs.SplitPart(left, "@", 1), strs.SplitPart(right, "@", 1)
			return leftTm < rightTm
		})
	}
	for _, idtm := range bm.EventIDTMs {
		bms = append(bms, strs.SplitPart(idtm, "@", 0))
	}
	return bms
}

func (bm *Bookmark) HasEvent(evtId string) bool {
	return In(evtId, bm.Bookmarks("")...)
}

func (bm *Bookmark) ToggleEvent(evtId string) (bool, error) {
	if bm.HasEvent(evtId) {
		n, err := bm.RemoveEvent(evtId)
		if err != nil {
			return false, err
		}
		if n != 1 {
			return false, fmt.Errorf("remove event [%s] error", evtId)
		}
		return false, nil
	} else {
		if err := bm.AddEvent(evtId); err != nil {
			return false, err
		}
		return true, nil
	}
}

func FetchBookmark(user string) (*Bookmark, error) {
	bookmark, err := bh.GetOneObject[Bookmark]([]byte(user))
	if err != nil {
		return nil, err
	}
	return bookmark, nil
}
