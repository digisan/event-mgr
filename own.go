package eventmgr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	bh "github.com/digisan/db-helper/badger"
	. "github.com/digisan/go-generics"
	lk "github.com/digisan/logkit"
)

// key:   Owner@YYYYMM-27582250-1
// value: EventIDs ([uuid])
type Own struct {
	OwnerYMSpan string           // uname@202206-27582250-1
	EventIDs    []string         // [uuid], owner self events
	fnDbStore   func(*Own) error // in db.go
}

func (own Own) String() string {
	sb := strings.Builder{}
	sb.WriteString(own.OwnerYMSpan + "\n")
	for i, id := range own.EventIDs {
		sb.WriteString(fmt.Sprintf("\t%02d\t%s\n", i, id))
	}
	return sb.String()
}

func (own *Own) BadgerDB() *badger.DB {
	return DbGrp.MyID
}

func (own *Own) Key() []byte {
	return []byte(own.OwnerYMSpan)
}

func (own *Own) Marshal(at any) (forKey, forValue []byte) {
	lk.FailOnErrWhen(len(own.OwnerYMSpan) == 0, "%v", errors.New("empty owner"))
	forKey = own.Key()
	forValue = []byte(fmt.Sprint(own.EventIDs))
	return
}

func (own *Own) Unmarshal(dbKey, dbVal []byte) (any, error) {
	own.OwnerYMSpan = string(dbKey)
	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	dbValStr = strings.TrimSpace(dbValStr)
	own.EventIDs = IF(len(dbValStr) > 0, strings.Split(dbValStr, " "), []string{})
	own.fnDbStore = bh.UpsertOneObject[Own]
	return own, nil
}

func streamUpdateOwn(span string, tmpEvts ...TempEvt) error {
	mOwnerEventIDs := make(map[string][]string)
	for _, evt := range tmpEvts {
		key := evt.owner + "@" + evt.yyyymm + "-" + span
		mOwnerEventIDs[key] = append(mOwnerEventIDs[key], evt.evtID)
	}
	for owner, ids := range mOwnerEventIDs {
		own := &Own{
			OwnerYMSpan: owner,
			EventIDs:    ids,
			fnDbStore:   bh.UpsertOneObject[Own],
		}
		if err := own.fnDbStore(own); err != nil {
			return err
		}
	}
	return nil
}

func FetchOwn(owner, yyyymm string) ([]string, error) {
	objects, err := bh.GetObjects[Own]([]byte(owner+"@"+yyyymm+"-"), nil)
	if err != nil {
		return nil, err
	}
	keys := FilterMap(objects, nil, func(i int, e *Own) string {
		return e.OwnerYMSpan
	})

	rtIds := []string{}
	for _, key := range keys {
		own, err := bh.GetOneObject[Own]([]byte(key))
		if err != nil {
			return nil, err
		}
		rtIds = append(rtIds, own.EventIDs...)
	}

	return Settify(rtIds...), nil
}

func deleteOwn(owner, yyyymm, span, id string) (int, error) {
	key := fmt.Sprintf("%s@%s-%s", owner, yyyymm, span)
	own, err := bh.GetOneObject[Own]([]byte(key))
	if err != nil {
		return -1, err
	}
	if own == nil {
		return 0, err
	}
	prevN := len(own.EventIDs)
	FilterFast(&own.EventIDs, func(i int, e string) bool { return e != id })
	if err := bh.UpsertOneObject(own); err != nil {
		return -1, err
	}
	return prevN - len(own.EventIDs), nil
}
