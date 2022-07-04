package eventmgr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	. "github.com/digisan/go-generics/v2"
	lk "github.com/digisan/logkit"
)

// key:   Owner@YYYYMM-27582250-1
// value: EventIDs ([uuid])
type Own struct {
	OwnerYMSpan string           // uname@202206-27582250-1
	EventIDs    []string         // [uuid]
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
	return eDB.dbOwnerIDs
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
	own.EventIDs = strings.Split(dbValStr, " ")
	return own, nil
}

func (own *Own) OnDbStore(dbStore func(*Own) error) {
	own.fnDbStore = dbStore
}

func updateOwn(span string, tmpEvts ...TempEvt) error {
	mOwnerEventIDs := make(map[string][]string)
	for _, evt := range tmpEvts {
		key := evt.owner + "@" + evt.yyyymm + "-" + span
		mOwnerEventIDs[key] = append(mOwnerEventIDs[key], evt.evtId)
	}
	for owner, ids := range mOwnerEventIDs {
		own := &Own{
			OwnerYMSpan: owner,
			EventIDs:    ids,
			fnDbStore:   UpsertOneObjectDB[Own],
		}
		if err := own.fnDbStore(own); err != nil {
			return err
		}
	}
	return nil
}

func FetchOwn(owner, yyyymm string) ([]string, error) {
	objects, err := GetObjectsDB[Own]([]byte(owner + "@" + yyyymm + "-"))
	if err != nil {
		return nil, err
	}
	keys := FilterMap(objects, nil, func(i int, e *Own) string {
		return e.OwnerYMSpan
	})

	rtIds := []string{}
	for _, key := range keys {
		own, err := GetOneObjectDB[Own]([]byte(key))
		if err != nil {
			return nil, err
		}
		rtIds = append(rtIds, own.EventIDs...)
	}

	return Settify(rtIds...), nil
}
