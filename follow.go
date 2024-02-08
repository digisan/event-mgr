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

// Follow a Post

type EventFollow struct {
	evtFlwee  string   // this event id
	evtFlwers []string // follower event ids
	fnDbStore func(*EventFollow) error
}

func newEventFollow(flwee string) *EventFollow {
	return &EventFollow{
		evtFlwee:  flwee,
		evtFlwers: []string{},
		fnDbStore: bh.UpsertOneObject[EventFollow],
	}
}

func NewEventFollow(flwee string, useExisting bool) (*EventFollow, error) {
	if flw, err := FetchFollow(flwee); err == nil && flw != nil {
		if useExisting {
			return flw, err
		}
		return nil, fmt.Errorf("<%s> is already existing, cannot be New", flwee)
	}
	return newEventFollow(flwee), nil
}

func (ef EventFollow) String() string {
	sb := strings.Builder{}
	sb.WriteString("followee: " + ef.evtFlwee + "\n")
	sb.WriteString("follower:")
	for _, f := range ef.evtFlwers {
		sb.WriteString("\n  " + f)
	}
	return sb.String()
}

func (ef *EventFollow) Key() []byte {
	return []byte(ef.evtFlwee)
}

func (ef *EventFollow) Marshal(at any) (forKey, forValue []byte) {
	forKey = ef.Key()
	lk.FailOnErrWhen(len(forKey) == 0, "%v", errors.New("empty followee"))
	forValue = []byte(fmt.Sprint(ef.evtFlwers))
	return
}

func (ef *EventFollow) Unmarshal(dbKey, dbVal []byte) (any, error) {
	ef.evtFlwee = string(dbKey)
	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	dbValStr = strings.TrimSpace(dbValStr)
	ef.evtFlwers = IF(len(dbValStr) > 0, strings.Split(dbValStr, " "), []string{})
	ef.fnDbStore = bh.UpsertOneObject[EventFollow]
	return ef, nil
}

func (ef *EventFollow) BadgerDB() *badger.DB {
	return DbGrp.IDFlwID
}

/////////////////////////////////////////////////////////////////////////////

func (ef *EventFollow) AddFollower(followers ...string) error {
	mtx.Lock()
	defer mtx.Unlock()

	if !EventHappened(ef.evtFlwee) {
		return fmt.Errorf("<%s> is not existing, cannot add followers", ef.evtFlwee)
	}
	ef.evtFlwers = append(ef.evtFlwers, followers...)
	ef.evtFlwers = Settify(ef.evtFlwers...)
	if err := ef.fnDbStore(ef); err != nil {
		return err
	}
	return nil
}

func (ef *EventFollow) RmFollower(followers ...string) (int, error) {
	mtx.Lock()
	defer mtx.Unlock()

	if !EventHappened(ef.evtFlwee) {
		return -1, fmt.Errorf("<%s> is not existing, cannot remove followers", ef.evtFlwee)
	}
	prevN := len(ef.evtFlwers)
	FilterFast(&ef.evtFlwers, func(i int, e string) bool { return NotIn(e, followers...) })
	if err := ef.fnDbStore(ef); err != nil {
		return -1, err
	}
	return prevN - len(ef.evtFlwers), nil
}

func FetchFollow(flwee string) (*EventFollow, error) {
	mtx.Lock()
	defer mtx.Unlock()

	if !EventHappened(flwee) {
		return nil, fmt.Errorf("<%s> is not existing, cannot be fetched", flwee)
	}
	return bh.GetOneObject[EventFollow]([]byte(flwee))
}

func Followers(flwee string) ([]string, error) {
	ef, err := FetchFollow(flwee)
	if err != nil {
		return nil, err
	}
	if ef == nil {
		return []string{}, nil
	}
	return ef.evtFlwers, nil
}

func deleteEventFollow(flwee string) (int, error) {
	mtx.Lock()
	defer mtx.Unlock()

	return bh.DeleteOneObject[EventFollow]([]byte(flwee))
}
