package eventmgr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	bh "github.com/digisan/db-helper/badger"
	. "github.com/digisan/go-generics/v2"
	lk "github.com/digisan/logkit"
)

// Follow a Post

type EventFollow struct {
	evtFlwee  string
	evtFlwers []string
	fnDbStore func(*EventFollow) error
}

func newEventFollow(flwee string) *EventFollow {
	return &EventFollow{
		evtFlwee:  flwee,
		evtFlwers: []string{},
		fnDbStore: bh.UpsertOneObjectDB[EventFollow],
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
	ef.fnDbStore = bh.UpsertOneObjectDB[EventFollow]
	return ef, nil
}

func (ef *EventFollow) BadgerDB() *badger.DB {
	return DbGrp.IDFlwIDs
}

/////////////////////////////////////////////////////////////////////////////

func (ef *EventFollow) AddFollower(followers ...string) error {
	if !EventIsAlive(ef.evtFlwee) {
		return fmt.Errorf("<%s> is not alive, cannot add followers", ef.evtFlwee)
	}
	ef.evtFlwers = append(ef.evtFlwers, followers...)
	ef.evtFlwers = Settify(ef.evtFlwers...)
	if err := ef.fnDbStore(ef); err != nil {
		return err
	}
	return nil
}

func (ef *EventFollow) RmFollower(followers ...string) error {
	if !EventIsAlive(ef.evtFlwee) {
		return fmt.Errorf("<%s> is not alive, cannot remove followers", ef.evtFlwee)
	}
	FilterFast(&ef.evtFlwers, func(i int, e string) bool {
		return NotIn(e, followers...)
	})
	if err := ef.fnDbStore(ef); err != nil {
		return err
	}
	return nil
}

func FetchFollow(flwee string) (*EventFollow, error) {
	if !EventIsAlive(flwee) {
		return nil, fmt.Errorf("<%s> is not alive, cannot be fetched", flwee)
	}
	return bh.GetOneObjectDB[EventFollow]([]byte(flwee))
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
	return bh.DeleteOneObjectDB[EventFollow]([]byte(flwee))
}
