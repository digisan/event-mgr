package eventmgr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	. "github.com/digisan/go-generics/v2"
	lk "github.com/digisan/logkit"
)

type EventFollow struct {
	evtFlwee  string
	evtFlwers []string
	fnDbStore func(*EventFollow) error
}

func NewEventFollow(followee string) *EventFollow {
	return &EventFollow{
		evtFlwee:  followee,
		evtFlwers: []string{},
	}
}

func (ef EventFollow) String() string {
	sb := strings.Builder{}
	sb.WriteString("followee: " + ef.evtFlwee + "\n")
	sb.WriteString("follower: ")
	for _, f := range ef.evtFlwers {
		sb.WriteString("\n  " + f)
	}
	return sb.String()
}

func (ef *EventFollow) Key() []byte {
	return []byte(ef.evtFlwee)
}

func (ef *EventFollow) Marshal() (forKey, forValue []byte) {
	lk.FailOnErrWhen(len(ef.Key()) == 0, "%v", errors.New("empty followee"))
	forKey = []byte(ef.evtFlwee)
	forValue = []byte(fmt.Sprint(ef.evtFlwers))
	return
}

func (ef *EventFollow) Unmarshal(dbKey, dbVal []byte) error {
	ef.evtFlwee = string(dbKey)
	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	ef.evtFlwers = strings.Split(dbValStr, " ")
	return nil
}

func (ef *EventFollow) BadgerDB() *badger.DB {
	return eDB.dbIDFlwIDs
}

func (ef *EventFollow) OnDbStore(dbStore func(*EventFollow) error) {
	ef.fnDbStore = dbStore
}

func (ef *EventFollow) AddFollower(followers ...string) error {
	ef.evtFlwers = append(ef.evtFlwers, followers...)
	ef.evtFlwers = Settify(ef.evtFlwers...)
	if ef.fnDbStore == nil {
		return errors.New("EventFollow [fnDbStore] is nil")
	}
	if err := ef.fnDbStore(ef); err != nil {
		return err
	}
	return nil
}

func (ef *EventFollow) RmFollower(followers ...string) error {
	FilterFast(&ef.evtFlwers, func(i int, e string) bool {
		return NotIn(e, followers...)
	})
	if ef.fnDbStore == nil {
		return errors.New("EventFollow [fnDbStore] is nil")
	}
	if err := ef.fnDbStore(ef); err != nil {
		return err
	}
	return nil
}

func GetFollowers(flwee string) ([]string, error) {
	ef, err := GetFlwDB(flwee)
	if err != nil {
		return nil, err
	}
	if ef == nil {
		return []string{}, nil
	}
	return ef.evtFlwers, nil
}
