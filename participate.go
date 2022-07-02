package eventmgr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	. "github.com/digisan/go-generics/v2"
	lk "github.com/digisan/logkit"
)

const (
	SEP_K = "^"
	SEP_V = "^"
)

type EventParticipate struct {
	pType     string
	evtId     string
	ptps      []string
	fnDbStore func(*EventParticipate) error
}

func NewEventParticipate(evtId, pType string) *EventParticipate {
	lk.FailOnErrWhen(strings.Contains(pType, SEP_K), "%v", fmt.Errorf("invalid symbol(%s) in pType", SEP_K))
	lk.FailOnErrWhen(strings.Contains(evtId, SEP_K), "%v", fmt.Errorf("invalid symbol(%s) in evtId", SEP_K))
	return &EventParticipate{
		pType: pType,
		evtId: evtId,
		ptps:  []string{},
	}
}

func (ep EventParticipate) String() string {
	sb := strings.Builder{}
	sb.WriteString("Type: " + ep.pType + "\n")
	sb.WriteString("EventID: " + ep.evtId + "\n")
	sb.WriteString("Participants:")
	for _, p := range ep.ptps {
		sb.WriteString("\n  " + p)
	}
	return sb.String()
}

func (ep *EventParticipate) Key() []byte {
	return []byte(fmt.Sprintf("%s%s%s", ep.pType, SEP_K, ep.evtId))
}

func (ep *EventParticipate) Marshal(at any) (forKey, forValue []byte) {
	forKey = ep.Key()
	lk.FailOnErrWhen(len(forKey) == 0, "%v", errors.New("empty event for participants"))
	forValue = []byte(fmt.Sprint(ep.ptps))
	return
}

func (ep *EventParticipate) Unmarshal(dbKey, dbVal []byte) error {
	dbKeyStr := string(dbKey)
	typeid := strings.Split(dbKeyStr, SEP_K)
	ep.pType = typeid[0]
	ep.evtId = typeid[1]
	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	ep.ptps = strings.Split(dbValStr, " ")
	return nil
}

func (ep *EventParticipate) BadgerDB() *badger.DB {
	return eDB.dbIDPtps
}

func (ep *EventParticipate) OnDbStore(dbStore func(*EventParticipate) error) {
	ep.fnDbStore = dbStore
}

func (ep *EventParticipate) AddPtps(participants ...string) error {
	ep.ptps = append(ep.ptps, participants...)
	ep.ptps = Settify(ep.ptps...)
	lk.FailOnErrWhen(ep.fnDbStore == nil, "%v", errors.New("EventParticipate [fnDbStore] is nil"))
	if err := ep.fnDbStore(ep); err != nil {
		return err
	}
	return nil
}

func (ep *EventParticipate) RmPtps(participants ...string) error {
	FilterFast(&ep.ptps, func(i int, e string) bool {
		return NotIn(e, participants...)
	})
	lk.FailOnErrWhen(ep.fnDbStore == nil, "%v", errors.New("EventParticipate [fnDbStore] is nil"))
	if err := ep.fnDbStore(ep); err != nil {
		return err
	}
	return nil
}

func GetParticipate(evtId, pType string) (*EventParticipate, error) {
	ep := NewEventParticipate(evtId, pType)
	ep, err := GetOneObjectDB[EventParticipate](ep.Key())
	if err != nil {
		return nil, err
	}
	return ep, err
}

func GetParticipants(evtId, pType string) ([]string, error) {
	ep := NewEventParticipate(evtId, pType)
	ep, err := GetOneObjectDB[EventParticipate](ep.Key())
	if err != nil {
		return nil, err
	}
	if ep == nil {
		return []string{}, nil
	}
	return ep.ptps, nil
}
