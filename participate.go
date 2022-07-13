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
		pType:     pType,
		evtId:     evtId,
		ptps:      []string{},
		fnDbStore: bh.UpsertOneObjectDB[EventParticipate],
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

func (ep *EventParticipate) Unmarshal(dbKey, dbVal []byte) (any, error) {
	dbKeyStr := string(dbKey)
	typeid := strings.Split(dbKeyStr, SEP_K)
	ep.pType = typeid[0]
	ep.evtId = typeid[1]
	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	ep.ptps = strings.Split(dbValStr, " ")
	ep.fnDbStore = bh.UpsertOneObjectDB[EventParticipate]
	return ep, nil
}

func (ep *EventParticipate) BadgerDB() *badger.DB {
	return DbGrp.IDPtps
}

func (ep *EventParticipate) AddPtps(participants ...string) error {
	ep.ptps = append(ep.ptps, participants...)
	ep.ptps = Settify(ep.ptps...)
	return ep.fnDbStore(ep)
}

func (ep *EventParticipate) RmPtps(participants ...string) error {
	FilterFast(&ep.ptps, func(i int, e string) bool {
		return NotIn(e, participants...)
	})
	return ep.fnDbStore(ep)
}

func Participate(evtId, pType string) (*EventParticipate, error) {
	ep := NewEventParticipate(evtId, pType)
	ep, err := bh.GetOneObjectDB[EventParticipate](ep.Key())
	if err != nil {
		return nil, err
	}
	return ep, err
}

func Participants(evtId, pType string) ([]string, error) {
	ep := NewEventParticipate(evtId, pType)
	ep, err := bh.GetOneObjectDB[EventParticipate](ep.Key())
	if err != nil {
		return nil, err
	}
	if ep == nil {
		return []string{}, nil
	}
	return ep.ptps, nil
}
