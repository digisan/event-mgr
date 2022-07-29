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

// Participate a Post, such as thumb

const (
	SEP_K = "^"
	SEP_V = "^"
)

type EventParticipate struct {
	evtId     string   // event id
	pType     string   // thumb, etc
	Ptps      []string // uname
	fnDbStore func(*EventParticipate) error
}

func newEventParticipate(evtId, pType string) *EventParticipate {
	return &EventParticipate{
		evtId:     evtId,
		pType:     pType,
		Ptps:      []string{},
		fnDbStore: bh.UpsertOneObjectDB[EventParticipate],
	}
}

func NewEventParticipate(evtId, pType string, useExisting bool) (*EventParticipate, error) {
	lk.FailOnErrWhen(strings.Contains(evtId, SEP_K), "%v", fmt.Errorf("invalid symbol(%s) in evtId", SEP_K))
	lk.FailOnErrWhen(strings.Contains(pType, SEP_K), "%v", fmt.Errorf("invalid symbol(%s) in pType", SEP_K))
	if p, err := Participate(evtId, pType); err == nil && p != nil {
		if useExisting {
			return p, err
		}
		return nil, fmt.Errorf("<%s> is already existing, cannot be New,", evtId)
	}
	return newEventParticipate(evtId, pType), nil
}

func (ep EventParticipate) String() string {
	sb := strings.Builder{}
	sb.WriteString("EventID: " + ep.evtId + "\n")
	sb.WriteString("Type: " + ep.pType + "\n")
	sb.WriteString("Participants:")
	for _, p := range ep.Ptps {
		sb.WriteString("\n  " + p)
	}
	return sb.String()
}

func (ep *EventParticipate) Key() []byte {
	return []byte(fmt.Sprintf("%s%s%s", ep.evtId, SEP_K, ep.pType))
}

func (ep *EventParticipate) Marshal(at any) (forKey, forValue []byte) {
	forKey = ep.Key()
	lk.FailOnErrWhen(len(forKey) == 0, "%v", errors.New("empty event for participants"))
	forValue = []byte(fmt.Sprint(ep.Ptps))
	return
}

func (ep *EventParticipate) Unmarshal(dbKey, dbVal []byte) (any, error) {
	dbKeyStr := string(dbKey)
	typeid := strings.Split(dbKeyStr, SEP_K)
	ep.evtId = typeid[0]
	ep.pType = typeid[1]

	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	dbValStr = strings.TrimSpace(dbValStr)
	ep.Ptps = IF(len(dbValStr) > 0, strings.Split(dbValStr, " "), []string{})

	ep.fnDbStore = bh.UpsertOneObjectDB[EventParticipate]
	return ep, nil
}

func (ep *EventParticipate) BadgerDB() *badger.DB {
	return DbGrp.IDPtps
}

/////////////////////////////////////////////////////////////////////////////

func (ep *EventParticipate) AddPtps(participants ...string) error {
	if !EventIsAlive(ep.evtId) {
		return fmt.Errorf("<%s> is not alive, cannot add participants", ep.evtId)
	}
	ep.Ptps = append(ep.Ptps, participants...)
	ep.Ptps = Settify(ep.Ptps...)
	return ep.fnDbStore(ep)
}

func (ep *EventParticipate) RmPtps(participants ...string) error {
	if !EventIsAlive(ep.evtId) {
		return fmt.Errorf("<%s> is not alive, cannot remove participants", ep.evtId)
	}
	FilterFast(&ep.Ptps, func(i int, e string) bool {
		return NotIn(e, participants...)
	})
	return ep.fnDbStore(ep)
}

func (ep *EventParticipate) HasPtp(participant string) (bool, error) {
	ptps, err := Participants(ep.evtId, ep.pType)
	if err != nil {
		return false, err
	}
	return In(participant, ptps...), nil
}

func (ep *EventParticipate) TogglePtp(participant string) (bool, error) {
	hasPtp, err := ep.HasPtp(participant)
	if err != nil {
		return false, err
	}
	if hasPtp {
		if err := ep.RmPtps(participant); err != nil {
			return false, err
		}
		return false, nil
	} else {
		if err := ep.AddPtps(participant); err != nil {
			return false, err
		}
		return true, nil
	}
}

func Participate(evtId, pType string) (*EventParticipate, error) {
	if !EventIsAlive(evtId) {
		return nil, fmt.Errorf("<%s> is not alive, its participate cannot be fetched", evtId)
	}
	ep := newEventParticipate(evtId, pType)
	ep, err := bh.GetOneObjectDB[EventParticipate](ep.Key())
	if err != nil {
		return nil, err
	}
	return ep, err
}

func Participants(evtId, pType string) ([]string, error) {
	if !EventIsAlive(evtId) {
		return nil, fmt.Errorf("<%s> is not alive, its participants cannot be fetched", evtId)
	}
	ep := newEventParticipate(evtId, pType)
	ep, err := bh.GetOneObjectDB[EventParticipate](ep.Key())
	if err != nil {
		return nil, err
	}
	if ep == nil {
		return []string{}, nil
	}
	return ep.Ptps, nil
}

func deleteParticipate(evtid string) (int, error) {
	return bh.DeleteObjectsDB[EventParticipate]([]byte(evtid))
}
