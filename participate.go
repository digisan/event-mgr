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

// Participate a Post, such as thumb, vote etc

const (
	SEP_V_Category    = "##"
	SEP_V_Participant = "^^"
	SEP_V_Map         = "::"
)

type EventParticipate struct {
	evtID     string              // K: event id
	mCatPtps  map[string][]string // V: map for each category's statistic, category can be "thumb", "vote-item" etc. Participants is [user] list
	fnDbStore func(*EventParticipate) error
}

func newEventParticipate(evtID string) *EventParticipate {
	return &EventParticipate{
		evtID:     evtID,
		mCatPtps:  make(map[string][]string),
		fnDbStore: bh.UpsertOneObject[EventParticipate],
	}
}

func NewEventParticipate(evtID string, useExisting bool) (*EventParticipate, error) {
	if p, err := Participate(evtID); err == nil && p != nil {
		if useExisting {
			return p, err
		}
		return nil, fmt.Errorf("event <%s> is already existing, cannot be NewEventParticipate", evtID)
	}
	return newEventParticipate(evtID), nil
}

func (ep EventParticipate) String() string {
	sb := strings.Builder{}
	sb.WriteString("--EventID: " + ep.evtID)
	for cat, ptps := range ep.mCatPtps {
		sb.WriteString(fmt.Sprintf("\n----Category:%s\tParticipants:%v", cat, ptps))
	}
	return sb.String()
}

func (ep *EventParticipate) Key() []byte {
	return []byte(ep.evtID)
}

// cat1::user11^^user12^^user13##cat2::user21^^user22##...
func (ep *EventParticipate) Value() []byte {
	sb := strings.Builder{}
	for cat, ptps := range ep.mCatPtps {
		sb.WriteString(cat + SEP_V_Map)
		for i, p := range ptps {
			sb.WriteString(IF(i < len(ptps)-1, p+SEP_V_Participant, p))
		}
		sb.WriteString(SEP_V_Category)
	}
	return []byte(strings.TrimSuffix(sb.String(), SEP_V_Category))
}

func (ep *EventParticipate) Marshal(at any) (forKey, forValue []byte) {
	forKey = ep.Key()
	lk.FailOnErrWhen(len(forKey) == 0, "%v", errors.New("empty event for participants"))
	forValue = ep.Value()
	return
}

func (ep *EventParticipate) Unmarshal(dbKey, dbVal []byte) (any, error) {
	ep.evtID = string(dbKey)

	dbValStr := string(dbVal)
	if ep.mCatPtps == nil {
		ep.mCatPtps = make(map[string][]string)
	}
	for _, catItem := range strings.Split(dbValStr, SEP_V_Category) {
		ss := strings.SplitN(catItem, SEP_V_Map, 2)
		cat, ptps := ss[0], ss[1]
		ep.mCatPtps[cat] = IF(len(ptps) == 0, []string{}, strings.Split(ptps, SEP_V_Participant))
	}

	ep.fnDbStore = bh.UpsertOneObject[EventParticipate]
	return ep, nil
}

func (ep *EventParticipate) BadgerDB() *badger.DB {
	return DbGrp.IDPtps
}

/////////////////////////////////////////////////////////////////////////////

func (ep *EventParticipate) AddParticipants(category string, participants ...string) error {
	if !EventHappened(ep.evtID) {
		return fmt.Errorf("<%s> is not existing, cannot add participants", ep.evtID)
	}
	ep.mCatPtps[category] = append(ep.mCatPtps[category], participants...)
	ep.mCatPtps[category] = Settify(ep.mCatPtps[category]...)
	return ep.fnDbStore(ep)
}

func (ep *EventParticipate) RmParticipants(category string, toRemove ...string) (int, error) {
	if !EventHappened(ep.evtID) {
		return -1, fmt.Errorf("<%s> is not existing, cannot remove participants", ep.evtID)
	}
	ptps := ep.mCatPtps[category]
	prevN := len(ptps)
	ptps = FilterMap(ptps, func(i int, e string) bool {
		return len(e) > 0 && NotIn(e, toRemove...)
	}, func(i int, e string) string {
		return e
	})
	ep.mCatPtps[category] = ptps
	if err := ep.fnDbStore(ep); err != nil {
		return -1, err
	}
	return prevN - len(ptps), nil
}

func (ep *EventParticipate) HasParticipant(category, participant string) bool {
	ptps, err := ep.Participants(category)
	if err != nil {
		return false
	}
	return In(participant, ptps...)
}

func (ep *EventParticipate) ToggleParticipant(category, participant string) (bool, error) {
	if ep.HasParticipant(category, participant) {
		n, err := ep.RmParticipants(category, participant)
		if err != nil {
			return false, err
		}
		if n != 1 {
			return false, fmt.Errorf("[%d] != 1, participant [%s] is not removed properly", n, participant)
		}
		return false, nil
	} else {
		if err := ep.AddParticipants(category, participant); err != nil {
			return false, err
		}
		return true, nil
	}
}

func (ep *EventParticipate) Participants(category string) ([]string, error) {
	if !EventHappened(ep.evtID) {
		return []string{}, fmt.Errorf("<%s> is not existing, cannot get participants", ep.evtID)
	}
	if ep.mCatPtps == nil {
		return []string{}, nil
	}
	if _, ok := ep.mCatPtps[category]; !ok {
		return []string{}, nil
	}
	return ep.mCatPtps[category], nil
}

////////////////////////////////////////////////////////////////////////////////////////////

func Participate(evtID string) (*EventParticipate, error) {
	if !EventHappened(evtID) {
		return nil, fmt.Errorf("<%s> is not existing, its participate cannot be fetched", evtID)
	}
	ep, err := bh.GetOneObject[EventParticipate]([]byte(evtID))
	if err != nil {
		return nil, err
	}
	if ep == nil {
		ep = newEventParticipate(evtID)
	}
	return ep, err
}

func deleteParticipate(evtID string) (int, error) {
	return bh.DeleteObjects[EventParticipate]([]byte(evtID))
}
