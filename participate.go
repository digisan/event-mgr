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

// Participate a Post, such as thumb, vote etc

const (
	SEP_V_Cat  = "^^"
	SEP_V_Ptpt = "^"
	SEP_V_Map  = ":"
)

type EventParticipate struct {
	evtId     string              // K: event id
	mCatPtps  map[string][]string // V: map for each category's statistic, category can be "thumb", "vote-item" etc. Participants is [user] list
	fnDbStore func(*EventParticipate) error
}

func newEventParticipate(evtId string) *EventParticipate {
	return &EventParticipate{
		evtId:     evtId,
		mCatPtps:  make(map[string][]string),
		fnDbStore: bh.UpsertOneObjectDB[EventParticipate],
	}
}

func NewEventParticipate(evtId string, useExisting bool) (*EventParticipate, error) {
	if p, err := Participate(evtId); err == nil && p != nil {
		if useExisting {
			return p, err
		}
		return nil, fmt.Errorf("<%s> is already existing, cannot be New,", evtId)
	}
	return newEventParticipate(evtId), nil
}

func (ep EventParticipate) String() string {
	sb := strings.Builder{}
	sb.WriteString("EventID: " + ep.evtId + "\n")
	for cat, ptps := range ep.mCatPtps {
		sb.WriteString(fmt.Sprintf("\n  Category:%s\tParticipants:%v", cat, ptps))
	}
	return sb.String()
}

func (ep *EventParticipate) Key() []byte {
	return []byte(ep.evtId)
}

// cat1:user11^user12^user13^^cat2:user21^user22^^...
func (ep *EventParticipate) Value() []byte {
	sb := strings.Builder{}
	for cat, ptps := range ep.mCatPtps {
		sb.WriteString(cat + SEP_V_Map)
		for i, p := range ptps {
			if i < len(ptps)-1 {
				sb.WriteString(p + SEP_V_Ptpt)
			} else {
				sb.WriteString(p)
			}
		}
		sb.WriteString(SEP_V_Cat)
	}
	return []byte(strings.TrimSuffix(sb.String(), SEP_V_Cat))
}

func (ep *EventParticipate) Marshal(at any) (forKey, forValue []byte) {
	forKey = ep.Key()
	lk.FailOnErrWhen(len(forKey) == 0, "%v", errors.New("empty event for participants"))
	forValue = ep.Value()
	return
}

func (ep *EventParticipate) Unmarshal(dbKey, dbVal []byte) (any, error) {
	ep.evtId = string(dbKey)

	dbValStr := string(dbVal)
	if ep.mCatPtps == nil {
		ep.mCatPtps = make(map[string][]string)
	}
	for _, catItem := range strings.Split(dbValStr, SEP_V_Cat) {
		ss := strings.SplitN(catItem, SEP_V_Map, 2)
		cat, ptps := ss[0], ss[1]
		ep.mCatPtps[cat] = strings.Split(ptps, SEP_V_Ptpt)
	}

	ep.fnDbStore = bh.UpsertOneObjectDB[EventParticipate]
	return ep, nil
}

func (ep *EventParticipate) BadgerDB() *badger.DB {
	return DbGrp.IDPtps
}

/////////////////////////////////////////////////////////////////////////////

func (ep *EventParticipate) AddPtps(category string, participants ...string) error {
	if !EventIsAlive(ep.evtId) {
		return fmt.Errorf("<%s> is not alive, cannot add participants", ep.evtId)
	}
	ep.mCatPtps[category] = append(ep.mCatPtps[category], participants...)
	ep.mCatPtps[category] = Settify(ep.mCatPtps[category]...)
	return ep.fnDbStore(ep)
}

func (ep *EventParticipate) RmPtps(category string, participants ...string) (int, error) {
	if !EventIsAlive(ep.evtId) {
		return -1, fmt.Errorf("<%s> is not alive, cannot remove participants", ep.evtId)
	}
	catptps := ep.mCatPtps[category]
	prevN := len(catptps)
	FilterFast(&catptps, func(i int, e string) bool { return NotIn(e, participants...) })
	ep.mCatPtps[category] = catptps
	if err := ep.fnDbStore(ep); err != nil {
		return -1, err
	}
	return prevN - len(catptps), nil
}

func (ep *EventParticipate) HasPtp(category, participant string) (bool, error) {
	ptps, err := ep.Ptps(category)
	if err != nil {
		return false, err
	}
	return In(participant, ptps...), nil
}

func (ep *EventParticipate) TogglePtp(category, participant string) (bool, error) {
	hasPtp, err := ep.HasPtp(category, participant)
	if err != nil {
		return false, err
	}
	if hasPtp {
		n, err := ep.RmPtps(category, participant)
		if err != nil {
			return false, err
		}
		if n != 1 {
			return false, fmt.Errorf("remove participant [%s] error", participant)
		}
		return false, nil
	} else {
		if err := ep.AddPtps(category, participant); err != nil {
			return false, err
		}
		return true, nil
	}
}

func (ep *EventParticipate) Ptps(category string) ([]string, error) {
	if !EventIsAlive(ep.evtId) {
		return []string{}, fmt.Errorf("<%s> is not alive, cannot get participants", ep.evtId)
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

func Participate(evtId string) (*EventParticipate, error) {
	if !EventIsAlive(evtId) {
		return nil, fmt.Errorf("<%s> is not alive, its participate cannot be fetched", evtId)
	}
	ep, err := bh.GetOneObjectDB[EventParticipate]([]byte(evtId))
	if err != nil {
		return nil, err
	}
	if ep == nil {
		ep = newEventParticipate(evtId)
	}
	return ep, err
}

// func Participants(evtId, category string) ([]string, error) {
// 	ep, err := Participate(evtId)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if ep == nil {
// 		return []string{}, nil
// 	}
// 	return ep.Ptps(category)
// }

func deleteParticipate(evtid string) (int, error) {
	return bh.DeleteObjectsDB[EventParticipate]([]byte(evtid))
}
