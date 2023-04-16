package eventmgr

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
	bh "github.com/digisan/db-helper/badger"
	. "github.com/digisan/go-generics/v2"
	lk "github.com/digisan/logkit"
	"github.com/google/uuid"
)

// key: ID;
// value: others
// if changed, modify 1) NewXXX, 2) KO/VO 3) (Key/Value)FieldAddr, 4) Marshal, 5) Unmarshal.
type Event struct {
	ID        string
	Tm        time.Time
	Owner     string
	EvtType   string
	RawJSON   string
	Public    bool
	Deleted   bool
	Followee  string
	fnDbStore func(*Event) error
}

// if [id] is empty, a new one will be assigned to event.
func NewEvent(id, owner, evtType, raw, flwee string) *Event {
	if len(id) == 0 {
		id = uuid.NewString()
	}
	return &Event{
		ID:        id,
		Tm:        time.Now().Truncate(time.Second),
		Owner:     owner,
		EvtType:   evtType,
		RawJSON:   raw,
		Public:    false,
		Deleted:   false,
		Followee:  flwee,
		fnDbStore: bh.UpsertOneObject[Event],
	}
}

func (evt Event) String() string {
	if len(evt.ID) > 0 {
		sb := strings.Builder{}
		t, v := reflect.TypeOf(evt), reflect.ValueOf(evt)
		for i := 0; i < t.NumField(); i++ {
			fld, val := t.Field(i), v.Field(i)
			if NotIn(fld.Name, "fnDbStore") {
				sb.WriteString(fmt.Sprintf("%-12s %v\n", fld.Name+":", val.Interface()))
			}
		}
		return sb.String()
	}
	return "[Empty Event]"
}

// db key order
const (
	KO_Id int = iota
	KO_END
)

func (evt *Event) KeyFieldAddr(ko int) *string {
	mFldAddr := map[int]*string{
		KO_Id: &evt.ID,
	}
	return mFldAddr[ko]
}

// db value order
const (
	VO_Tm int = iota
	VO_Owner
	VO_EvtType
	VO_RawJSON
	VO_Pub
	VO_Del
	VO_Flwee
	VO_END
)

func (evt *Event) ValFieldAddr(vo int) any {
	mFldAddr := map[int]any{
		VO_Tm:      &evt.Tm,
		VO_Owner:   &evt.Owner,
		VO_EvtType: &evt.EvtType,
		VO_RawJSON: &evt.RawJSON,
		VO_Pub:     &evt.Public,
		VO_Del:     &evt.Deleted,
		VO_Flwee:   &evt.Followee,
	}
	return mFldAddr[vo]
}

////////////////////////////////////////////////////

func (evt *Event) BadgerDB() *badger.DB {
	return DbGrp.IDEvt
}

func (evt *Event) Key() []byte {
	return []byte(evt.ID)
}

func (evt *Event) Marshal(at any) (forKey, forValue []byte) {
	lk.FailOnErrWhen(len(evt.ID) == 0, "%v", errors.New("empty event id"))

	forKey = evt.Key()

	for i := 0; i < VO_END; i++ {
		switch v := evt.ValFieldAddr(i).(type) {

		case *string:
			forValue = append(forValue, []byte(*v)...)

		case *bool:
			forValue = append(forValue, []byte(fmt.Sprint(*v))...)

		case *time.Time:
			forValue = append(forValue, []byte(v.Format(time.RFC3339))...)
		}
		if i < VO_END-1 {
			forValue = append(forValue, []byte(SEP)...)
		}
	}
	return
}

func (evt *Event) Unmarshal(dbKey, dbVal []byte) (any, error) {
	evt.ID = string(dbKey)
	segs := bytes.SplitN(dbVal, []byte(SEP), VO_END)
	for i := 0; i < VO_END; i++ {
		sval := string(segs[i])
		switch i {

		case VO_Tm:
			t, err := time.Parse(time.RFC3339, sval)
			if err != nil {
				lk.WarnOnErr("%v", err)
				return nil, err
			}
			*evt.ValFieldAddr(i).(*time.Time) = t

		case VO_Pub, VO_Del:
			pub, err := strconv.ParseBool(sval)
			if err != nil {
				lk.WarnOnErr("%v", err)
				return nil, err
			}
			*evt.ValFieldAddr(i).(*bool) = pub

		default:
			*evt.ValFieldAddr(i).(*string) = sval
		}
	}
	evt.fnDbStore = bh.UpsertOneObject[Event]
	return evt, nil
}

func (evt *Event) Publish(pub bool) error {
	evt.Public = pub
	return evt.fnDbStore(evt)
}

func (evt *Event) markDeleted() error {
	evt.Deleted = true
	return evt.fnDbStore(evt)
}

func FetchEvent(aliveOnly bool, id string) (*Event, error) {
	event, err := bh.GetOneObject[Event]([]byte(id))
	if err != nil {
		return nil, err
	}
	if aliveOnly {
		if event != nil && !event.Deleted {
			return event, err
		}
		return nil, err
	}
	return event, err
}

func FetchEvents(aliveOnly bool, ids ...string) (events []*Event, err error) {
	for _, id := range ids {
		event, err := FetchEvent(aliveOnly, id)
		if err != nil {
			return nil, err
		}
		if event != nil {
			events = append(events, event)
		}
	}
	return
}

func PubEvent(id string, flag bool) error {
	event, err := FetchEvent(true, id)
	if err != nil {
		return err
	}
	if event != nil {
		return event.Publish(flag)
	}
	return fmt.Errorf("couldn't find %s, publish nothing", id)
}

func EventExists(id string) bool {
	event, err := FetchEvent(true, id)
	return err == nil && event != nil
}

func EventHappened(id string) bool {
	event, err := FetchEvent(false, id)
	return err == nil && event != nil
}

func DelEvent(ids ...string) ([]string, error) {
	var deleted []string
	for _, id := range ids {
		event, err := FetchEvent(false, id)
		if err != nil {
			return nil, err
		}
		if event != nil {

			// STEP 1: delete from span-ids
			ok, err := DelOneEventID(CreateSpanAt(event.Tm), id)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("%v, @%v", "event-id in Span-EventID is not registered normally", id)
			}

			// STEP 2: mark deleted
			if err := event.markDeleted(); err != nil {
				return nil, err
			}

			deleted = append(deleted, id)
		}
	}
	return deleted, nil
}

func EraseEvent(ids ...string) ([]string, error) {
	DbGrp.Lock()
	defer DbGrp.Unlock()

	var erased []string
	for _, id := range ids {

		// STEP 1: fetch event to get its tm
		event, err := FetchEvent(false, id)
		if err != nil {
			return nil, err
		}
		if event == nil {
			continue
		}

		// STEP 2: delete from id-event
		n, err := bh.DeleteOneObject[Event]([]byte(id))
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue
		}

		// event content has been deleted, then...
		if n == 1 {

			// STEP 3: delete from span-ids
			span := CreateSpanAt(event.Tm)
			ok, err := DelOneEventID(span, id)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("%v, @%v", "event-id in Event-Body is not consistent with Span-EventID", id)
			}

			// eIds, err := FetchEvtIDs([]byte(span))
			// if err != nil {
			// 	return -1, err
			// }
			// tmpEvents := FilterMap(
			// 	eIds,
			// 	func(i int, s string) bool { return s != id },
			// 	func(i int, e string) TempEvt { return TempEvt{evtID: e} },
			// )
			// es := &EventSpan{mSpanCache: map[string][]TempEvt{span: tmpEvents}}
			// if err := bh.UpsertPartObject(es, span); err != nil {
			// 	return -1, err
			// }

			// STEP 4: delete from own
			n, err := deleteOwn(event.Owner, event.Tm.Format("200601"), span, id)
			if err != nil {
				return nil, err
			}
			if n != 1 {
				return nil, fmt.Errorf("%v", "n must be 1 in deleting from Own")
			}

			// STEP 5: delete follow
			if _, err = deleteEventFollow(id); err != nil {
				return nil, err
			}

			// STEP 6: delete participants
			if _, err = deleteParticipate(id); err != nil {
				return nil, err
			}

			//
			erased = append(erased, id)
		}
	}
	return erased, nil
}

func DelOneEventID(span string, id string) (bool, error) {
	eIds, err := FetchEvtID([]byte(span))
	if err != nil {
		return false, err
	}
	tmpEvents := FilterMap(
		eIds,
		func(i int, s string) bool { return s != id },
		func(i int, e string) TempEvt { return TempEvt{evtID: e} },
	)
	es := &EventSpan{mSpanCache: map[string][]TempEvt{span: tmpEvents}}
	if err := bh.UpsertPartObject(es, span); err != nil {
		return false, err
	}
	return len(eIds) > len(tmpEvents), nil
}

func DelGlobalEventID(ids ...string) ([]string, error) {

	spans, err := FetchSpan(nil)
	if err != nil {
		return nil, err
	}

	ids = Settify(ids...)
	var deleted []string
SPAN:
	for _, span := range spans {
		for _, id := range ids {
			ok, err := DelOneEventID(span, id)
			if err != nil {
				return deleted, err
			}
			if ok {
				deleted = append(deleted, id)
				ids = FilterMap4SglTyp(ids, func(i int, e string) bool { return e != id }, nil)
				if len(deleted) == len(ids) {
					break SPAN
				}
				continue SPAN
			}
		}
	}
	return deleted, nil
}
