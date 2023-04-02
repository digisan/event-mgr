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
	Flwee     string
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
		Flwee:     flwee,
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
		VO_Flwee:   &evt.Flwee,
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
	evt, err := bh.GetOneObject[Event]([]byte(id))
	if err != nil {
		return nil, err
	}
	if aliveOnly {
		if evt != nil && !evt.Deleted {
			return evt, err
		}
		return nil, err
	}
	return evt, err
}

func FetchEvents(aliveOnly bool, ids ...string) (evts []*Event, err error) {
	for _, id := range ids {
		evt, err := FetchEvent(aliveOnly, id)
		if err != nil {
			return nil, err
		}
		if evt != nil {
			evts = append(evts, evt)
		}
	}
	return
}

func PubEvent(id string, flag bool) error {
	evt, err := FetchEvent(true, id)
	if err != nil {
		return err
	}
	if evt != nil {
		return evt.Publish(flag)
	}
	return fmt.Errorf("couldn't find %s, publish nothing", id)
}

func DelEvent(ids ...string) (int, error) {
	cnt := 0
	for _, id := range ids {
		evt, err := FetchEvent(true, id)
		if err != nil {
			return -1, err
		}
		if evt != nil {
			evt.markDeleted()
			cnt++
		}
	}
	return cnt, nil
}

func EventHappened(id string) bool {
	evt, err := FetchEvent(true, id)
	return err == nil && evt != nil
}

func EraseEvents(ids ...string) (int, error) {
	DbGrp.Lock()
	defer DbGrp.Unlock()

	cnt := 0
	for _, id := range ids {

		// step 1: fetch event to get its tm
		evt, err := FetchEvent(false, id)
		if err != nil {
			return -1, err
		}
		if evt == nil {
			continue
		}

		// step 2: delete from id-event
		n, err := bh.DeleteOneObject[Event]([]byte(id))
		if err != nil {
			return -1, err
		}
		if n == 0 {
			continue
		}

		if n == 1 {

			// step 3: delete from span-ids
			span := getSpanAt(evt.Tm)
			eIds, err := FetchEvtIDs([]byte(span))
			if err != nil {
				return -1, err
			}
			tmpEvts := FilterMap(
				eIds,
				func(i int, s string) bool { return s != id },
				func(i int, e string) TempEvt { return TempEvt{evtId: e} },
			)
			es := &EventSpan{mSpanCache: map[string][]TempEvt{span: tmpEvts}}
			if err := bh.UpsertPartObject(es, span); err != nil {
				return -1, err
			}

			// step 4: delete from own
			n, err := deleteOwn(evt.Owner, evt.Tm.Format("200601"), span, id)
			if err != nil {
				return -1, err
			}
			lk.FailOnErrWhen(n != 1, "%v", "n must be 1 in deleting from Own")

			// step 5: delete follow
			if _, err = deleteEventFollow(id); err != nil {
				return -1, err
			}

			// step 6: delete participants
			if _, err = deleteParticipate(id); err != nil {
				return -1, err
			}

			//
			cnt++
		}
	}
	return cnt, nil
}
