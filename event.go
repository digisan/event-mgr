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
	. "github.com/digisan/go-generics/v2"
	lk "github.com/digisan/logkit"
	"github.com/google/uuid"
)

// key: ID;
// value: others
type Event struct {
	ID        string
	Tm        time.Time
	Owner     string
	EvtType   string
	RawJSON   string
	Public    bool
	fnDbStore func(*Event) error
}

// if [id] is empty, a new one will be assigned to event.
func NewEvent(id, owner, evtType, raw string) *Event {
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
		fnDbStore: UpsertOneObjectDB[Event],
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
	MOK_Id int = iota
	MOK_N
)

func (evt *Event) KeyFieldAddr(mok int) *string {
	mFldAddr := map[int]*string{
		MOK_Id: &evt.ID,
	}
	return mFldAddr[mok]
}

// db value order
const (
	MOV_Tm int = iota
	MOV_Owner
	MOV_EvtType
	MOV_RawJSON
	MOV_Pub
	MOV_N
)

func (evt *Event) ValFieldAddr(mov int) any {
	mFldAddr := map[int]any{
		MOV_Tm:      &evt.Tm,
		MOV_Owner:   &evt.Owner,
		MOV_EvtType: &evt.EvtType,
		MOV_RawJSON: &evt.RawJSON,
		MOV_Pub:     &evt.Public,
	}
	return mFldAddr[mov]
}

////////////////////////////////////////////////////

func (evt *Event) BadgerDB() *badger.DB {
	return eDB.dbIDEvt
}

func (evt *Event) Key() []byte {
	return []byte(evt.ID)
}

func (evt *Event) Marshal(at any) (forKey, forValue []byte) {
	lk.FailOnErrWhen(len(evt.ID) == 0, "%v", errors.New("empty event id"))

	forKey = evt.Key()

	for i := 0; i < MOV_N; i++ {
		switch v := evt.ValFieldAddr(i).(type) {

		case *string:
			forValue = append(forValue, []byte(*v)...)

		case *bool:
			forValue = append(forValue, []byte(fmt.Sprint(*v))...)

		case *time.Time:
			forValue = append(forValue, []byte(v.Format(time.RFC3339))...)

		}
		if i < MOV_N-1 {
			forValue = append(forValue, []byte(SEP)...)
		}
	}
	return
}

func (evt *Event) Unmarshal(dbKey, dbVal []byte) (any, error) {
	evt.ID = string(dbKey)
	segs := bytes.SplitN(dbVal, []byte(SEP), MOV_N)
	for i := 0; i < MOV_N; i++ {
		sval := string(segs[i])
		switch i {

		case MOV_Tm:
			t, err := time.Parse(time.RFC3339, sval)
			if err != nil {
				lk.WarnOnErr("%v", err)
				return nil, err
			}
			*evt.ValFieldAddr(i).(*time.Time) = t

		case MOV_Pub:
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
	evt.fnDbStore = UpsertOneObjectDB[Event]
	return evt, nil
}

func (evt *Event) Publish(pub bool) error {
	evt.Public = pub
	return evt.fnDbStore(evt)
}

func FetchEvent(id string) (*Event, error) {
	return GetOneObjectDB[Event]([]byte(id))
}

func FetchEvents(ids ...string) (evts []*Event, err error) {
	for _, id := range ids {
		evt, err := FetchEvent(id)
		if err != nil {
			return nil, err
		}
		evts = append(evts, evt)
	}
	return
}
