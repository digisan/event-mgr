package eventmgr

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	. "github.com/digisan/go-generics/v2"
	lk "github.com/digisan/logkit"
)

// key: ID;
// value: others
type Event struct {
	ID        string
	Owner     string
	EvtType   string
	MetaJSON  string
	Publish   bool
	fnDbStore func(*Event, bool) error
}

func NewEvent(owner, evtType, meta string) *Event {
	return &Event{
		ID:        "",
		Owner:     owner,
		EvtType:   evtType,
		MetaJSON:  meta,
		Publish:   false,
		fnDbStore: nil,
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
	MOV_Owner int = iota
	MOV_EvtType
	MOV_MetaJSON
	MOV_Pub
	MOV_N
)

func (evt *Event) ValFieldAddr(mov int) any {
	mFldAddr := map[int]any{
		MOV_Owner:    &evt.Owner,
		MOV_EvtType:  &evt.EvtType,
		MOV_MetaJSON: &evt.MetaJSON,
		MOV_Pub:      &evt.Publish,
	}
	return mFldAddr[mov]
}

////////////////////////////////////////////////////

func (evt *Event) Marshal() (forKey []byte, forValue []byte) {
	lk.FailOnErrWhen(len(evt.ID) == 0, "%v", errors.New("empty event id"))

	forKey = []byte(evt.ID)
	for i := 0; i < MOV_N; i++ {
		switch v := evt.ValFieldAddr(i).(type) {
		case *string:
			forValue = append(forValue, []byte(*v)...)
		case *bool:
			forValue = append(forValue, []byte(fmt.Sprint(*v))...)
		}
		if i < MOV_N-1 {
			forValue = append(forValue, []byte(SEP)...)
		}
	}
	return
}

func (evt *Event) Unmarshal(dbKey, dbVal []byte) error {
	evt.ID = string(dbKey)
	segs := bytes.SplitN(dbVal, []byte(SEP), MOV_N)
	for i := 0; i < MOV_N; i++ {
		switch i {
		case MOV_Pub:
			pub, err := strconv.ParseBool(string(segs[i]))
			if err != nil {
				lk.WarnOnErr("%v", err)
				return err
			}
			*evt.ValFieldAddr(i).(*bool) = pub
		default:
			*evt.ValFieldAddr(i).(*string) = string(segs[i])
		}
	}
	return nil
}

func (evt *Event) DbStoreFunc(dbStore func(*Event, bool) error) {
	evt.fnDbStore = dbStore
}
