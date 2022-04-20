package eventmgr

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	lk "github.com/digisan/logkit"
)

// key: id;
// value: others
type Event struct {
	id         string
	owner      string
	evtType    string
	metaJSON   string
	publish    bool
	fnDbAppend func(*Event, bool) error
}

func NewEvent(owner, evtType, meta string) *Event {
	return &Event{
		id:       "",
		owner:    owner,
		evtType:  evtType,
		metaJSON: meta,
		publish:  false,
	}
}

// db key order
const (
	MOK_Id int = iota
	MOK_N
)

func (evt *Event) KeyFieldAddr(mok int) *string {
	mFldAddr := map[int]*string{
		MOK_Id: &evt.id,
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
		MOV_Owner:    &evt.owner,
		MOV_EvtType:  &evt.evtType,
		MOV_MetaJSON: &evt.metaJSON,
		MOV_Pub:      &evt.publish,
	}
	return mFldAddr[mov]
}

////////////////////////////////////////////////////

func (evt *Event) Marshal() (forKey []byte, forValue []byte) {
	lk.FailOnErrWhen(len(evt.id) == 0, "%v", errors.New("empty event id"))

	forKey = []byte(evt.id)
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
	evt.id = string(dbKey)
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

func (evt *Event) SetDbAppendFunc(dbUpdate func(*Event, bool) error) {
	evt.fnDbAppend = dbUpdate
}
