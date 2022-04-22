package eventmgr

import (
	"fmt"
	"strings"
	"sync"
	"time"

	lk "github.com/digisan/logkit"
)

var mSpanType = map[string]int64{
	"DAY":          1440,
	"HALF_DAY":     720,
	"TWO_HOUR":     120,
	"HOUR":         60,
	"HALF_HOUR":    30,
	"QUARTER_HOUR": 15,
	"TEN_MINUTE":   10,
	"FIVE_MINUTE":  5,
	"TWO_MINUTE":   2,
	"MINUTE":       1,
}

// key: span; value: IDs
type EventSpan struct {
	mtx        *sync.Mutex
	spanType   string
	mSpanIDs   map[string][]string
	prevSpan   string
	fnDbAppend func(*EventSpan, bool) error
}

func NewEventSpan() *EventSpan {
	return &EventSpan{
		mtx:        &sync.Mutex{},
		spanType:   dfltSpanType,
		mSpanIDs:   make(map[string][]string),
		prevSpan:   "",
		fnDbAppend: nil,
	}
}

func (es EventSpan) String() string {
	sb := strings.Builder{}
	for span, ids := range es.mSpanIDs {
		sb.WriteString(span + ": \n")
		for idx, id := range ids {
			sb.WriteString(fmt.Sprintf("\t%02d\t%s\n", idx, id))
		}
	}
	return sb.String()
}

func (es *EventSpan) SetSpan(spanType string) error {
	if _, ok := mSpanType[spanType]; !ok {
		return fmt.Errorf("[%v] is an unsupported setting value", spanType)
	}
	es.spanType = spanType
	return nil
}

func (es *EventSpan) GetSpan() string {
	ts := time.Now().Unix()
	tsMin := ts / 60
	sm, ok := mSpanType[es.spanType]
	if !ok {
		es.spanType = dfltSpanType
		sm = mSpanType[es.spanType]
	}
	start := tsMin / sm * sm
	return fmt.Sprintf("%d-%d", start, sm)
}

func (es *EventSpan) DbAppendFunc(dbUpdate func(*EventSpan, bool) error) {
	es.fnDbAppend = dbUpdate
}

func (es *EventSpan) AddEvent(evt *Event) error {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	dbKey := es.GetSpan()
	defer func() { es.prevSpan = dbKey }()

	///////////////////////////////////////////////

	if evt.fnDbStore == nil {
		return fmt.Errorf("Event [OnDbStore] must be done before AddEvent")
	}
	if err := evt.fnDbStore(evt, false); err != nil {
		return err
	}
	lk.Log("%v", evt)

	///////////////////////////////////////////////

	if es.prevSpan != "" && dbKey != es.prevSpan {
		if es.fnDbAppend == nil {
			return fmt.Errorf("EventSpan [SetDbAppendFunc] must be done before AddEvent")
		}
		if err := es.fnDbAppend(es, false); err != nil { // store mSpanRefIDs at 'prevSpan'
			return err
		}
		delete(es.mSpanIDs, es.prevSpan)
	}

	es.mSpanIDs[dbKey] = append(es.mSpanIDs[dbKey], evt.ID)
	return nil
}

func (es *EventSpan) Flush() error {
	if es.fnDbAppend == nil {
		return fmt.Errorf("EventSpan [SetDbAppendFunc] must be done before AddEvent")
	}
	es.prevSpan = es.GetSpan()
	if err := es.fnDbAppend(es, true); err != nil { // store mSpanRefIDs at 'prevSpan'
		return err
	}
	delete(es.mSpanIDs, es.prevSpan)
	return nil
}

func (es *EventSpan) Marshal() (forKey, forValue []byte) {
	forKey = []byte(es.prevSpan)
	forValue = []byte(strings.Join(es.mSpanIDs[es.prevSpan], SEP))
	return
}

func (es *EventSpan) Unmarshal(dbKey, dbVal []byte) error {
	if es.mSpanIDs == nil {
		es.mSpanIDs = make(map[string][]string)
	}
	es.mSpanIDs[string(dbKey)] = strings.Split(string(dbVal), SEP)
	return nil
}

func (es *EventSpan) CurrentIDS() []string {
	return es.mSpanIDs[es.GetSpan()]
}

// func (es *EventSpan)
