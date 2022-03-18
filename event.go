package eventmgr

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

var mSpanMinute = map[string]int64{
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

type EventBlock struct {
	mtx        sync.Mutex
	spanType   string
	mSpanIDs   map[string][]string
	prevSpan   string
	fnDbAppend func(*EventBlock, bool) error
}

const dfltSpanType = "TEN_MINUTE"
const SEP = "^"

func NewEventBlock() *EventBlock {
	return &EventBlock{
		mtx:        sync.Mutex{},
		spanType:   dfltSpanType,
		mSpanIDs:   make(map[string][]string),
		prevSpan:   "",
		fnDbAppend: nil,
	}
}

func (eb EventBlock) String() string {
	sb := strings.Builder{}
	for span, ids := range eb.mSpanIDs {
		sb.WriteString(span + ": \n")
		for _, id := range ids {
			sb.WriteString("\t" + id + "\n")
		}
	}
	return sb.String()
}

func (eb *EventBlock) SetSpan(spanType string) error {
	if _, ok := mSpanMinute[spanType]; !ok {
		return fmt.Errorf("[%v] is an unsupported setting value", spanType)
	}
	eb.spanType = spanType
	return nil
}

func (eb *EventBlock) GetTmSpan() string {
	ts := time.Now().Unix()
	tsMin := ts / 60
	sm, ok := mSpanMinute[eb.spanType]
	if !ok {
		eb.spanType = dfltSpanType
		sm = mSpanMinute[eb.spanType]
	}
	start := tsMin / sm * sm
	return fmt.Sprintf("%d-%d", start, sm)
}

func (eb *EventBlock) SetDbAppendFunc(dbUpdate func(*EventBlock, bool) error) {
	eb.fnDbAppend = dbUpdate
}

// (refID -- resPath)
func (eb *EventBlock) AddEvent(refID, resPath string) error {
	eb.mtx.Lock()
	defer eb.mtx.Unlock()

	dbKey := eb.GetTmSpan()
	defer func() { eb.prevSpan = dbKey }()

	if eb.prevSpan != "" && dbKey != eb.prevSpan {
		if eb.fnDbAppend != nil {
			if err := eb.fnDbAppend(eb, false); err == nil { // store mSpanRefIDs at 'prevSpan'
				delete(eb.mSpanIDs, eb.prevSpan)
			} else {
				return err
			}
		} else {
			return fmt.Errorf("[SetDbAppendFunc] must be done before AddEvent")
		}
	}

	eb.mSpanIDs[dbKey] = append(eb.mSpanIDs[dbKey], refID)
	return nil
}

func (eb *EventBlock) Flush() error {
	if eb.fnDbAppend != nil {
		eb.prevSpan = eb.GetTmSpan()
		if err := eb.fnDbAppend(eb, true); err == nil { // store mSpanRefIDs at 'prevSpan'
			delete(eb.mSpanIDs, eb.prevSpan)
		} else {
			return err
		}
	} else {
		return fmt.Errorf("[SetDbAppendFunc] must be done before AddEvent")
	}
	return nil
}

func (eb *EventBlock) MarshalSpanIDs() (forKey, forValue []byte) {
	forKey = []byte(eb.prevSpan)
	forValue = []byte(strings.Join(eb.mSpanIDs[eb.prevSpan], SEP))
	return
}

func (eb *EventBlock) UnmarshalSpanIDs(dbKey, dbVal []byte) {
	if eb.mSpanIDs == nil {
		eb.mSpanIDs = make(map[string][]string)
	}
	eb.mSpanIDs[string(dbKey)] = strings.Split(string(dbVal), SEP)
}

func (eb *EventBlock) MarshalIDsSpan() (forKeys [][]byte, forValues [][]byte) {
	for _, id := range eb.mSpanIDs[eb.prevSpan] {
		forKeys = append(forKeys, []byte(id))
		forValues = append(forValues, []byte(eb.prevSpan))
	}
	return
}

func (eb *EventBlock) CurrentIDS() []string {
	return eb.mSpanIDs[eb.GetTmSpan()]
}
