package eventmgr

import (
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/digisan/go-generics/v2"
	"github.com/digisan/gotk/strs"
	lk "github.com/digisan/logkit"
)

type TempEvt struct {
	owner  string
	yyyymm string
	evtId  string
}

var (
	curSpanType = "TEN_MINUTE"
	cache       = []TempEvt{}
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

func SetSpanType(spanType string) error {
	if _, ok := mSpanType[spanType]; !ok {
		return fmt.Errorf("[%v] is an unsupported value", spanType)
	}
	curSpanType = spanType
	return nil
}

// tm: such as "2h20m", "30m", "2s"
func getSpan(tm string, past bool) string {
	if len(tm) == 0 {
		tm = "0s"
	}
	duration, err := time.ParseDuration(tm)
	lk.FailOnErr("%v", err)

	duration = IF(past, -duration, duration)

	then := time.Now().Add(duration) // past is minus duration
	tsMin := then.Unix() / 60

	sm, ok := mSpanType[curSpanType]
	lk.FailOnErrWhen(!ok, "%v", fmt.Errorf("error at '%s'", curSpanType))
	start := tsMin / sm * sm
	return fmt.Sprintf("%d-%d", start, sm)
}

func NowSpan() string {
	return getSpan("", false)
}

func PastSpan(tm string) string {
	return getSpan(tm, true)
}

func FutureSpan(tm string) string {
	return getSpan(tm, false)
}

// key: span; value: IDs
type EventSpan struct {
	mtx        *sync.Mutex
	mSpanIDs   map[string][]string
	prevSpan   string
	fnDbAppend func(*EventSpan, bool) error
}

func NewEventSpan(spanType string) *EventSpan {
	if len(spanType) != 0 {
		SetSpanType(spanType)
	}
	return &EventSpan{
		mtx:        &sync.Mutex{},
		mSpanIDs:   make(map[string][]string),
		prevSpan:   "",
		fnDbAppend: SaveEvtSpan,
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

func (es *EventSpan) OnDbAppend(dbUpdate func(*EventSpan, bool) error) {
	es.fnDbAppend = dbUpdate
}

func (es *EventSpan) AddEvent(evt *Event) error {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	dbKey := NowSpan()
	defer func() { es.prevSpan = dbKey }()

	///////////////////////////////////////////////

	if evt.fnDbStore == nil {
		return fmt.Errorf("Event [OnDbStore] must be done before AddEvent")
	}
	if err := evt.fnDbStore(evt, false); err != nil {
		return err
	}
	lk.Log("%v", evt)

	// temp cache ids filling...
	cache = append(cache, TempEvt{
		owner:  evt.Owner,
		yyyymm: evt.Tm.Format("200601"),
		evtId:  evt.ID,
	})

	///////////////////////////////////////////////

	if es.prevSpan != "" && dbKey != es.prevSpan {
		es.Flush(false) // already locked
	}

	es.mSpanIDs[dbKey] = append(es.mSpanIDs[dbKey], evt.ID)
	return nil
}

// only manually use it at exiting...
func (es *EventSpan) Flush(lock bool) error {

	// store a batch of span event IDs
	if es.fnDbAppend == nil {
		return fmt.Errorf("EventSpan [SetDbAppendFunc] must be done before AddEvent")
	}
	if err := es.fnDbAppend(es, lock); err != nil { // store mSpanRefIDs at 'prevSpan'
		return err
	}
	delete(es.mSpanIDs, es.prevSpan)

	// update owner - eventIDs storage
	if err := updateOwn(cache...); err != nil {
		return err
	}

	// temp cache ids clearing...
	cache = cache[:0]

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

func (es *EventSpan) CurrIDs() []string {
	return es.mSpanIDs[NowSpan()]
}

// past: such as "2h20m", "30m", "2s"
// order: "DESC", "ASC"
func FetchEvtIDsByTm(past, order string) (ids []string, err error) {

	ids = FilterMap(cache, nil, func(i int, e TempEvt) string { return e.evtId })
	ids = Reverse(ids)

	psNum := int(strs.SplitPartToNum(PastSpan(past), "-", 0))
	nsNum := int(strs.SplitPartToNum(NowSpan(), "-", 0))

	tsGrp := []string{}
	for i := nsNum; i > psNum; i-- {
		tsGrp = append(tsGrp, fmt.Sprint(i))
	}

	for _, ts := range tsGrp {
		es, err := GetEvtSpan(ts)
		if err != nil {
			lk.WarnOnErr("%v", err)
			return nil, err
		}
		_, vs := Map2KVs(es.mSpanIDs, func(i, j string) bool {
			switch order {
			case "ASC":
				return i < j
			case "DESC":
				return i > j
			default:
				return i > j
			}
		}, nil)
		ids = append(ids, MergeArray(vs...)...)
	}
	return
}

// default IDs period is one week.
// if one week's events is less than [n], return all of week's events
func FetchEvtIDsByCnt(n int, period, order string) (ids []string, err error) {
	if len(period) == 0 {
		period = "168h"
	}
	if len(order) == 0 {
		order = "DESC"
	}
	ids, err = FetchEvtIDsByTm(period, order)
	if err != nil {
		return nil, err
	}
	if len(ids) > n {
		return ids[:n], nil
	}
	return ids, nil
}
