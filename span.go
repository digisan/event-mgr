package eventmgr

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/digisan/go-generics/v2"
	"github.com/digisan/gotk/misc"
	"github.com/digisan/gotk/strs"
	lk "github.com/digisan/logkit"
)

type TempEvt struct {
	owner  string
	yyyymm string
	evtId  string
}

// key: span; value: IDs
type EventSpan struct {
	mtx      *sync.Mutex
	mSpanIDs map[string][]string
}

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

var (
	onceES      sync.Once
	es          *EventSpan = nil
	curSpanType            = "MINUTE"
	cache                  = []TempEvt{}
)

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

func InitEventSpan(spanType string, ctx context.Context) {
	if len(spanType) != 0 {
		SetSpanType(spanType)
	}
	if es == nil {
		onceES.Do(func() {
			es = &EventSpan{
				mtx:      &sync.Mutex{},
				mSpanIDs: make(map[string][]string),
			}
			// <--- flush monitor ---> //
			ticker := time.NewTicker(time.Duration(1 * int64(time.Second)))
			pSpan := ""
			go func() {
				for {
					nSpan := NowSpan()
					select {
					case <-ctx.Done():
						lk.FailOnErr("%v", flush(nSpan))
						ticker.Stop()
						return
					case <-ticker.C:
						if nSpan != pSpan && pSpan != "" {
							lk.FailOnErr("%v", flush(pSpan))
						}
						pSpan = nSpan
					}
				}
			}()
		})
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

func AddEvent(evt *Event) error {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	dbKey := NowSpan()

	if evt.fnDbStore == nil {
		return fmt.Errorf("Event [OnDbStore] must be done before AddEvent")
	}
	if err := evt.fnDbStore(evt); err != nil {
		return err
	}
	// lk.Log("%v", evt)

	// temp cache ids filling...
	cache = append(cache, TempEvt{
		owner:  evt.Owner,
		yyyymm: evt.Tm.Format("200601"),
		evtId:  evt.ID,
	})

	es.mSpanIDs[dbKey] = append(es.mSpanIDs[dbKey], evt.ID)
	return nil
}

func flush(span string) error {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	defer misc.TrackTime(time.Now())

	lk.Log("before flushing ------> span: %s -- id count: %d", span, len(es.mSpanIDs[span]))

	// store a batch of span event IDs
	if err := SaveEvtSpanDB(span); err != nil { // store mSpanRefIDs at 'prevSpan'
		return err
	}
	delete(es.mSpanIDs, span)

	// update owner - eventIDs storage
	if err := updateOwn(cache...); err != nil {
		return err
	}

	// temp cache ids clearing...
	cache = cache[:0]

	return nil
}

func MarshalAt(span string) (forKey, forValue []byte) {
	forKey = []byte(span)
	forValue = []byte(strings.Join(es.mSpanIDs[span], SEP))
	return
}

func CurrIDs() []string {
	return es.mSpanIDs[NowSpan()]
}

// past: such as "2h20m", "30m", "2s"
func FetchEvtIDsByTm(past string) (ids []string, err error) {

	ids = FilterMap(cache, nil, func(i int, e TempEvt) string { return e.evtId })
	ids = Reverse(ids)

	psNum := int(strs.SplitPartToNum(PastSpan(past), "-", 0))
	nsNum := int(strs.SplitPartToNum(NowSpan(), "-", 0))

	tsGrp := []string{}
	for i := nsNum; i > psNum; i-- {
		tsGrp = append(tsGrp, fmt.Sprint(i))
	}

	for _, ts := range tsGrp {
		idsEach, err := GetEvtSpanDB(ts)
		if err != nil {
			lk.WarnOnErr("%v", err)
			return nil, err
		}
		ids = append(ids, idsEach...)
	}
	return
}

// default IDs period is one week. [period] is like '10h', '500m' etc.
// if events count is less than [n], return all events
func FetchEvtIDsByCnt(n int, period string) (ids []string, err error) {
	if len(period) == 0 {
		period = "168h"
	}
	ids, err = FetchEvtIDsByTm(period)
	if err != nil {
		return nil, err
	}
	if len(ids) > n {
		return ids[:n], nil
	}
	return ids, nil
}
