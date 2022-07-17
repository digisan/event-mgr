package eventmgr

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	bh "github.com/digisan/db-helper/badger"
	. "github.com/digisan/go-generics/v2"
	"github.com/digisan/gotk/misc"
	"github.com/digisan/gotk/strs"
	lk "github.com/digisan/logkit"
)

var (
	onceES      sync.Once
	es          *EventSpan = nil
	curSpanType            = "MINUTE"
)

type TempEvt struct {
	owner  string // uname
	yyyymm string // "202208"
	evtId  string // "uuid"
}

// key: span; value: TempEvts
type EventSpan struct {
	mtx        *sync.Mutex
	mSpanCache map[string][]TempEvt // key: "27582250-1"
}

func (es *EventSpan) BadgerDB() *badger.DB {
	return DbGrp.SpanIDs
}

func (es *EventSpan) Key() []byte {
	panic("JUST PLACE HOLDER, DO NOT INVOKE")
	return []byte("EventSpan")
}

func (es *EventSpan) Marshal(at any) (forKey, forValue []byte) {
	span := at.(string)
	forKey = []byte(span)
	cache := es.mSpanCache[span]
	ids := FilterMap(cache, nil, func(i int, e TempEvt) string { return e.evtId })
	forValue = []byte(strings.Join(ids, SEP))
	return
}

func (es *EventSpan) Unmarshal(dbKey []byte, dbVal []byte) (any, error) {
	if es.mSpanCache == nil {
		es.mSpanCache = make(map[string][]TempEvt)
	}
	key := string(dbKey)
	if dbVal = bytes.TrimSpace(dbVal); len(dbVal) > 0 {
		ids := strings.Split(string(dbVal), SEP)
		for _, id := range ids {
			es.mSpanCache[key] = append(es.mSpanCache[key], TempEvt{
				evtId: id,
			})
		}
		return ids, nil
	}
	return []string{}, nil
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

func SetSpanType(spanType string) error {
	if _, ok := mSpanType[spanType]; !ok {
		return fmt.Errorf("[%v] is an unsupported value", spanType)
	}
	curSpanType = spanType
	return nil
}

func getSpanAt(tm time.Time) string {
	tsMin := tm.Unix() / 60
	sm, ok := mSpanType[curSpanType]
	lk.FailOnErrWhen(!ok, "%v", fmt.Errorf("error at '%s'", curSpanType))
	start := tsMin / sm * sm
	return fmt.Sprintf("%d-%d", start, sm)
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
	return getSpanAt(then)
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
				mtx:        &sync.Mutex{},
				mSpanCache: make(map[string][]TempEvt),
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
	for span, cache := range es.mSpanCache {
		sb.WriteString(span + ": \n")
		for idx, tEvt := range cache {
			sb.WriteString(fmt.Sprintf("\t%04d\t%s\n", idx, tEvt.evtId))
			// sb.WriteString(fmt.Sprintf("\t\t%s\n", tEvt.owner))
			// sb.WriteString(fmt.Sprintf("\t\t%s\n", tEvt.yyyymm))
		}
	}
	return sb.String()
}

// store real event
func AddEvent(evt *Event) error {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	if err := evt.fnDbStore(evt); err != nil {
		return err
	}
	// lk.Log("%v", evt)

	dbKey := NowSpan()
	es.mSpanCache[dbKey] = append(es.mSpanCache[dbKey], TempEvt{
		owner:  evt.Owner,
		yyyymm: evt.Tm.Format("200601"),
		evtId:  evt.ID,
	})

	// lk.Log("after adding ------> span: %s -- id count: %d", dbKey, len(es.mSpanCache[dbKey]))
	return nil
}

// store event indices
func flush(span string) error {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	defer misc.TrackTime(time.Now())

	lk.Log("before flushing ------> span: %s -- id count: %d", span, len(es.mSpanCache[span]))

	// update [owner] - eventIDs storage
	if err := updateOwn(span, es.mSpanCache[span]...); err != nil {
		return err
	}

	// store a batch of span event IDs
	if err := bh.UpsertPartObjectDB(es, span); err != nil { // store mSpanRefIDs at 'prevSpan'
		return err
	}
	delete(es.mSpanCache, span)

	return nil
}

func CurrIDs() []string {
	cache := es.mSpanCache[NowSpan()]
	return FilterMap(cache, nil, func(i int, e TempEvt) string { return e.evtId })
}

func FetchSpans(prefix []byte) (spans []string, err error) {
	mES, err := bh.GetMapDB[EventSpan](prefix, nil)
	if err != nil {
		return nil, err
	}
	spans, _ = Map2KVs(mES, func(i, j string) bool { return i < j }, nil)
	return spans, nil
}

// prefix: span id, e.g. 27632141-1
func FetchEvtIDs(prefix []byte) (ids []string, err error) {
	mES, err := bh.GetMapDB[EventSpan](prefix, nil)
	if err != nil {
		return nil, err
	}
	idsDB := []string{}
	spans, _ := Map2KVs(mES, func(i, j string) bool { return i < j }, nil)
	for _, span := range spans {
		idsDB = append(idsDB, mES[span].([]string)...)
	}
	return append(Reverse(CurrIDs()), idsDB...), nil
}

// past: such as "2h20m", "30m", "2s"
func FetchEvtIDsByTm(past string) (ids []string, err error) {

	psNum := int(strs.SplitPartToNum(PastSpan(past), "-", 0))
	nsNum := int(strs.SplitPartToNum(NowSpan(), "-", 0))

	tsGrp := []string{}
	for i := nsNum; i > psNum; i-- {
		tsGrp = append(tsGrp, fmt.Sprint(i))
	}

	ids = Reverse(CurrIDs())
	for _, ts := range tsGrp {
		idBatch, err := FetchEvtIDs([]byte(ts))
		if err != nil {
			lk.WarnOnErr("%v", err)
			return nil, err
		}
		ids = append(ids, idBatch...)
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
