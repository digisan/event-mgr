package eventmgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	bh "github.com/digisan/db-helper/badger"
	. "github.com/digisan/go-generics/v2"
	"github.com/digisan/gotk/strs"
	"github.com/digisan/gotk/track"
	lk "github.com/digisan/logkit"
)

var (
	onceES      sync.Once
	es          *EventSpan = nil
	curSpanType            = "MINUTE"
	rtm                    = regexp.MustCompile(`^\d+[hms]$`)
)

type TempEvt struct {
	owner  string // uname
	yyyymm string // "202208"
	evtID  string // "uuid"
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
	ids := FilterMap(cache, nil, func(i int, e TempEvt) string { return e.evtID })
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
				evtID: id,
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
			sb.WriteString(fmt.Sprintf("\t%04d\t%s\n", idx, tEvt.evtID))
			// sb.WriteString(fmt.Sprintf("\t\t%s\n", tEvt.owner))
			// sb.WriteString(fmt.Sprintf("\t\t%s\n", tEvt.yyyymm))
		}
	}
	return sb.String()
}

// store real event, but excludes follower events
func AddEvent(evt *Event) error {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	// (we store all events (original & followers) into event db)
	if err := evt.fnDbStore(evt); err != nil {
		return err
	}
	// lk.Log("%v", evt)

	// register event-ids into span, (we only register original events into span db)
	if len(evt.Followee) == 0 {
		dbKey := NowSpan()
		es.mSpanCache[dbKey] = append(es.mSpanCache[dbKey], TempEvt{
			owner:  evt.Owner,
			yyyymm: evt.Tm.Format("200601"),
			evtID:  evt.ID,
		})
	}

	// lk.Log("after adding ------> span: %s -- id count: %d", dbKey, len(es.mSpanCache[dbKey]))
	return nil
}

// store event indices
func flush(span string) error {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	if n := len(es.mSpanCache[span]); n > 0 {

		defer track.TrackTime(time.Now())
		lk.Log("before flushing ------> span: [%s] -- id count: [%d]", span, n)

		// update [owner] - eventIDs storage
		if err := streamUpdateOwn(span, es.mSpanCache[span]...); err != nil {
			return err
		}

		// store a batch of span event IDs
		if err := bh.UpsertPartObject(es, span); err != nil { // store mSpanRefIDs at 'prevSpan'
			return err
		}
	}

	delete(es.mSpanCache, span)

	return nil
}

func CurIDs() []string {
	cache := es.mSpanCache[NowSpan()]
	return FilterMap(cache, nil, func(i int, e TempEvt) string { return e.evtID })
}

func FetchSpans(prefix []byte) (spans []string, err error) {
	mES, err := bh.GetMap[EventSpan](prefix, nil)
	if err != nil {
		return nil, err
	}
	spans, _ = MapToKVs(mES, func(i, j string) bool { return i < j }, nil)
	return spans, nil
}

// prefix: span id, e.g. 27632141-1
func FetchEvtIDs(prefix []byte) (ids []string, err error) {
	mES, err := bh.GetMap[EventSpan](prefix, nil)
	if err != nil {
		return nil, err
	}
	idsDB := []string{}
	spans, _ := MapToKVs(mES, func(i, j string) bool { return i < j }, nil)
	for _, span := range spans {
		idsDB = append(idsDB, mES[span].([]string)...)
	}
	return append(Reverse(CurIDs()), idsDB...), nil
}

// past: such as "2h20m", "30m", "2s"
func FetchEvtIDsByTm(past string) (ids []string, err error) {

	psNum := strs.SplitPartTo[int](PastSpan(past), "-", 0)
	nsNum := strs.SplitPartTo[int](NowSpan(), "-", 0)

	tsGrp := []string{}
	for i := nsNum; i > psNum; i-- {
		tsGrp = append(tsGrp, fmt.Sprint(i))
	}

	ids = Reverse(CurIDs())
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

// [period] is like []"10h", "500m", "10s"] etc.
// if events count is less than [n], return all events
func FetchEvtIDsByCnt(n int, periods ...string) (ids []string, err error) {
	if len(periods) == 0 {
		periods = []string{"10m", "1h", "12h", "24h"}
	}
	for _, period := range periods {
		if !rtm.MatchString(period) {
			return nil, errors.New("periods must be like '1h', '50m', '20s'")
		}
		ids, err = FetchEvtIDsByTm(period)
		if err != nil {
			return nil, err
		}
		if len(ids) >= n {
			return ids[:n], nil
		}
	}
	return ids, nil
}
