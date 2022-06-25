package eventmgr

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lk "github.com/digisan/logkit"
)

func TestSpan(t *testing.T) {
	fmt.Println(NowSpan())
}

func TestAddEvent(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	InitDB("./data")
	defer CloseDB()

	//
	// Init *** EventSpan ***
	//
	InitEventSpan("MINUTE", ctx)
	// fmt.Println(es.CurrIDs())

	var n uint64

	ticker := time.NewTicker(20 * time.Millisecond)
	done := make(chan bool)
	go func() {
		for {
			select {
			case t := <-ticker.C:
				fmt.Sprintln("Tick at", t)

				go func() {
					//
					// Get *** Event ***
					//
					evt := NewEvent("", "uname", "eType", "metajson")

					//
					// TEST *** reading when writing ***
					//
					// if err := FillEvtSpan("27510424"); err != nil {
					// 	panic(err)
					// }

					/////////////////////////////////

					atomic.AddUint64(&n, 1)

					lk.FailOnErr("%v", AddEvent(evt))
				}()

			case <-done:
				cancel() // here to inform 'flush' final part; if we leave at defer, it doesn't work
				return
			}
		}
	}()

	time.Sleep(60 * time.Second)
	ticker.Stop()
	fmt.Println("Ticker stopped")
	time.Sleep(1 * time.Second)

	done <- true
	time.Sleep(2 * time.Second) // some time for flushing...

	fmt.Println("------> total:", n)
}

// wait for a moment to span changed, then running 'V2'

func TestAddEventV2(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	var n uint64
	var wg sync.WaitGroup

	const N = 1000
	wg.Add(N)

	for i := 0; i < N; i++ {
		go func() {
			evt := NewEvent("", "uname", "eType", "metajson")
			lk.FailOnErr("%v", AddEvent(evt))
			atomic.AddUint64(&n, 1)
			wg.Done()
		}()
	}

	wg.Wait()

	fmt.Println("------> total:", n)

	cancel()

	time.Sleep(2 * time.Second)
}

func TestGetAllEvtIds(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	ids, err := FetchAllEvtIDs() // GetEvtIdAllDB()
	if err != nil {
		panic(err)
	}

	fmt.Println("------> total:", len(ids))
}

func TestGetEvtIdRangeDB(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	// fmt.Println(NowSpan())

	ids, err := GetEvtIdRangeDB("27561528")
	if err != nil {
		panic(err)
	}

	fmt.Println(ids)
}

func TestGetSpanAllDB(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	spans, err := GetSpanAllDB()
	if err != nil {
		panic(err)
	}

	for _, span := range spans {
		fmt.Println("--->", span)
	}
}

func TestFetchSpanIDsByTime(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	ids, err := FetchEvtIDsByTm("40m")
	if err != nil {
		panic(err)
	}
	for j, id := range ids {
		fmt.Println(j, id)
	}
}

func TestFetchSpanIDsByCnt(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	ids, err := FetchEvtIDsByCnt(200, "") // 'a week' period
	if err != nil {
		panic(err)
	}
	for j, id := range ids {
		fmt.Println(j, id)
	}
}

func TestGetEvt(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	id := "03dd1fc3-1abe-45c9-89a3-aa806f10c5d6"

	evt, err := GetEvtDB(id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("---------------\n%v---------------\n", evt)

	evt.OnDbStore(SaveEvtDB)

	if err := evt.Publish(true); err != nil { // make this event public
		panic(err)
	}

	evt, err = GetEvtDB(id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("---------------\n%v---------------\n", evt)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////

func TestMarshal(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitEventSpan("MINUTE", ctx)

	evt := NewEvent("", "cdutwhu", "post", "json doc for event description")

	fmt.Println(evt)

	key, val := evt.Marshal()

	evt1 := &Event{}
	evt1.Unmarshal(key, val)

	fmt.Println("equal", evt == evt1)
	fmt.Println("deep equal", reflect.DeepEqual(evt, evt1))
	fmt.Println()

	fmt.Println(evt1)
}

func TestGetOwnKeysDB(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	keys, err := GetOwnSpanKeysDB("uname", "202206")
	if err != nil {
		panic(err)
	}

	for _, key := range keys {
		fmt.Println("--->", key)
	}
}

func TestFetchOwn(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	ids, err := FetchOwn("uname", "202206")
	lk.WarnOnErr("%v", err)

	fmt.Println("----->", len(ids))

	// for i, eid := range ids {
	// 	fmt.Println(i, eid)
	// }

	time.Sleep(1 * time.Second)
}

func TestFollow(t *testing.T) {
	flw := NewEventFollow("000")
	flw.AddFollower("1", "2")
	fmt.Println(flw)

	flw.RmFollower("2", "3")
	fmt.Println(flw)

	key, val := flw.Marshal()
	flw1 := NewEventFollow("")
	flw1.Unmarshal(key, val)
	fmt.Println(flw1)
}

func TestFollowDB(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	flw := NewEventFollow("000")
	flw.OnDbStore(SaveFlwDB)

	err := flw.AddFollower("1", "2")
	if err == nil {
		fmt.Println(flw)
	} else {
		fmt.Println(err)
	}

	fmt.Println("-------------")

	fmt.Println(GetFlwDB("0"))
}
