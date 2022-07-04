package eventmgr

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// . "github.com/digisan/go-generics/v2"
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
					evt := NewEvent("", "uname", "eType", "rawjson")

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

// wait for a moment (as per 'InitEventSpan') to span changed, then running 'V2' again.

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
			evt := NewEvent("", "uname", "eType", "rawjson")
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

func TestFetchSpans(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	spans, err := FetchSpans([]byte("")) // 'nil' is for fetching all
	if err != nil {
		panic(err)
	}
	for _, span := range spans {
		fmt.Println(span)
	}
}

func TestFetchEvtIds(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	ids, err := FetchEvtIDs([]byte("27615")) // 'nil' is for fetching all
	if err != nil {
		panic(err)
	}

	fmt.Println("------> total:", len(ids))
}

func TestFetchEventIDsByTime(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	ids, err := FetchEvtIDsByTm("20m")
	if err != nil {
		panic(err)
	}
	// for j, id := range ids {
	// 	fmt.Println(j, id)
	// }
	fmt.Println(len(ids))
}

func TestFetchEventIDsByCnt(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	ids, err := FetchEvtIDsByCnt(400, "45m") // 'a week' period default
	if err != nil {
		panic(err)
	}
	// for j, id := range ids {
	// 	fmt.Println(j, id)
	// }
	fmt.Println(len(ids))
}

func TestGetEvt(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	id := "50277fda-7cc4-4c16-8ddc-f8657d75510d"

	evt, err := GetOneObjectDB[Event]([]byte(id))
	if err != nil {
		panic(err)
	}
	fmt.Printf("---------------\n%v---------------\n", evt)

	evt.OnDbStore(UpsertOneObjectDB[Event])

	if err := evt.Publish(true); err != nil { // make this event public
		panic(err)
	}

	evt, err = GetOneObjectDB[Event]([]byte(id))
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

	evt1 := &Event{}
	evt1.Unmarshal(evt.Marshal(nil))

	fmt.Println("equal", evt == evt1)
	fmt.Println("deep equal", reflect.DeepEqual(evt, evt1))
	fmt.Println()

	fmt.Println(evt1)
}

func TestFetchOwn(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	ids, err := FetchOwn("uname", "202207")
	lk.WarnOnErr("%v", err)

	fmt.Println("----->", len(ids))

	// for i, eid := range ids {
	// 	fmt.Println(i, eid)
	// }

	time.Sleep(1 * time.Second)
}

func TestFollow(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	flw := NewEventFollow("000")
	flw.OnDbStore(UpsertOneObjectDB[EventFollow])

	flw.AddFollower("1", "2")
	fmt.Println(flw)

	flw.RmFollower("2", "3")
	fmt.Println(flw)

	flw1 := NewEventFollow("")
	flw1.Unmarshal(flw.Marshal(nil))
	fmt.Println(flw1)
}

func TestFollowDB(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	flw := NewEventFollow("00")
	flw.OnDbStore(UpsertOneObjectDB[EventFollow])

	err := flw.AddFollower("1", "2", "3")
	if err == nil {
		fmt.Println(flw)
	} else {
		fmt.Println(err)
	}

	fmt.Println("-------------")

	fmt.Println(GetOneObjectDB[EventFollow]([]byte("00")))
}

func TestGetFollowers(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	fids, err := GetFollowers("00")
	fmt.Println(fids, err)
}

func TestParticipate(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	ep := NewEventParticipate("001", "thumb")
	ep.OnDbStore(UpsertOneObjectDB[EventParticipate])

	err := ep.AddPtps("A", "a", "b", "c")
	if err == nil {
		fmt.Println(ep)
	} else {
		fmt.Println(err)
	}

	fmt.Println("-------------")

	fmt.Println(GetParticipants("001", "thumb"))

}

func TestGetParticipants(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	ep, _ := GetParticipate("001", "thumb")
	ep.OnDbStore(UpsertOneObjectDB[EventParticipate])
	ep.RmPtps("b", "c", "d")

	ptps, err := GetParticipants("001", "thumb")
	fmt.Println(ptps, err)

}
