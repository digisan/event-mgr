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
	// fmt.Println(es.CurIDs())

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
					evt := NewEvent("", "uname", "eType", "rawjson", "")

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
	// defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	var n uint64
	var wg sync.WaitGroup

	const N = 10
	wg.Add(N)

	flwee := ""
	ef := newEventFollow(flwee)
	for i := 0; i < N; i++ {
		go func(i int) {

			evt := NewEvent("", "uname", "eType", "rawjson", flwee) // if followee exists, span-db excludes
			fmt.Println("auto event id:", evt.ID)

			if len(flwee) > 0 && ef != nil && len(ef.evtFlwee) > 0 {
				lk.FailOnErr("%v", ef.AddFollower(evt.ID))
			}

			lk.FailOnErr("%v", AddEvent(evt))
			atomic.AddUint64(&n, 1)
			wg.Done()
		}(i)
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

	ids, err := FetchEvtIDs([]byte("")) // 'nil' is for fetching all
	if err != nil {
		panic(err)
	}

	fmt.Println("------> total:", len(ids))
	if len(ids) > 0 {
		fmt.Println("------> first:", ids[0])
	}
	fmt.Println()
	if len(ids) <= 20 {
		for i, id := range ids {
			fmt.Printf("%02d -- %s\n", i, id)
		}
	}
}

func TestFetchEventIDsByTime(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	ids, err := FetchEvtIDsByTm("80m")
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
	if len(ids) > 0 {
		fmt.Println(ids[0])
	}
}

var (
	id  = "3912825f-e0a5-4236-b36a-7cbc72d61d66"
	id1 = "4b46db73-4650-4249-a19a-bd189b4616c5"
)

func TestGetEvt(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	for _, id := range []string{id, id1} {

		evt, err := FetchEvent(true, id)
		if err != nil {
			panic(err)
		}
		if evt == nil {
			fmt.Printf("Could NOT find [%s]\n", id)
			continue
		}
		fmt.Printf("---------------\n%v---------------\n", evt)

		fmt.Println("Publishing...")
		if err := PubEvent(id, true); err != nil { // make this event public
			panic(err)
		}

		evt, err = FetchEvent(true, id)
		if err != nil {
			panic(err)
		}
		if evt == nil {
			fmt.Printf("Could NOT find [%s]\n", id)
			continue
		}
		fmt.Printf("---------------\n%v---------------\n", evt)
	}
}

func TestDelEvent(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	fmt.Println(DelEvent(id, id1))
}

func TestEraseEvents(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE", ctx)

	fmt.Println(EraseEvents(id, id1))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////

func TestMarshal(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	InitEventSpan("MINUTE", ctx)

	evt := NewEvent("", "cdutwhu", "post", "json doc for event description", "")
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

	ids, err := FetchOwn("uname", "202208")
	lk.WarnOnErr("%v", err)

	fmt.Println("----->", len(ids))

	if len(ids) <= 20 {
		for i, id := range ids {
			fmt.Println(i, id)
		}
	}

	time.Sleep(1 * time.Second)
}

func TestFollow(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	flw, err := NewEventFollow("3912825f-e0a5-4236-b36a-7cbc72d61d66", true)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = flw.AddFollower("10", "20", "30", "40")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(flw)

	fmt.Println("---------------------------------------------")

	flw.RmFollower("20")
	fmt.Println(flw)
}

func TestGetFollowers(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	fids, err := Followers("3912825f-e0a5-4236-b36a-7cbc72d61d66")
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, id := range fids {
		fmt.Println(id)
	}
}

func TestParticipate(t *testing.T) {

	// ctx, cancel := context.WithCancel(context.Background())
	// InitEventSpan("MINUTE", ctx)
	// defer cancel()

	InitDB("./data")
	defer CloseDB()

	// evt := NewEvent("001", "self", "event-type", "raw", "")
	// err := AddEvent(evt)
	// if err != nil {
	// 	panic(err)
	// }

	ep, err := NewEventParticipate("001", true)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = ep.AddParticipants("thumb", "a", "b", "c")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("original:\n", ep)

	fmt.Println("-------------")

	fmt.Println(ep.Participants("thumb"))

	fmt.Println("-------------")

	if _, err = ep.ToggleParticipant("thumb", "D"); err != nil {
		fmt.Println(err)
	}

	fmt.Println("after toggle:\n", ep)
}

func TestGetParticipants(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	id := "001"
	ep, err := Participate(id)
	if err != nil {
		panic(err)
	}
	if ep == nil {
		fmt.Printf("Could NOT find [%s]\n", id)
		return
	}
	ep.RmParticipants("thumb", "b", "c", "d")

	ptps, err := ep.Participants("thumb")
	fmt.Println(ptps, err)
}

func TestBookmark(t *testing.T) {

	// e34bab66-1884-43e9-b4a6-edfb38ca0dcd
	// 19b1ca66-6be0-46bf-8c67-5cd62c12adc1
	// 281aee18-debb-4fd9-bb56-6f0e5c20e9b7

	InitDB("./data")
	defer CloseDB()

	bm, err := NewBookmark("qmiao", true)
	if err != nil {
		panic(err)
	}

	// if err := bm.AddEvent("e34bab66-1884-43e9-b4a6-edfb38ca0dcd"); err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// if err := bm.AddEvent("19b1ca66-6be0-46bf-8c67-5cd62c12adc1"); err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// if err := bm.AddEvent("281aee18-debb-4fd9-bb56-6f0e5c20e9b7"); err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// n, err := bm.RemoveEvent("e34bab66-1884-43e9-b4a6-edfb38ca0dcd")
	// fmt.Println(n, err)

	has, err := bm.ToggleEvent("281aee18-debb-4fd9-bb56-6f0e5c20e9b7")
	fmt.Println(has, err)

	fmt.Println(bm.Bookmarks("desc"))
}
