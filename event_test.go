package eventmgr

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lk "github.com/digisan/logkit"
)

func TestAddEvent(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	//
	// Init *** EventSpan ***
	//
	InitEventSpan("MINUTE")
	// fmt.Println(es.CurrIDs())

	var n uint64

	ticker := time.NewTicker(10 * time.Millisecond)
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

					/////////////////////////////////

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
				lk.FailOnErr("%v", Flush(true))
				return
			}
		}
	}()

	time.Sleep(1 * time.Minute)
	ticker.Stop()
	fmt.Println("Ticker stopped")
	time.Sleep(1 * time.Second)

	done <- true
	time.Sleep(10 * time.Second) // some time for flushing...

	fmt.Println("------> total:", n)
}

func TestAddEventV2(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE")

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

	Flush(true)

	fmt.Println("------> total:", n)
}

func TestListEvtSpan(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE")

	if err := ListEvtSpan(); err != nil {
		panic(err)
	}

	n := 0
	for _, ids := range es.mSpanIDs {
		n += len(ids)
	}
	fmt.Println("------> total:", n)
}

func TestGetEvtSpan(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE")

	// fmt.Println(NowSpan())

	if err := FillEvtSpan("27561528"); err != nil {
		panic(err)
	}

	fmt.Println(es)
}

func TestFetchSpanIDsByTime(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE")

	SetSpanType("MINUTE")
	ids, err := FetchEvtIDsByTm("40m", "DESC")
	if err != nil {
		panic(err)
	}
	for j, id := range ids {
		fmt.Println(j, id)
	}
}

func TestFetchSpanIDsByCnt(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE")

	SetSpanType("MINUTE")
	ids, err := FetchEvtIDsByCnt(200, "", "") // 'a week' period, 'DESC' sort
	if err != nil {
		panic(err)
	}
	for j, id := range ids {
		fmt.Println(j, id)
	}
}

func TestGetEvt(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE")

	id := "03dd1fc3-1abe-45c9-89a3-aa806f10c5d6"

	evt, err := GetEvt(id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("---------------\n%v---------------\n", evt)

	evt.OnDbStore(SaveEvt)

	if err := evt.Publish(true); err != nil { // make this event public
		panic(err)
	}

	evt, err = GetEvt(id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("---------------\n%v---------------\n", evt)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////

func TestMarshal(t *testing.T) {
	InitEventSpan("MINUTE")

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

func TestFetchOwn(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	InitEventSpan("MINUTE")

	ids, err := FetchOwn("uname", "202206")
	lk.WarnOnErr("%v", err)

	for i, eid := range ids {
		fmt.Println(i, eid)
	}

}
