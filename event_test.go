package eventmgr

import (
	"fmt"
	"reflect"
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
	es := NewEventSpan("MINUTE")

	// fmt.Println(es.CurrIDs())

	ticker := time.NewTicker(1 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case t := <-ticker.C:
				fmt.Println("Tick at", t)

				/////////////////////////////////

				//
				// Get *** Event ***
				//
				evt := NewEvent("", "uname", "eType", "metajson")

				/////////////////////////////////

				//
				// TEST *** reading when writing ***
				//
				// es1, err := edb.GetEvtSpan("27510424")
				// if err != nil {
				// 	panic(err)
				// }
				// fmt.Println(es1)

				/////////////////////////////////

				lk.FailOnErr("%v", es.AddEvent(evt))

			case <-done:

				fmt.Println("Flushing......................")

				lk.FailOnErr("%v", es.Flush(true))
				return
			}
		}
	}()

	time.Sleep(2 * time.Minute)
	ticker.Stop()
	done <- true
	fmt.Println("Ticker stopped")

	time.Sleep(2 * time.Second)
}

func TestListEvtSpan(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	es, err := ListEvtSpan()
	if err != nil {
		panic(err)
	}

	fmt.Println(es)
}

func TestGetEvtSpan(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	// fmt.Println(NowSpan())

	es, err := GetEvtSpan("27561528")
	if err != nil {
		panic(err)
	}

	fmt.Println(es)
}

func TestFetchSpanIDsByTime(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

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

	ids, err := FetchOwn("uname", "202206")
	lk.WarnOnErr("%v", err)

	for i, eid := range ids {
		fmt.Println(i, eid)
	}

}
