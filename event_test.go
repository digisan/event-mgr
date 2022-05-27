package eventmgr

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	lk "github.com/digisan/logkit"
)

func TestAddEvent(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	//
	// Init *** EventSpan ***
	//
	es := NewEventSpan("MINUTE", edb.SaveEvtSpan)

	// fmt.Println(es.CurrIDs())

	ticker := time.NewTicker(3 * time.Second)
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
				evt := NewEvent("", "uname", "eType", "metajson", edb.SaveEvt)

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
				lk.FailOnErr("%v", es.Flush())
				return
			}
		}
	}()

	time.Sleep(2 * time.Minute)
	ticker.Stop()
	done <- true
	fmt.Println("Ticker stopped")
}

func TestListEvtSpan(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	es, err := edb.ListEvtSpan()
	if err != nil {
		panic(err)
	}

	fmt.Println(es)
}

func TestGetEvtSpan(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	// fmt.Println(NowSpan())

	es, err := edb.GetEvtSpan("27548779")
	if err != nil {
		panic(err)
	}

	fmt.Println(es)
}

func TestFetchSpanIDsByTime(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	SetSpanType("MINUTE")
	ids, err := FetchEvtIDsByTime(edb, "4m", "DESC")
	if err != nil {
		panic(err)
	}
	for j, id := range ids {
		fmt.Println(j, id)
	}
}

func TestFetchSpanIDsByCnt(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	SetSpanType("MINUTE")
	ids, err := FetchEvtIDsByCnt(edb, 20, "DESC")
	if err != nil {
		panic(err)
	}
	for j, id := range ids {
		fmt.Println(j, id)
	}
}

func TestGetEvt(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	id := "03dd1fc3-1abe-45c9-89a3-aa806f10c5d6"

	evt, err := edb.GetEvt(id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("---------------\n%v---------------\n", evt)

	evt.OnDbStore(edb.SaveEvt)
	if err := evt.Publish(true); err != nil { // make this event public
		panic(err)
	}

	evt, err = edb.GetEvt(id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("---------------\n%v---------------\n", evt)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////

func TestMarshal(t *testing.T) {
	evt := NewEvent("", "cdutwhu", "post", "json doc for event description", nil)

	fmt.Println(evt)

	key, val := evt.Marshal()

	evt1 := &Event{}
	evt1.Unmarshal(key, val)

	fmt.Println("equal", evt == evt1)
	fmt.Println("deep equal", reflect.DeepEqual(evt, evt1))
	fmt.Println()

	fmt.Println(evt1)
}
