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

	// Init *** EventSpan ***
	//
	es := NewEventSpan()
	es.SetDbAppendFunc(edb.SaveEvtSpan)
	es.SetSpan("MINUTE")

	// fmt.Println(es.CurrentIDS())

	ticker := time.NewTicker(1 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case t := <-ticker.C:
				fmt.Println("Tick at", t)

				/////////////////////////////////

				// Get *** Event ***
				//
				evt := NewEvent("uname", "eType", "metajson")
				evt.SetDbAppendFunc(edb.SaveEvt)

				/////////////////////////////////

				lk.FailOnErr("%v", es.AddEvent(evt))
			case <-done:
				lk.FailOnErr("%v", es.Flush())
				return
			}
		}
	}()

	time.Sleep(1 * time.Minute)
	ticker.Stop()
	done <- true
	fmt.Println("Ticker stopped")
}

func TestListEvent(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	eb, err := edb.ListEvtSpan()
	if err != nil {
		panic(err)
	}

	fmt.Println(eb)

}

func TestGetEvt(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	evt, err := edb.GetEvt("499baeb5-ebc7-40ed-9e1f-79e538ee776e")
	if err != nil {
		panic(err)
	}

	fmt.Println("---------------", *evt)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////

func TestMarshal(t *testing.T) {
	evt := &Event{
		id:       "12345",
		owner:    "cdutwhu",
		evtType:  "post",
		metaJSON: "json doc for event description",
		publish:  true,
	}

	fmt.Println(evt)

	key, val := evt.Marshal()

	evt1 := &Event{}
	evt1.Unmarshal(key, val)

	fmt.Println("equal", evt == evt1)
	fmt.Println("deep equal", reflect.DeepEqual(evt, evt1))

	fmt.Println(evt1)
}
