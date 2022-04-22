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
	es := NewEventSpan()
	es.DbAppendFunc(edb.SaveEvtSpan)
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

				//
				// Get *** Event ***
				//
				evt := NewEvent("", "uname", "eType", "metajson", edb.SaveEvt)

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

	id := "048a3587-c842-42da-a5cf-c9d097710963"

	evt, err := edb.GetEvt(id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("---------------\n%v---------------\n", evt)

	evt.OnDbStore(edb.SaveEvt)
	if err := evt.Publish(true); err != nil {
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
