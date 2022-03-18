package eventmgr

import (
	"fmt"
	"testing"
	"time"

	lk "github.com/digisan/logkit"
	"github.com/google/uuid"
)

func TestAddEvent(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	es := NewEventSpan()
	es.SetDbAppendFunc(edb.AppendEvtToSpan)
	es.SetSpan("MINUTE")

	// fmt.Println(es.CurrentIDS())

	ticker := time.NewTicker(1 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				lk.FailOnErr("%v", es.Flush())
				return
			case t := <-ticker.C:
				id := uuid.NewString()
				fmt.Println("Tick at", t, id)
				lk.FailOnErr("%v", es.AddEvent(id, nil))
			}
		}
	}()

	time.Sleep(2 * time.Minute)
	ticker.Stop()
	done <- true
	fmt.Println("Ticker stopped")
}

func TestListEvent(t *testing.T) {

	edb := GetDB("./data")
	defer edb.Close()

	eb, err := edb.ListEventBlock()
	if err != nil {
		panic(err)
	}

	fmt.Println(eb)

}
