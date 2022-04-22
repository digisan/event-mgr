package main

import (
	"fmt"
	"time"

	em "github.com/digisan/event-mgr"
	lk "github.com/digisan/logkit"
)

func main() {

	edb := em.GetDB("./data")
	defer edb.Close()

	//
	// Init *** EventSpan ***
	//
	es := em.NewEventSpan()
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
				evt := em.NewEvent("", "uname", "eType", "metajson", edb.SaveEvt)

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
