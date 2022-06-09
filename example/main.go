package main

import (
	"fmt"
	"time"

	em "github.com/digisan/event-mgr"
	lk "github.com/digisan/logkit"
)

func main() {

	em.InitDB("./data")
	defer em.CloseDB()

	//
	// Init *** EventSpan ***
	//
	es := em.NewEventSpan("MINUTE", em.SaveEvtSpan)

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
				evt := em.NewEvent("", "uname", "eType", "metajson", em.SaveEvt)

				/////////////////////////////////

				lk.FailOnErr("%v", es.AddEvent(evt))
			case <-done:
				lk.FailOnErr("%v", es.Flush(true))
				return
			}
		}
	}()

	time.Sleep(1 * time.Minute)
	ticker.Stop()
	done <- true
	fmt.Println("Ticker stopped")
}
