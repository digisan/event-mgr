package main

import (
	"context"
	"fmt"
	"time"

	em "github.com/digisan/event-mgr"
	lk "github.com/digisan/logkit"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	em.InitDB("./data")
	defer em.CloseDB()

	//
	// Init *** EventSpan ***
	//
	em.InitEventSpan("MINUTE", ctx)

	// fmt.Println(es.CurIDs())

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
				evt := em.NewEvent("", "uname", "eType", "rawjson", "")

				/////////////////////////////////

				lk.FailOnErr("%v", em.AddEvent(evt))
			case <-done:
				cancel()
				return
			}
		}
	}()

	time.Sleep(1 * time.Minute)
	ticker.Stop()
	done <- true
	fmt.Println("Ticker stopped")
}
