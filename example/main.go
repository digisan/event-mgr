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
	em.InitEventSpan("TWO_MINUTE", ctx)

	// fmt.Println(es.CurIDs())

	// {
	// 	spans, err := em.FetchSpan(nil)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	for _, span := range spans {
	// 		fmt.Println(span)
	// 	}
	// 	return
	// }

	{
		ids, err := em.FetchEvtIDByTm("100m") // em.FetchEvtID(nil)
		if err != nil {
			fmt.Println(err)
		}
		for _, id := range ids {
			fmt.Println(id)
		}
		return
	}

	{
		fmt.Println(em.DelEvent("ff0b0912-a505-44d7-93aa-fe5b7fd3923e"))
		// fmt.Println(em.EraseEvent("26b9c484-f598-4102-87bf-204dd87603ed"))
		// fmt.Println(em.DelGlobalEventID("8b5c0689-b2a9-4862-87e4-6af1ce4b6f9d"))
		return
	}

	ticker := time.NewTicker(1 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case t := <-ticker.C:
				fmt.Println("Tick at", t)

				/////////////////////////////////

				evt := em.NewEvent("", "uname", "eType", "rawjson", "")
				fmt.Println("Current Span:", em.CreateSpanAt(evt.Tm), "--->", evt.ID)

				/////////////////////////////////

				lk.FailOnErr("%v", em.AddEvent(evt))

			case <-done:
				cancel()
				return
			}
		}
	}()
	time.Sleep(4 * time.Second)
	done <- true
	time.Sleep(1 * time.Second)

	ticker.Stop()
	fmt.Println("Ticker stopped")
}
