package eventmgr

import (
	"fmt"
	"testing"
)

func TestGetObjectDB(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	// flw := NewEventFollow("000")
	// flw.OnDbStore(SaveFlwDB) // for add/remove etc

	flw := NewEventFollow("400")
	flw.OnDbStore(UpsertOneObjectDB[EventFollow])

	flw.AddFollower("1", "2")
	flw.RmFollower("2")
	fmt.Println(flw)

	fmt.Println(UpsertOneObjectDB(flw))

	fmt.Println("-----")

	ef, err := GetOneObjectDB[EventFollow]([]byte("400"))
	if err != nil {
		panic(err)
	}
	fmt.Println(ef)

	fmt.Println(DeleteOneObjectDB[EventFollow]([]byte("400")))
}

func TestGetAllObjectsDB(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	evtflws, err := GetObjectsDB[EventFollow]([]byte("400"))
	if err != nil {
		panic(err)
	}
	for _, evtflw := range evtflws {
		fmt.Println(evtflw)
		fmt.Println("-----")
	}

}
