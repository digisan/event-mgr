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

	flw := NewEventFollow("001")
	flw.AddFollower("1", "2")
	flw.RmFollower("1")
	fmt.Println(flw)

	fmt.Println(SaveOneObjectDB(flw))

	fmt.Println("-----")

	fmt.Println(GetOneObjectDB[EventFollow]([]byte("001")))
}
