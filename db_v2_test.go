package eventmgr

import (
	"fmt"
	"testing"
)

func TestGetObjectDB(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	flw := NewEventFollow("300")

	flw.AddFollower("1", "2")
	flw.RmFollower("2")
	fmt.Println(flw)

	fmt.Println(UpsertOneObjectDB(flw))

	fmt.Println("-----")

	ef, err := GetOneObjectDB[EventFollow]([]byte("300"))
	if err != nil {
		panic(err)
	}
	fmt.Println(ef)

	// fmt.Println(DeleteOneObjectDB[EventFollow]([]byte("400")))
}

func TestGetAllObjectsDB(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	mEvtFlws, err := GetMapDB[EventFollow]([]byte("3"))
	if err != nil {
		panic(err)
	}
	fmt.Println(mEvtFlws)

}
