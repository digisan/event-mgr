package eventmgr

// import (
// 	"strings"
// 	"sync"
// )

// type EventSub struct {
// 	smIdSubs   *sync.Map
// 	fnDbAppend func(id, sid string) error
// 	fnDbRemove func(id, sid string) error
// }

// func NewEventSub() *EventSub {
// 	return &EventSub{
// 		smIdSubs:   &sync.Map{},
// 		fnDbAppend: nil,
// 		fnDbRemove: nil,
// 	}
// }

// func (es EventSub) String() string {
// 	sb := strings.Builder{}
// 	es.smIdSubs.Range(func(key, value any) bool {

// 	}
// }
