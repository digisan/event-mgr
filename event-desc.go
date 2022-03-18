package eventmgr

// key: refID; value: others
type EventDesc struct {
	id      string
	owner   string
	evtType string
	tmSpan  string
	resPath string
	visGrp  []string
}

// db key order
const (
	MOK_Id int = iota
)

func (ed *EventDesc) KeyFieldAddr(mok int) *string {
	mFldAddr := map[int]*string{
		MOK_Id: &ed.id,
	}
	return mFldAddr[mok]
}

// db value order
const (
	MOV_Owner int = iota
	MOV_EvtType
	MOV_TmSpan
	MOV_ResPath
	MOV_VisGrp
)

func (ed *EventDesc) ValFieldAddr(mov int) *string {
	mFldAddr := map[int]*string{
		MOV_Owner: &ed.owner,
		// ...
	}
	return mFldAddr[mov]
}

////////////////////////////////////////////////////

func NewEventDesc(id, etype, span, path string, visgrp ...string) *EventDesc {
	return &EventDesc{
		id:      id,
		evtType: etype,
		tmSpan:  span,
		resPath: path,
		visGrp:  visgrp,
	}
}

// func (es *EventSpan) MarshalIDsSpan() (forKeys [][]byte, forValues [][]byte) {
// 	for _, id := range es.mSpanIDs[es.prevSpan] {
// 		forKeys = append(forKeys, []byte(id))
// 		forValues = append(forValues, []byte(es.prevSpan))
// 	}
// 	return
// }
