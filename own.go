package eventmgr

import (
	"errors"
	"fmt"
	"strings"

	lk "github.com/digisan/logkit"
)

// key:   Owner@YYYYMM
// value: EventIDs ([uuid])
type Own struct {
	OwnerYM   string           // uname@202206
	EventIDs  []string         // [uuid]
	fnDbStore func(*Own) error // in db.go
}

func (own Own) String() string {
	sb := strings.Builder{}
	sb.WriteString(own.OwnerYM + "\n")
	for i, id := range own.EventIDs {
		sb.WriteString(fmt.Sprintf("\t%02d\t%s\n", i, id))
	}
	return sb.String()
}

func (own *Own) Marshal() (forkey, forValue []byte) {
	lk.FailOnErrWhen(len(own.OwnerYM) == 0, "%v", errors.New("empty owner"))
	forkey = []byte(own.OwnerYM)
	forValue = []byte(fmt.Sprint(own.EventIDs))
	return
}

func (own *Own) Unmarshal(dbKey, dbVal []byte) error {
	own.OwnerYM = string(dbKey)

	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	own.EventIDs = append(own.EventIDs, strings.Split(dbValStr, " ")...)

	return nil
}

func (own *Own) OnDbStore(dbStore func(*Own) error) {
	own.fnDbStore = dbStore
}

func updateOwn(tmpEvts ...TempEvt) error {
	for _, evt := range tmpEvts {
		own, err := GetOwnDB(evt.owner, evt.yyyymm)
		if err != nil {
			return err
		}

		own.OnDbStore(SaveOwnDB)

		own.OwnerYM = evt.owner + "@" + evt.yyyymm
		own.EventIDs = append([]string{evt.evtId}, own.EventIDs...)

		err = own.fnDbStore(own)
		if err != nil {
			return err
		}
	}
	return nil
}

func FetchOwn(owner, yyyymm string) ([]string, error) {
	own, err := GetOwnDB(owner, yyyymm)
	if err != nil {
		return nil, err
	}
	return own.EventIDs, nil
}
