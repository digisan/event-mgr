package eventmgr

import (
	"errors"
	"fmt"
	"strings"
	"time"

	lk "github.com/digisan/logkit"
)

// key: Owner-TmYM;
// value: EventIDs ([uuid])
type Own struct {
	Owner     string   // uname
	TmYM      string   // e.g. 2022-06 2024-12
	EventIDs  []string // [uuid]
	fnDbStore func(*Own, bool) error
}

// for load
func NewOwn(owner string, onDbStore func(*Own, bool) error) *Own {
	return &Own{
		Owner:     owner,
		TmYM:      time.Now().Format("2006-01"),
		EventIDs:  []string{},
		fnDbStore: onDbStore,
	}
}

func (own Own) String() string {
	sb := strings.Builder{}
	sb.WriteString(own.Owner + "\n")
	sb.WriteString(own.TmYM + "\n")
	for i, id := range own.EventIDs {
		sb.WriteString(fmt.Sprintf("\t%02d\t%s\n", i, id))
	}
	return sb.String()
}

func (own *Own) Marshal() (forkey, forValue []byte) {
	lk.FailOnErrWhen(len(own.Owner) == 0, "%v", errors.New("empty owner"))
	forkey = []byte(fmt.Sprintf("%s@%s", own.Owner, own.TmYM))
	forValue = []byte(fmt.Sprint(own.EventIDs))
	return
}

func (own *Own) Unmarshal(dbKey, dbVal []byte) error {
	ss := strings.SplitN(string(dbKey), "@", 2)
	own.Owner = ss[0]
	own.TmYM = ss[1]

	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	own.EventIDs = append(own.EventIDs, strings.Split(dbValStr, " ")...)

	return nil
}

func (own *Own) OnDbStore(dbStore func(*Own, bool) error) {
	own.fnDbStore = dbStore
}

////////////////////
