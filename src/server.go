package bayou

import (
	"errors"
	"fmt"
	"net/rpc"
	"strings"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

/************************
 *   TYPE DEFINITIONS   *
 ************************/

/* Go object representing a Bayou Server */
type BayouServer struct {
	// Unique index into peers array
	id	  int
	// Represents the other bayou servers
	peers []*rpc.Client

	// Current logical time
	currTime VectorClock

	// Stores all written ops and their corresponding undo ops
	WriteLog []LogEntry
	UndoLog	 []LogEntry
	// Operations that conflict and fail to merge are stored here
	ErrorLog []LogEntry

	// Maintains ID of latest write discarded from each server
	Omitted	[]int
}

/* Vector Clock: parallel array holding a monotonically *
 * increasing logical time (int) for each peer server   */
type VectorClock []int

/* Represents an entry in a Bayou server log */
type LogEntry struct {
	WriteID	  int
	Committed bool
	Timestamp VectorClock
}

/* Implements sort interface for log entries      *
 * Sorts by committed vs. tentative, then by time */
type ByCommitedAndTime []LogEntry

/* AntiEntropy RPC arguments structure */
type AntiEntropyArgs struct {
}

/* AntiEntropy RPC reply structure */
type AntiEntropyReply struct {
}

/* Update or Undo function type:	  *
 * Takes a database, and returns void */
type operation func(*sql.DB)

/* Dependency check function type:      *
 * Takes a database, and returns a bool */
type depcheck func(*sql.DB) bool

/* Merge process function type:       *
 * Takes a database, and returns void */
type mergeproc func(*sql.DB)

/****************************
 *   BAYOU SERVER METHODS   *
 ****************************/

/****************************
 *   VECTOR CLOCK METHODS   *
 ****************************/

/* Returns a new vector clock of the specified length */
func NewVectorClock(length int) VectorClock {
	return make([]int, length)
}

/* Sets the logical time at idx to specified value        *
 * Returns an error if newTime is less than current value */
func (vc VectorClock) SetTime(idx int, newTime int) error {
	if (newTime < vc[idx]) {
		return errors.New("SetTime Failed: New time less than current time")
	}
	vc[idx] = newTime
	return nil
}

/* Increments the logical time at idx */
func (vc VectorClock) Inc(idx int) {
	vc[idx] = vc[idx] + 1
}

/* Returns whether this vector clock is *
 * strictly "less than" the other one   */
func (vc VectorClock) LessThan(other VectorClock) bool {
	if len(vc) != len(other) {
		return false
	}
	// vc is less than other iff each logical time is less
	// than or equal to the other's logical time for each
	// peer, and at least one of those is strictly less than
	strictly_less_seen := false
	for idx, _ := range vc {
		if !strictly_less_seen && vc[idx] < other[idx] {
			strictly_less_seen = true
		}
		if (vc[idx] > other[idx]) {
			return false
		}
	}
	return strictly_less_seen
}

func (vc VectorClock) String() string {
	return "VC: " +  strings.Trim(strings.Replace(fmt.Sprint(([]int)(vc)),
		" ", ", ", -1), "[]")
}

/***************************
 *   LOG SORTING METHODS   *
 ***************************/

