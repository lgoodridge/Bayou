package bayou

import (
	"errors"
	"fmt"
	"strings"
)

/************************
 *   TYPE DEFINITIONS   *
 ************************/

/* Vector Clock: parallel array holding a monotonically *
 * increasing logical time (int) for each peer server   */
type VectorClock []int

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
        debugf("WARNING: Vector clocks of different lengths were compared:\n" +
            "This: " + vc.String() + "\tOther: " + other.String())
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

/* Sets all logical clocks to the max of  *
 * this and the other VC's logical clocks */
func (vc VectorClock) Max(other VectorClock) {
    if len(vc) != len(other) {
        debugf("WARNING: Vector clocks of different lengths were maxed:\n" +
            "This: " + vc.String() + "\tOther: " + other.String())
    }
    // Update logical clock if other one is higher,
    // or append to vector clock if other one is longer
    for idx, _ := range other {
        if idx >= len(vc) {
            vc = append(vc, other[idx])
        } else if vc[idx] < other[idx] {
            vc[idx] = other[idx]
        }
    }
}

func (vc VectorClock) String() string {
    return "VC: " +  strings.Trim(strings.Replace(fmt.Sprint(([]int)(vc)),
        " ", ", ", -1), "[]")
}
