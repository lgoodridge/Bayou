package bayou

import (
    "fmt"
    "io/ioutil"
    "log"
    "math/rand"
    "os"
    "time"
)

/********************
 *    CONSTANTS     *
 ********************/

/* Whether to display debug output */
const DEBUG_MODE bool = false

/**************************
 *    ERROR UTILITIES     *
 **************************/

/* Prints error message and crashes if error exists  */
func check(e error, prefix string) {
    if e != nil {
        Log.Fatal(prefix + e.Error())
    }
}

/****************************
 *    LOGGING UTILITIES     *
 ****************************/

/* Custom logger (prints without timestamp) */
var Log *log.Logger

func init() {
    Log = log.New(os.Stderr, "", 0)
}

/* Prints output if DEBUG_MODE is true */
func debugf(format string, a ...interface{}) {
    if DEBUG_MODE {
        Log.Printf(format, a...)
    }
}

/* Disables the standard log */
func disableStdLog() {
    log.SetOutput(ioutil.Discard)
}

/* Restore standard log */
func restoreStdLog() {
    log.SetOutput(os.Stderr)
}

/*******************************
 *    RANDOMNESS UTILITIES     *
 *******************************/

/* Source of randomness used by bayou package */
var random *rand.Rand

func init() {
    random = rand.New(rand.NewSource(time.Now().Unix()))
}

/* Returns a random integer */
func randomInt() int {
    return random.Int()
}

/******************************
 *    BAYOU LOG UTILITIES     *
 ******************************/

/* Returns the length of the log at the provided timestamp   *
 * aka The number of entries that occurred by the given time */
func getLengthAtTime(log []LogEntry, targetTimestamp VectorClock) int {
    var searchIndex int
    for searchIndex = len(log) - 1; searchIndex >= 0; searchIndex-- {
        if targetTimestamp.LessThan(log[searchIndex].Timestamp) {
            break
        }
    }
    return searchIndex + 1
}

func logToString(log []LogEntry) string {
    logStr := ""
    for _, entry := range log {
        logStr = logStr + entry.String() + "\n"
    }
    return logStr
}

func (entry LogEntry) String() string {
    return fmt.Sprintf("#%d: ", entry.WriteID) + entry.Op.Desc
}

