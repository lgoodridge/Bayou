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
const DEBUG_MODE bool = true

/* Maximum number of characters to use when *
 * printing a log entry's query string      */
const MAX_QUERY_CHARS int = 50

/**************************
 *    ERROR UTILITIES     *
 **************************/

/* Prints error message and crashes if error exists  */
func check(e error, prefix string) {
    if e != nil {
        Log.Fatal(prefix + e.Error())
    }
}

/*************************
 *    FILE UTILITIES     *
 *************************/

/* Returns whether a file exists at the provided path */
func fileExists(filePath string) bool {
    _, err := os.Stat(filePath)
    return err == nil
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

/* Returns a random integer less than max */
func randomIntn(max int) int {
    return random.Intn(max)
}

/************************
 *    TIME UTILITIES    *
 ************************/

/* Format string to use when converting times to strings */
const TIME_FORMAT_STR = "2006-01-02 15:04"

/* Returns a new timeout duration between *
 * minDuration to 2*minDuration ms long   */
func getRandomTimeout(minDuration int) time.Duration {
    millis := minDuration + randomIntn(minDuration)
    return time.Duration(millis) * time.Millisecond
}

/* Returns whether the provided times are equal      *
 * according to precision specified by format string */
func timesEqual(time1 time.Time, time2 time.Time) bool {
    return time1.Format(TIME_FORMAT_STR) == time2.Format(TIME_FORMAT_STR)
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
    queryStr := entry.Query
    if len(queryStr) > MAX_QUERY_CHARS {
        queryStr = queryStr[:MAX_QUERY_CHARS] + "..."
    }
    return fmt.Sprintf("#%d: ", entry.WriteID) + entry.Query
}

/***************************
 *    CLIENT UTILITIES     *
 ***************************/

/* Deserializes the result of a BayouDB read *
 * into a slice of client Room structs       */
func deserializeRooms(rr ReadResult) []Room {
    rooms := make([]Room, len(rr))
    for i, rowData := range rr {
        room := Room{}
        room.Id = string(rowData["Id"].([]byte))
        room.Name = string(rowData["Name"].([]byte))
        room.StartTime = rowData["StartTime"].(time.Time)
        room.EndTime = rowData["EndTime"].(time.Time)
        rooms[i] = room
    }
    return rooms
}

func (room Room) String() string {
    return fmt.Sprintf("Room: #%s, %s, %s, %s", room.Id, room.Name,
            room.StartTime, room.EndTime)
}

