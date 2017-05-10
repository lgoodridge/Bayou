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
const MAX_QUERY_CHARS int = 300

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

/* Returns whether two log entries have the same content *
 * Also checks timestamp equality if checkTime is true   */
func entriesAreEqual(entry1 LogEntry, entry2 LogEntry, checkTime bool) bool {
    contentEqual := (entry1.WriteID == entry2.WriteID) &&
            (entry1.Query == entry2.Query) &&
            (entry1.Check == entry2.Check) &&
            (entry1.Merge == entry2.Merge)
    if checkTime && contentEqual {
        if len(entry1.Timestamp) != len(entry2.Timestamp) {
            return false
        }
        for idx, _ := range entry1.Timestamp {
            if entry1.Timestamp[idx] != entry2.Timestamp[idx] {
                return false
            }
        }
    }
    return contentEqual
}

func NewLogEntry(writeID int, vclock VectorClock, query string,
        check string, merge string) LogEntry {
    // Make defensive copy of VectorClock
    copyclock := NewVectorClock(len(vclock))
    copy(copyclock, vclock)
    return LogEntry{writeID, copyclock, query, check, merge}
}

func (entry LogEntry) String() string {
    queryStr := entry.Query
    if len(queryStr) > MAX_QUERY_CHARS {
        queryStr = queryStr[:MAX_QUERY_CHARS] + "..."
    }
    return fmt.Sprintf("#%d: ", entry.WriteID) + "\n" +
            entry.Timestamp.String() + "\n" + queryStr
}

/***************************
 *    CLIENT UTILITIES     *
 ***************************/

/* Returns a query string that inserts a room   *
 * into the database wwith provided information */
func getInsertQuery(room Room) string {
    return fmt.Sprintf(`
        INSERT OR REPLACE INTO rooms(
            Id,
            Name,
            StartTime,
            EndTime
        ) values(%s, "%s", dateTime("%s"), dateTime("%s"))
    `, room.Id, room.Name, room.StartTime.Format(TIME_FORMAT_STR),
            room.EndTime.Format(TIME_FORMAT_STR))
}

/* Returns a query string that deletes a room *
 * with the specified id from the database    */
func getDeleteQuery(id string) string {
    return fmt.Sprintf(`
        DELETE FROM rooms
        WHERE Id = %s
    `, id)
}

/* Returns a query string that retrieves the    *
 * room with the specified id from the database */
func getReadQuery(id string) string {
    return fmt.Sprintf(`
        SELECT Id, Name, StartTime, EndTime
        FROM rooms
        WHERE Id == "%s"
    `, id)
}

/* Returns a query string that retrieves all  *
 * the rooms from the database, ordered by ID */
func getReadAllQuery() string {
    return `
        SELECT Id, Name, StartTime, EndTime
        FROM rooms
        ORDER BY Id
    `
}

/* Returns a query string that returns *
 * either 0 (false) or 1 (true)        */
func getBoolQuery(value bool) string {
    var bitValue int
    if value {
        bitValue = 1
    } else {
        bitValue = 0
    }
    return fmt.Sprintf("SELECT %d", bitValue)
}

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

/* Returns whether two Rooms have identical content */
func roomsAreEqual(room1 Room, room2 Room) bool {
    return (room1.Id == room2.Id) &&
            (room1.Name == room2.Name) &&
            (timesEqual(room1.StartTime, room2.StartTime)) &&
            (timesEqual(room1.EndTime, room2.EndTime))
}

func (room Room) String() string {
    return fmt.Sprintf("Room: #%s, %s, %s, %s", room.Id, room.Name,
            room.StartTime, room.EndTime)
}

