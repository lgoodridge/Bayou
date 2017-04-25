package bayou

import (
    "math/rand"
    "time"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

/* Database used by Bayou server *
 * Extends sqlite3 database type */
type BayouDB struct {
    *sql.DB
}

type Room struct {
    Id          string
    Name        string
    StartTime   time.Time
    EndTime     time.Time
}

/*
 * Opens the Database file
 */
func InitDB(filepath string) *BayouDB {
    sqlDB, err := sql.Open("sqlite3", filepath)
    if err != nil { Log.Fatal(err) }
    if sqlDB == nil { Log.Fatal("db nil") }
    return &BayouDB{sqlDB}
}

/*
 * Creates a Database table if one
 * does not exist already
 */
func (db *BayouDB) CreateTable() {
    // create table if not exists
    sql_table := `
    CREATE TABLE IF NOT EXISTS rooms(
        Id TEXT NOT NULL PRIMARY KEY,
        Name TEXT,
        StartTime DATETIME,
        EndTime DATETIME,
        Owner TEXT
    );
    `

    _, err := db.Exec(sql_table)
    if err != nil { Log.Fatal(err) }
}

/* Executes provided query on the database */
func (db *BayouDB) Execute(query string) {
    _, err := db.Exec(query)
    check(err, "Error executing query (" + query + "): ")
}

/* Executes provided query on the   *
 * database, and returns the result */
func (db *BayouDB) Read(query string) *sql.Rows {
    rows, err := db.Query(query)
    check(err, "Error executing read (" + query + "): ")
    return rows
}

// TODO: I might not have done this properly, please take a look  - Lance
/* Executes provided query on the database *
 * and returns the (boolean) result        */
func (db *BayouDB) Check(query string) bool {
    rows, err := db.Query(query)
    check(err, "Error executing check (" + query + "): ")
    for rows.Next() {
        var boolResult bool
        err = rows.Scan(&boolResult)
        check(err, "Error scanning result of check (" + query + "): ")
        return boolResult
    }
    return false
}

/*********************************************
 * TODO:
 * Most of these methods should probably be
 * refactored or implemented in the client?
 *                                 - Lance
 *********************************************/

/*
 * Stores a list of items in the Database.
 * If the item exists already it will be
 * overwritten
*/
func (db *BayouDB) StoreItem(items []Room) {
    sql_additem := `
    INSERT OR REPLACE INTO rooms(
        Id,
        Name,
        StartTime,
        EndTime
    ) values(?, ?, ?, ?)
    `

    stmt, err := db.Prepare(sql_additem)
    if err != nil { Log.Fatal(err) }
    defer stmt.Close()

    for _, item := range items {
        _, err2 := stmt.Exec(item.Id, item.Name,
            item.StartTime, item.EndTime)
        if err2 != nil { Log.Fatal(err2) }
    }
}

/*
 * Returns every item in the database
 */
func (db *BayouDB) ReadAllItems() []Room {
    sql_readall := `
    SELECT Id, Name, StartTime, EndTime FROM rooms
    ORDER BY datetime(StartTime) DESC
    `

    rows, err := db.Query(sql_readall)
    if err != nil { Log.Fatal(err) }
    defer rows.Close()

    var result []Room
    for rows.Next() {
        item := Room{}
        err2 := rows.Scan(&item.Id, &item.Name,
            &item.StartTime, &item.EndTime)
        if err2 != nil { Log.Fatal(err2) }
        result = append(result, item)
    }
    return result
}

/*
 * Returns only the database items between the start and the
 * end times provided
 */
func (db *BayouDB) ReadItemInDateRange(name string,
        start time.Time, end time.Time) []Room {
    // get the dates into the correct format
    startTxt := start.Format("2006-01-02 03:04")
    endTxt   := end.Format("2006-01-02 03:04")
    // build the SQL query string
    sql_readall := `
    SELECT Id, Name, StartTime, EndTime FROM rooms
    WHERE StartTime BETWEEN "` + startTxt + `" AND "` + endTxt + `"
    `

    debugf(sql_readall + "\n")

    // Read the query out of the DB
    rows, err := db.Query(sql_readall)
    if err != nil { Log.Fatal(err) }
    defer rows.Close()

    // Read the query into a datastructure
    var result []Room
    for rows.Next() {
        item := Room{}
        err2 := rows.Scan(&item.Id, &item.Name,
            &item.StartTime, &item.EndTime)
        if err2 != nil { Log.Fatal(err2) }
        result = append(result, item)
    }
    return result
}

/*
 * Returns random start and end time of event within a week
 * All events last for 1 hour
 */
func randDate() [2]time.Time {
    var t [2]time.Time
    startDay := rand.Intn(7)
    startHour := rand.Intn(23)

    t[0] = createDate(startDay, startHour)
    t[1] = createDate(startDay, startHour + 1)
    debugf("%s\n", t[0])
    return t
}

/*
 * Creates a date given a date (0-6)
 * and a time (0-23).
 * All dates are in January 2000 because that was a good time
 */
func createDate(day, hour int) time.Time {
    loc, _ := time.LoadLocation("")
    return time.Date(2000, 1, day, hour, 0, 0, 0, loc)
}

var nextID int

/*
 * Attempts to claim a room with the given name,
 * day, and hour.
 * Returns a "" if successful, and an error string otherwise
 */
func (db *BayouDB) ClaimRoom(name string, day, hour int) string {
    startDate := createDate(day, hour)
    endDate := createDate(day, hour + 1)

    // TODO: we need to check that conflict with the
    // actual room name
    events := db.ReadItemInDateRange(name, startDate, endDate)

    // If event exists then we have a conflict
    if events != nil {
        return "Room already taken"
    // Otherwise return the room
    } else {
        var r []Room
        r = append(r, Room{string(nextID), name, startDate, endDate})
        db.StoreItem(r)
        nextID += 1
        return ""
    }
}
