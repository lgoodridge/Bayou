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
    db := &BayouDB{sqlDB}
    // TODO: Should probably move this
    db.CreateTable()
    return db;
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
    // TODO: Make sure everyone ELSE closes the rows
//    defer rows.Close()
    check(err, "Error executing read (" + query + "): ")
    return rows
}

/* Executes provided query on the database *
 * and returns the (boolean) result        */
func (db *BayouDB) Check(query string) bool {
    rows, err := db.Query(query)
    defer rows.Close()
    check(err, "Error executing check (" + query + "): ")
    for rows.Next() {
        var boolResult bool
        err = rows.Scan(&boolResult)
        check(err, "Error scanning result of check (" + query + "): ")
        return boolResult
    }
    return false
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

