package bayou

import (
    "encoding/gob"
    "time"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

/************************
 *   TYPE DEFINITIONS   *
 ************************/

/* Database used by Bayou server *
 * Extends sqlite3 database type */
type BayouDB struct {
    *sql.DB
}

/* Represents the results of BayouDB read query: *
 * Each map in the slice corresponds to a row's  *
 * data, with the keys being the column names    */
type ReadResult []map[string]interface{}

/************************
 *   DATABASE METHODS   *
 ************************/

/*
 * Opens the Database file
 */
func InitDB(filepath string) *BayouDB {
    gob.Register(time.Time{})
    sqlDB, err := sql.Open("sqlite3", filepath)
    if err != nil { Log.Fatal(err) }
    if sqlDB == nil { Log.Fatal("db nil") }
    db := &BayouDB{sqlDB}
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
func (db *BayouDB) Read(query string) ReadResult {
    rows, err := db.Query(query)
    defer rows.Close()
    check(err, "Error executing read (" + query + "): ")

    columns, err2 := rows.Columns()
    check(err2, "Error getting columns for read query (" + query + ")")

    var result []map[string]interface{}

    for rows.Next() {
        check(rows.Err(), "Error getting result of read query (" + query + ")")

        // sql package requires pointers when scanning, so
        // create slice to actually store the values, and
        // another slice to contain the pointers to them
        columnVals := make([]interface{}, len(columns))
        columnPtrs := make([]interface{}, len(columns))
        for i, _ := range columns {
            columnPtrs[i] = &columnVals[i]
        }

        // Scan results into column pointer slice
        err = rows.Scan(columnPtrs...)
        check(err, "Error scanning result of read query (" + query + ")")

        // Create map for the row, and append it to result slice
        rowMap := make(map[string]interface{})
        for i, columnName := range columns {
            rowMap[columnName] = columnVals[i]
        }
        result = append(result, rowMap)
    }

    return result
}

/* Executes provided query on the database *
 * and returns the (boolean) result        */
func (db *BayouDB) Check(query string) bool {
    rows, err := db.Query(query)
    defer rows.Close()
    check(err, "Error executing check (" + query + "): ")

    // Ensure the query returned a result
    hasResult := rows.Next()
    if !hasResult {
        Log.Fatal("No result returned from check query (" + query + ")")
    }
    check(rows.Err(), "Error getting result of check query (" + query + ")")

    var boolResult bool
    err = rows.Scan(&boolResult)
    check(err, "Error scanning result of check (" + query + "): ")

    return boolResult
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

/*
 * Returns random start and end time of event within a week
 * All events last for 1 hour
 */
func randDate() [2]time.Time {
    var t [2]time.Time
    startDay := randomIntn(7)
    startHour := randomIntn(23)

    t[0] = createDate(startDay, startHour)
    t[1] = createDate(startDay, startHour + 1)
    debugf("%s\n", t[0])
    return t
}

