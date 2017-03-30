package main

import (
    "fmt"
    "log"
    "math/rand"
    "time"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

type Room struct {
    Id          string
    Name        string
    StartTime   time.Time
    EndTime     time.Time
}

func InitDB(filepath string) *sql.DB {
    db, err := sql.Open("sqlite3", filepath)
    if err != nil { panic(err) }
    if db == nil { panic("db nil") }
    return db
}

func CreateTable(db *sql.DB) {
    // create table if not exists
    sql_table := `
    CREATE TABLE IF NOT EXISTS rooms(
        Id TEXT NOT NULL PRIMARY KEY,
        Name TEXT,
        StartTime DATETIME,
        EndTime DATETIME
    );
    `

    _, err := db.Exec(sql_table)
    if err != nil { panic(err) }
}

func StoreItem(db *sql.DB, items []Room) {
    sql_additem := `
    INSERT OR REPLACE INTO rooms(
        Id,
        Name,
        StartTime,
        EndTime 
    ) values(?, ?, ?, ?)
    `

    stmt, err := db.Prepare(sql_additem)
    if err != nil { panic(err) }
    defer stmt.Close()

    for _, item := range items {
        _, err2 := stmt.Exec(item.Id, item.Name,
            item.StartTime, item.EndTime)
        if err2 != nil { panic(err2) }
    }
}

func ReadItem(db *sql.DB) []Room {
    sql_readall := `
    SELECT Id, Name, StartTime, EndTime FROM rooms 
    ORDER BY datetime(StartTime) DESC
    `

    rows, err := db.Query(sql_readall)
    if err != nil { panic(err) }
    defer rows.Close()

    var result []Room
    for rows.Next() {
        item := Room{}
        err2 := rows.Scan(&item.Id, &item.Name,
            &item.StartTime, &item.EndTime)
        if err2 != nil { panic(err2) }
        result = append(result, item)
    }
    return result
}
func ReadItemInDateRange(db *sql.DB, start, end time.Time) []Room {
//    sql_readall := `
//    SELECT Id, Name, StartTime, EndTime FROM rooms 
//    WHERE StartTime BETWEEN date('` + start.Format("15:04") + `') AND  date('` +
//    end.Format("15:04") + `')
//    ORDER BY datetime(StartTime) DESC
//    `

    sql_readall := `
    SELECT Id, Name, StartTime, EndTime FROM rooms 
    WHERE StartTime BETWEEN "2000-01-02" AND "2000-01-03"
    `
    fmt.Println(sql_readall)

    rows, err := db.Query(sql_readall)
    if err != nil { panic(err) }
    defer rows.Close()

    var result []Room
    for rows.Next() {
        item := Room{}
        err2 := rows.Scan(&item.Id, &item.Name,
            &item.StartTime, &item.EndTime)
        if err2 != nil { panic(err2) }
        result = append(result, item)
    }
    return result
}

func randDate() [2]time.Time {
    var t [2]time.Time
    startDay := rand.Intn(7)
    startHour := rand.Intn(23)

    t[0] = createDate(startDay, startHour)
    t[1] = createDate(startDay, startHour + 1)
    fmt.Println(t[0])
    return t
}

func createDate(day, hour int) time.Time {
    loc, _ := time.LoadLocation("")
    return time.Date(2000, 1, day, hour, 0, 0, 0, loc)
}

func main() {
    // Seed the random generator so we get unique results
    rand.Seed(time.Now().Unix())

    // Open the Datapath
    const dbpath = "foo.db"
    db := InitDB(dbpath)
    defer db.Close()

    // Create the DB table
    CreateTable(db)

    // Store 2 items
    items := []Room{
        Room{"1", "Frist", createDate(2, 1), createDate(1, 3)},
        Room{"2", "Friend", createDate(1, 2), createDate(1, 4)},
    }
    StoreItem(db, items)

    // Try and read selection of items
    readItems :=  ReadItemInDateRange(db, createDate(0,0), createDate(0,5))
    for _, item := range(readItems) {
        log.Printf(item.Name)
    }
    fmt.Println("Done")

    // Read all items
    readItems2 := ReadItem(db)
    for _, item := range(readItems2) {
        log.Printf(item.Name)
        fmt.Println(item.StartTime)
    }
}
