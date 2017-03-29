package main

import (
    "time"
    "log"
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

func main() {
    const dbpath = "foo.db"

    db := InitDB(dbpath)
    defer db.Close()
    CreateTable(db)

    items := []Room{
        Room{"1", "Frist", time.Now(), time.Now()},
        Room{"2", "Friend", time.Now(), time.Now()},
    }
    StoreItem(db, items)

    readItems := ReadItem(db)
    for _, item := range(readItems) {
        log.Printf(item.Name)
    }

    log.Printf("Next Test")

    items2 := []Room{
        Room{"1", "Frist", time.Now(), time.Now()},
        Room{"3", "Sherrard", time.Now(), time.Now()},
    }
    StoreItem(db, items2)

    readItems2 := ReadItem(db)
    for _, item := range(readItems2) {
        log.Printf(item.Name)
    }
}
