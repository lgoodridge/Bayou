package main

import (
    "log"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

type TestItem struct {
    Id  string
    Name    string
    Phone   string
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
    CREATE TABLE IF NOT EXISTS items(
        Id TEXT NOT NULL PRIMARY KEY,
        Name TEXT,
        Phone TEXT,
        InsertedDatetime DATETIME
    );
    `

    _, err := db.Exec(sql_table)
    if err != nil { panic(err) }
}

func StoreItem(db *sql.DB, items []TestItem) {
    sql_additem := `
    INSERT OR REPLACE INTO items(
        Id,
        Name,
        Phone,
        InsertedDatetime
    ) values(?, ?, ?, CURRENT_TIMESTAMP)
    `

    stmt, err := db.Prepare(sql_additem)
    if err != nil { panic(err) }
    defer stmt.Close()

    for _, item := range items {
        _, err2 := stmt.Exec(item.Id, item.Name, item.Phone)
        if err2 != nil { panic(err2) }
    }
}

func ReadItem(db *sql.DB) []TestItem {
    sql_readall := `
    SELECT Id, Name, Phone FROM items
    ORDER BY datetime(InsertedDatetime) DESC
    `

    rows, err := db.Query(sql_readall)
    if err != nil { panic(err) }
    defer rows.Close()

    var result []TestItem
    for rows.Next() {
        item := TestItem{}
        err2 := rows.Scan(&item.Id, &item.Name, &item.Phone)
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

    items := []TestItem{
        TestItem{"1", "A", "213"},
        TestItem{"2", "B", "214"},
    }
    StoreItem(db, items)

    readItems := ReadItem(db)
    log.Printf(readItems[0].Name)

    items2 := []TestItem{
        TestItem{"1", "C", "215"},
        TestItem{"3", "D", "216"},
    }
    StoreItem(db, items2)

    readItems2 := ReadItem(db)
    log.Printf(readItems2[0].Name)
}
