package bayou

import (
    "log"
    "math/rand"
    "time"
    "testing"
)

/*************************
 *    HELPER METHODS     *
 *************************/

/* Performs necessary setup for the tests */
func init() {
    rand.Seed(time.Now().Unix())
}

/* Fails provided test if a and b are not equal */
func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
    if a != b {
        t.Fatal(message)
    }
}

/*************************
 *    DATABASE TESTS     *
 *************************/

/* Tests basic database functionality */
func TestDBBasic(t *testing.T) {
    // Open the Datapath
    const dbpath = "foo.db"
    db := InitDB(dbpath)
    defer db.Close()

    // Create the DB table
    CreateTable(db)

    err := claimRoom(db, "Frist", 1, 1)
    if err != "" {
        log.Println(err)
    }
    err = claimRoom(db, "Friend", 4, 2)
    if err != "" {
        log.Println(err)
    }

    // Read all items
    readItems2 := ReadAllItems(db)
    for _, item := range(readItems2) {
        log.Println(item.Name)
        log.Println(item.StartTime)
    }
}
