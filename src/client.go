package bayou

import (
    "net/rpc"
    "strconv"
)

/************************
 *   TYPE DEFINITIONS   *
 ************************/

/* Go object representing a Bayou Client */
type BayouClient struct {
    id     int
    server *rpc.Client
}

/****************************
 *   BAYOU CLIENT METHODS   *
 ****************************/

/* Returns a new Bayou Client                  *
 * Connects to its server on the provided port */
func NewBayouClient(id int, port int) *BayouClient {
    // Connect to the server
    rpcClient, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(port))
    if err != nil {
        Log.Fatal("Failed to connect to server: ", err)
    }

    client := &BayouClient{id, rpcClient}
    return client
}

/* "Kills" a Bayou Client, closing *
 * connection with the server      */
func (client *BayouClient) Kill() {
    client.server.Close()
}

// TODO (David)
/* Returns the status of the room with provided name at the provided time *
 * If onlyStable is true, tentative claims are not considered             */
func (client *BayouClient) CheckRoom(name string, day int, hour int,
        onlyStable bool) Room {
    return Room{}
}

// TODO (David)
/* Claims a room at the provided date and time */
func (client *BayouClient) ClaimRoom(name string, day int, hour int) {
    startDate := createDate(day, hour)
    endDate := createDate(day, hour + 1)
    room := Room{"ID", name, startDate, endDate}

    debugf("Printing this so go doesn't complain: " + room.Name)

    // TODO: Needs to be redone, sorry!  - Lance
    /*
    // add room to database
    opFunc := func (db *BayouDB) {
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

        _, err2 := stmt.Exec(room.Id, room.Name,
        room.StartTime, room.EndTime)
        if err2 != nil { Log.Fatal(err2) }
    }

    op := Operation{ opFunc, "Claiming a Room"};
    undoFunc := func (db *BayouDB) {
        // TODO: implement
    }
    undo := Operation{ undoFunc, "Undo Claiming a Room"};
    check := func (db *BayouDB) bool {
        // TODO: implement
        return true
    }
    merge := func (db *BayouDB) bool {
        // TODO: implement
        return true
    }
    err, hasConflict, wasResolved := client.sendWriteRPC(op, undo, check, merge)
    debugf("HadConflict: %d wasResolved: %d \n %s\n", hasConflict, wasResolved, err)
    */
}

/**********************
 *   HELPER METHODS   *
 **********************/

/* Sends a Read RPC to the client's server    *
 * Returns an error if the RPC fails, and     *
 * the result of the read query if successful */
func (client *BayouClient) sendReadRPC(readQuery string,
        fromCommit bool) (err error, data interface{}) {
    readArgs := &ReadArgs{readQuery, fromCommit}
    var readReply ReadReply

    // Send RPC and process the results
    err = client.server.Call("BayouServer.Read", readArgs, &readReply)
    if err != nil {
        data = readReply.Data
    } else {
        data = nil
    }
    return
}

/* Sends a Write RPC to the client's server              *
 * Returns an error if the RPC fails, and if successful, *
 * whether the write had a conflict and whether it was   *
 * eventually resolved                                   */
func (client *BayouClient) sendWriteRPC(writeQuery string, undoQuery string,
        check string, merge string) (err error, hasConflict bool,
        wasResolved bool) {
    writeArgs := &WriteArgs{randomInt(), writeQuery, undoQuery, check, merge}
    var writeReply WriteReply

    // Send RPC and process the results
    err = client.server.Call("BayouServer.Write", writeArgs, &writeReply)
    if err != nil {
        hasConflict = writeReply.HasConflict
        wasResolved = writeReply.WasResolved
    } else {
        hasConflict = false
        wasResolved = false
    }
    return
}
