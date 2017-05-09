package bayou

import (
    "fmt"
    "net/rpc"
    "time"
)

/************************
 *   TYPE DEFINITIONS   *
 ************************/

/* Go object representing a Bayou Client */
type BayouClient struct {
    id     int
    server *rpc.Client
}

/* Represents a room in the scheduling app */
type Room struct {
    Id          string
    Name        string
    StartTime   time.Time
    EndTime     time.Time
}

/****************************
 *   BAYOU CLIENT METHODS   *
 ****************************/

/* Returns a new Bayou Client            *
 * Provided RPC client should already be *
 * connected to this client's server    */
func NewBayouClient(id int, rpcClient *rpc.Client) *BayouClient {
    client := &BayouClient{id, rpcClient}
    return client
}

/* "Kills" a Bayou Client, closing *
 * connection with the server      */
func (client *BayouClient) Kill() {
    client.server.Close()
}

/* Returns the status of the room with provided name at the provided time *
 * If onlyStable is true, tentative claims are not considered             */
func (client *BayouClient) CheckRoom(name string, day int, hour int,
        onlyStable bool) Room {
    // Generate Dates
    startDate := createDate(day, hour)
//    endDate := createDate(day, hour + 1)
    startTxt := startDate.Format("2006-01-02 15:04")
//    endTxt   := endDate.Format("2006-01-02 15:04")

    query := fmt.Sprintf(`
    SELECT Id, Name, StartTime, EndTime FROM rooms
    WHERE StartTime BETWEEN dateTime("%s") AND dateTime("%s")
    `, startTxt, startTxt);
    err, result :=  client.sendReadRPC(query, false)
    check(err, "SendReadRPC failed: ")

    rooms := deserializeRooms(result)
    if (len(rooms) > 1) {
        debugf("Multiple rooms returned")
    }

    if (len(rooms) == 0) {
        var r Room
        r.Id = "-1"
        return r
    }
    return rooms[0]
}

/* Claims a room at the provided date and time */
func (client *BayouClient) ClaimRoom(name string, day int, hour int) {
    // Generate Dates
    startDate := createDate(day, hour)
    endDate   := createDate(day, hour + 1)
    startTxt  := startDate.Format("2006-01-02 15:04")
    endTxt    :=   endDate.Format("2006-01-02 15:04")
    id := "1"

    // Create Room
    // TODO: Make global id's
    // room := Room{id, name, startDate, endDate}

    query := fmt.Sprintf(`
    INSERT OR REPLACE INTO rooms(
        Id,
        Name,
        StartTime,
        EndTime
    ) values(%s, "%s", dateTime("%s"), dateTime("%s"))
    `, id, name, startTxt, endTxt);

    check := fmt.Sprintf(`
    SELECT CASE WHEN EXISTS (
            SELECT *
            FROM rooms
            WHERE StartTime BETWEEN dateTime("%s") AND dateTime("%s")
    )
    THEN CAST(0 AS BIT)
    ELSE CAST(1 AS BIT) END
    `, startTxt, startTxt);

    // Always return false because we can't merge
    merge := `
    SELECT 0
    `

    // TODO: Fix this with global ids
    undo := fmt.Sprintf(`
    DELETE FROM rooms
    WHERE Id = %d
    `, id);

    _, hasConflict, wasResolved := client.sendWriteRPC(query,
            undo, check, merge)
    debugf("hasConflict %v\n", hasConflict)
    debugf("wasResolved %v\n", wasResolved)
}

/**********************
 *   HELPER METHODS   *
 **********************/

/* Sends a Read RPC to the client's server    *
 * Returns an error if the RPC fails, and     *
 * the result of the read query if successful */
func (client *BayouClient) sendReadRPC(readQuery string,
        fromCommit bool) (err error, data ReadResult) {
    readArgs := &ReadArgs{readQuery, fromCommit}
    var readReply ReadReply

    // Send RPC and process the results
    err = client.server.Call("BayouServer.Read", readArgs, &readReply)
    if err == nil {
        data = readReply.Data
    } else {
        debugf("Client #%d Read RPC Failed: " + err.Error(), client.id)
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
    if err == nil {
        hasConflict = writeReply.HasConflict
        wasResolved = writeReply.WasResolved
    } else {
        debugf("Client #%d Write RPC Failed: " + err.Error(), client.id)
        hasConflict = false
        wasResolved = false
    }
    return
}
