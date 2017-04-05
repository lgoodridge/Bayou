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
}

/* Sends a Read RPC to the client's server    *
 * Returns an error if the RPC fails, and     *
 * the result of the read query if successful */
func (client *BayouClient) sendReadRPC(readfunc ReadFunc,
        fromCommit bool) (err error, data interface{}) {
    readArgs := &ReadArgs{readfunc, fromCommit}
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
func (client *BayouClient) sendWriteRPC(op Operation, undo Operation,
        check DepCheck, merge MergeProc) (err error, hasConflict bool,
        wasResolved bool) {
    writeArgs := &WriteArgs{randomInt(), op, undo, check, merge}
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
