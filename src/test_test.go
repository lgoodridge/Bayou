package bayou

import (
    "fmt"
    "net/rpc"
    "os"
    "path/filepath"
    "sync"
    "testing"
)

/*************************
 *    HELPER METHODS     *
 *************************/

/* Fails provided test if condition is not true */
func assert(t *testing.T, cond bool, message string) {
    if !cond {
        t.Fatal(message)
    }
}

/* Fails provided test if a and b are not equal */
func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
    assert(t, a == b, message)
}

/* Fails provided test if err is not nil */
func ensureNoError(t *testing.T, err error, prefix string) {
    if err != nil {
        t.Fatal(prefix + err.Error())
    }
}

/*************************
 *    DATABASE TESTS     *
 *************************/

/* Returns the Bayou database with the provided filename *
 * Clears the database before returning if reset is true *
 * All test databases are stored in the "db" directory   */
func getDB(filename string, reset bool) *BayouDB {
    dirname := "db"
    os.MkdirAll(dirname, os.ModePerm)
    dbFilepath := filepath.Join(dirname, filename)
    if reset {
        os.RemoveAll(dbFilepath)
    }
    return InitDB(dbFilepath)
}

/* Fails provided test if rooms are not equal */
func assertRoomsEqual(t *testing.T, room Room, exp Room) {
    failMsg := "Expected Room: " + exp.String() +
            "\tReceived: " + room.String()
    if (room.Id != exp.Id) ||
       (room.Name != exp.Name) ||
       !timesEqual(room.StartTime, exp.StartTime) ||
       !timesEqual(room.EndTime, exp.EndTime) {
        t.Fatal(failMsg)
    }
}

/* Tests basic database functionality */
func TestDBBasic(t *testing.T) {
    // Open the Database
    const dbpath = "dbbasic.db"
    db := getDB(dbpath, false)
    defer db.Close()

    name := "Fine"
    id := "1"
    startDate := createDate(0, 0)
    endDate := createDate(1, 5)
    startTxt := startDate.Format("2006-01-02 15:04")
    endTxt   := endDate.Format("2006-01-02 15:04")

    // Execute insertion query
    query := fmt.Sprintf(`
    INSERT OR REPLACE INTO rooms(
        Id,
        Name,
        StartTime,
        EndTime
    ) values(%s, "%s", dateTime("%s"), dateTime("%s"))
    `, id, name, startTxt, endTxt)
    db.Execute(query)

    // Execute read query
    readQuery := `
        SELECT Id, Name, StartTime, EndTime
        FROM rooms
        WHERE Id == "1"
    `
    result := db.Read(readQuery)

    // Ensure results are as expected
    rooms := deserializeRooms(result)
    assertEqual(t, len(rooms), 1, "Read query returned wrong number of rooms.")
    assertRoomsEqual(t, rooms[0], Room{id, name, startDate, endDate})

    // Execute a dependency check query
    check := fmt.Sprintf(`
    SELECT CASE WHEN EXISTS (
            SELECT *
            FROM rooms
            WHERE StartTime BETWEEN dateTime("%s") AND dateTime("%s")
    )
    THEN CAST(1 AS BIT)
    ELSE CAST(0 AS BIT) END
    `, startTxt, endTxt);
    assert(t, db.Check(check), "Dependency check failed.")

    // Execute a merge check query
    merge := `
    SELECT 0
    `
    assert(t, !db.Check(merge), "Merge check failed.")
}


/*****************************
 *    VECTOR CLOCK TESTS     *
 *****************************/

/* Fails provided test if VCs are not equal */
func assertVCsEqual(t *testing.T, vc VectorClock, exp VectorClock) {
    failMsg := "Expected VC: " + exp.String() + "\tReceived: " + vc.String()
    if len(vc) != len(exp) {
        t.Fatal(failMsg)
    }
    for idx, _ := range vc {
        if vc[idx] != exp[idx] {
            t.Fatal(failMsg)
        }
    }
}

/* Unit tests vector clock */
func TestVectorClock(t *testing.T) {
    vc := NewVectorClock(4)
    assertVCsEqual(t, vc, VectorClock{0, 0, 0, 0})

    // Ensure Inc works as expected
    vc.Inc(1)
    vc.Inc(3)
    vc.Inc(3)
    assertVCsEqual(t, vc, VectorClock{0, 1, 0, 2})

    // Ensure Set works as expected
    err := vc.SetTime(0, 6)
    ensureNoError(t, err, "SetTime returned an error: ")
    err = vc.SetTime(1, 4)
    ensureNoError(t, err, "SetTime returned an error: ")
    err = vc.SetTime(2, 0)
    ensureNoError(t, err, "SetTime returned an error: ")
    assertVCsEqual(t, vc, VectorClock{6, 4, 0, 2})

    // Ensure Set returns error when trying to
    // set time less than what is already stored
    err = vc.SetTime(1, 3)
    if err == nil {
        t.Fatal("SetTime did not return an error when rewinding time.")
    }
    assertVCsEqual(t, vc, VectorClock{6, 4, 0, 2})

    // Ensure LessThan works as expected
    wrongSize := VectorClock{0, 0, 0}
    greater := VectorClock{6, 5, 0, 2}
    equal := VectorClock{6, 4, 0, 2}
    less := VectorClock{6, 3, 0, 2}

    assert(t, !wrongSize.LessThan(vc), "LessThan returned true for VC of " +
        "different size")
    assert(t, !greater.LessThan(vc), "LessThan returned true for greater VC")
    assert(t, !equal.LessThan(vc), "LessThan returned true for equal VC")
    assert(t, less.LessThan(vc), "LessThan returned false for lesser VC")

    // Ensure Max works as expected
    other := VectorClock{5, 5, 2, 2}
    vc.Max(other)
    assertVCsEqual(t, vc, VectorClock{6, 5, 2, 2})
    // Ensure other wasn't affected
    assertVCsEqual(t, other, VectorClock{5, 5, 2, 2})
}

/*****************************
 *    BAYOU SERVER TESTS     *
 *****************************/

/* Fails provided test if Bayou Logs do not have equal content *
 * Ensures order of entries are the same is checkOrder is true */
func assertLogsEqual(t *testing.T, log []LogEntry, exp []LogEntry,
        checkOrder bool) {
    failMsg := "Expected Log: " + logToString(exp) + "\nReceived: " +
            logToString(log)
    assertEqual(t, len(log), len(exp), failMsg)
    if checkOrder {
        for idx, _ := range log {
            assert(t, entriesAreEqual(log[idx], exp[idx], true), failMsg)
        }
    } else {
        logCopy := make([]LogEntry, len(log))
        copy(logCopy, log)
        for _, expEntry := range exp {
            for idx, logEntry := range logCopy {
                if entriesAreEqual(expEntry, logEntry, false) {
                    logCopy = append(log[:idx], log[idx+1:]...)
                    break
                }
            }
            t.Fatal(failMsg)
        }
    }
}

/* Fails provided test if provided *
 * Room lists are not identical    */
func assertRoomListsEqual(t *testing.T, rooms []Room, exp []Room,
        prefix string) {
    roomStr := ""
    expStr := ""
    for _, entry := range rooms {
        roomStr = roomStr + entry.String() + "\n"
    }
    for _, entry := range exp {
        expStr = expStr + entry.String() + "\n"
    }
    failMsg := prefix + ":\n\nExepected Rooms:\n" + expStr +
            "\nReceived: " + roomStr

    assertEqual(t, len(rooms), len(exp), failMsg)
    for idx, _ := range rooms {
        assert(t, roomsAreEqual(rooms[idx], exp[idx]), failMsg)
    }
}

/* Fails provided test if database contents *
 * do not match the provided Room list      */
func assertDBContentsEqual(t *testing.T, db *BayouDB, exp []Room) {
    readQuery := `
        SELECT Id, Name, StartTime, EndTime
        FROM rooms
        ORDER BY Id
    `
    result := db.Read(readQuery)
    rooms := deserializeRooms(result)
    assertRoomListsEqual(t, rooms, exp, "Database does not contain " +
        "expected contents")
}

/* Kills each of the provided servers */
func cleanupServers(servers []*BayouServer) {
    for _, server := range servers {
        server.Kill()
        server.commitDB.Close()
        server.fullDB.Close()
        DeletePersist(server.id)
    }
}

/* Closes each of the provided RPC clients */
func cleanupRPCClients(clients []*rpc.Client) {
    for _, client := range clients {
        client.Close()
    }
}

/* Creates a network of Bayou servers and RPC clients *
 * A server is started for each provided server port,  *
 * and a an RPC client for each provided client port   */
func createNetwork(testName string, serverPorts []int,
        clientPorts []int) ([]*BayouServer, []*rpc.Client) {
    serverList := make([]*BayouServer, len(serverPorts))
    rpcClients := make([]*rpc.Client, len(clientPorts))
    for i, port := range serverPorts {
        id := fmt.Sprintf("%d", i)
        commitDB := getDB(testName + "_" + id + "_commit.db", true)
        fullDB := getDB(testName + "_" + id + "_commit.db", true)
        serverList[i] = NewBayouServer(i, rpcClients, commitDB, fullDB, port)
    }
    for i, port := range clientPorts {
        rpcClients[i] = startRPCClient(port)
    }
    return serverList, rpcClients
}

/* Shuts down and cleans up the provided network */
func removeNetwork(servers []*BayouServer, clients []*rpc.Client) {
    cleanupRPCClients(clients)
    cleanupServers(servers)
}

/* Tests server RPC functionality */
func TestServerRPC(t *testing.T) {
    numClients := 10
    port := 1111
    otherPort := 1112

    serverPorts := []int{port, otherPort}
    clientPorts := make([]int, numClients)
    for i := 0; i < numClients; i++ {
        clientPorts[i] = port
    }
    clientPorts[1] = otherPort

    servers, clients := createNetwork("test_rpc", serverPorts, clientPorts)
    defer cleanupRPCClients(clients)

    server := servers[0]
    otherServer := servers[1]
    defer server.Kill()

    // Test a single RPC
    pingArgs := &PingArgs{2}
    var pingReply PingReply
    err := clients[server.id].Call("BayouServer.Ping", pingArgs, &pingReply)
    ensureNoError(t, err, "Single Ping RPC failed: ")
    assert(t, pingReply.Alive, "Single Ping RPC failed.")

    var wg sync.WaitGroup
    wg.Add(numClients)

    argArr := make([]PingArgs, numClients)
    replyArr := make([]PingReply, numClients)

    // Test several RPC calls at once
    for i := 0; i < numClients; i++ {
        go func(id int) {
            debugf("Client #%d sending ping!", id)
            argArr[id].SenderID = id
            newErr := clients[id].Call("BayouServer.Ping",
                    &argArr[id], &replyArr[id])
            ensureNoError(t, newErr, "Concurrent Ping RPC Failed: ")
            assert(t, replyArr[id].Alive, "Concurrent Ping RPC failed.")
            wg.Done()
        } (i)
    }
    wg.Wait()

    // Test inter-server RPC
    success := server.SendPing(otherServer.id)
    assert(t, success, "Inter-server Ping RPC failed.")
    success = otherServer.SendPing(server.id)
    assert(t, success, "Inter-server Ping RPC failed.")

    // Ensure RPC to Killed server fails
    otherServer.Kill()
    success = server.SendPing(otherServer.id)
    assert(t, !success, "Ping to Killed server suceeded.")
}

/* Tests server Read and Write functions */
func TestServerReadWrite(t *testing.T) {
    numClients := 10
    port := 1113

    serverPorts := []int{port}
    clientPorts := make([]int, numClients)
    for i := 0; i < numClients; i++ {
        clientPorts[i] = port
    }

    servers, clients := createNetwork("test_read_write",
            serverPorts, clientPorts)
    server := servers[0]
    defer removeNetwork(servers, clients)

    room := Room{"0", "RW0", createDate(0, 0), createDate(0, 1)}
    rooms := []Room{room}

    query := getInsertQuery(room)
    undo := getDeleteQuery(room.Id)
    check := getBoolQuery(true)
    merge := getBoolQuery(false)

    writeEntry := LogEntry{0, VectorClock{0}, query, check, merge}
    undoEntry := LogEntry{0, VectorClock{0}, undo, check, merge}

    // Test a single uncommitted write
    writeArgs := &WriteArgs{0, query, undo, check, merge}
    var writeReply WriteReply
    err := clients[server.id].Call("BayouServer.Write", writeArgs, &writeReply)
    ensureNoError(t, err, "Single Write RPC failed: ")

    assert(t, writeReply.HasConflict, "Write falsely returned conflict.")
    assert(t, writeReply.WasResolved, "Write was not resolved.")
    assert(t, len(server.CommitLog) == 0, "Uncommitted write changed " +
            "commit log.")
    assert(t, len(server.ErrorLog) == 0, "Write was falsely written " +
            "to error log.")
    assertLogsEqual(t, server.TentativeLog, []LogEntry{writeEntry}, true)
    assertLogsEqual(t, server.UndoLog, []LogEntry{undoEntry}, true)
    assertDBContentsEqual(t, server.fullDB, rooms)
}

/* Tests server Anti-Entropy communication */
func TestServerAntiEntropy(t *testing.T) {
    Log.Println("Test not implemented.")
}

/* Tests server persistence and recovery */
func TestServerPersist(t *testing.T) {
    Log.Println("Test not implemented.")
}

/******************************
 *    BAYOU NETWORK TESTS     *
 ******************************/

/* Creates a network of Bayou Server-Client clusters */
func createBayouNetwork(testName string, numClusters int) ([]*BayouServer,
        []*BayouClient) {
    ports := make([]int, numClusters)
    for i := 0; i < numClusters; i++ {
        ports[i] = 1111 + i
    }
    clientList := make([]*BayouClient, numClusters)
    serverList, rpcClients := createNetwork(testName, ports, ports)
    for i, rpcClient := range rpcClients {
        clientList[i] = NewBayouClient(i, rpcClient)
    }
    return serverList, clientList
}

/* Shuts down and cleans up the provided network */
func removeBayouNetwork(servers []*BayouServer, clients []*BayouClient) {
    for _, client := range clients {
        client.Kill()
    }
    cleanupServers(servers)
}

/* Starts inter-server communication on the provided network */
func startBayouNetworkComm(servers []*BayouServer) {
    for _, server := range servers {
        server.Start()
    }
}

/* Tests client functionality */
func TestClient(t *testing.T) {
    servers, clients := createBayouNetwork("test_client", 1)
    defer removeBayouNetwork(servers, clients)

    // Test non-conflicting write
    clients[0].ClaimRoom("Frist", 1, 1)

    // TODO: Check something?
}

/* Tests that a Bayou network     *
 * eventually reaches consistency */
func TestNetworkBasic(t *testing.T) {
    Log.Println("Test not implemented.")
}

/* Tests that a Bayou network eventually reaches *
 * consistency in the face of network partitions */
func TestNetworkPartition(t *testing.T) {
    Log.Println("Test not implemented.")
}
