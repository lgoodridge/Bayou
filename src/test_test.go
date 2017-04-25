package bayou

import (
    "fmt"
    "net"
    "net/rpc"
    "os"
    "path/filepath"
    "sync"
    "testing"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

/*************************
 *    HELPER METHODS     *
 *************************/

/* Fails provided test if condition is not true */
func assert(t *testing.T, cond bool, message string) {
    assertEqual(t, cond, true, message)
}

/* Fails provided test if a and b are not equal */
func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
    if a != b {
        t.Fatal(message)
    }
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

/* Tests basic database functionality */
func TestDBBasic(t *testing.T) {
    // Open the Database
    const dbpath = "david.db"
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
    `, id, name, startTxt, endTxt);
    db.Execute(query)

    // Execute read query
    readQuery := `
        SELECT Id, Name, StartTime, EndTime
        FROM rooms
        WHERE Id == "1"
    `
    rows := (*sql.Rows)(db.Read(readQuery))
    defer rows.Close()

    // Ensure read query returned a result
    hasRow := rows.Next()
    if !hasRow {
        t.Fatal("Read query failed to return result rows.")
    }
    ensureNoError(t, rows.Err(), "Error getting read query results.")

    // Ensure results are as expected
    item := Room{}
    err := rows.Scan(&item.Id, &item.Name,
            &item.StartTime, &item.EndTime)
    ensureNoError(t, err, "Error scanning read query results.")

    assertEqual(t, item.Name, name, "Item Name does not match inserted value.")

    fmt.Println(startDate)
    fmt.Println(item.StartTime)
    assert(t, item.StartTime.Equal(startDate),
            "Item StartTime does not match inserted value.")
    assert(t, item.EndTime.Equal(endDate),
            "Item EndTime does not match inserted value.")

    // Ensure there are no extra results
    hasRow = rows.Next()
    assert(t, !hasRow, "Read query returned more than one result.")

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
func assertVCEqual(t *testing.T, vc VectorClock, exp VectorClock) {
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
    assertVCEqual(t, vc, VectorClock{0, 0, 0, 0})

    // Ensure Inc works as expected
    vc.Inc(1)
    vc.Inc(3)
    vc.Inc(3)
    assertVCEqual(t, vc, VectorClock{0, 1, 0, 2})

    // Ensure Set works as expected
    err := vc.SetTime(0, 6)
    ensureNoError(t, err, "SetTime returned an error: ")
    err = vc.SetTime(1, 4)
    ensureNoError(t, err, "SetTime returned an error: ")
    err = vc.SetTime(2, 0)
    ensureNoError(t, err, "SetTime returned an error: ")
    assertVCEqual(t, vc, VectorClock{6, 4, 0, 2})

    // Ensure Set returns error when trying to
    // set time less than what is already stored
    err = vc.SetTime(1, 3)
    if err == nil {
        t.Fatal("SetTime did not return an error when rewinding time.")
    }
    assertVCEqual(t, vc, VectorClock{6, 4, 0, 2})

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
    assertVCEqual(t, vc, VectorClock{6, 5, 2, 2})
    // Ensure other wasn't affected
    assertVCEqual(t, other, VectorClock{5, 5, 2, 2})
}

/*****************************
 *    BAYOU SERVER TESTS     *
 *****************************/

/* Sets up an array of RPC clients, each listening *
 * to one of the ports in the provided ports array */
func setupClients(clients []*rpc.Client, ports []int) {
    if len(clients) != len(ports) {
        Log.Fatal("Test Error: Length of client and port arrays do not match.")
    }
    for idx, port := range ports {
        clients[idx] = startRPCClient(port)
    }
}

/* Sets up an array of Dummy WC RPC servers, each    *
 * serving on one of the ports in the provided array *
 * Returns an array of listeners for clean up        */
func setupDummyServers(ports []int) []net.Listener {
    listeners := make([]net.Listener, len(ports))
    for idx, port := range ports {
        listeners[idx] = startWCServer(port)
    }
    return listeners
}

/* Closes each of the provided RPC clients */
func cleanupClients(clients []*rpc.Client) {
    for _, client := range clients {
        client.Close()
    }
}

/* Closes each of the provided dummy servers */
func cleanupDummyServers(listeners []net.Listener) {
    for _, listener := range listeners {
        listener.Close()
    }
}

/* Tests server RPC functionality */
func TestServerRPC(t *testing.T) {
    numClients := 10
    port := 1111
    otherPort := 1112

    ports := make([]int, numClients)
    for i := 0; i < numClients; i++ {
        ports[i] = port
    }
    ports[1] = otherPort
    clients := make([]*rpc.Client, len(ports))

    commitDB := getDB("test_commit.db", true)
    fullDB := getDB("test_full.db", true)
    defer commitDB.Close()
    defer fullDB.Close()

    // Start up Bayou servers and RPC clients
    serverID := 0
    otherServerID := 1
    server := NewBayouServer(serverID, clients, commitDB,
            fullDB, ports[serverID])
    otherServer := NewBayouServer(otherServerID, clients, commitDB,
            fullDB, ports[otherServerID])
    setupClients(clients, ports)
    defer server.Kill()
    defer cleanupClients(clients)

    // Test a single RPC
    pingArgs := &PingArgs{2}
    var pingReply PingReply
    err := clients[serverID].Call("BayouServer.Ping", pingArgs, &pingReply)
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
    success := server.SendPing(otherServerID)
    assert(t, success, "Inter-server Ping RPC failed.")
    success = otherServer.SendPing(serverID)
    assert(t, success, "Inter-server Ping RPC failed.")
}

/* Tests server Read and Write functions */
func TestServerReadWrite(t *testing.T) {
    Log.Println("Test not implemented.")
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
func createNetwork(testName string, numClusters int) ([]*BayouServer,
        []*BayouClient) {
    serverList := make([]*BayouServer, numClusters)
    clientList := make([]*BayouClient, numClusters)
    rpcClients := make([]*rpc.Client, numClusters)
    port := 1111
    for i := 0; i < numClusters; i++ {
        id := fmt.Sprintf("%d", i)
        commitDB := getDB(testName + id + ".commit.db", true)
        fullDB := getDB(testName + id + ".full.db", true)
        server := NewBayouServer(i, rpcClients, commitDB, fullDB, port + i)
        serverList[i] = server
        clientList[i] = NewBayouClient(i, port + i)
        rpcClients[i] = startRPCClient(port + i)
        clientList[i] = &BayouClient{i, rpcClients[i]}
    }
    return serverList, clientList
}

/* Starts inter-server communication on the provided network */
func startNetwork(servers []*BayouServer) {
    for _, server := range servers {
        server.Start()
    }
}

/* Shuts down and cleans up the provided network */
func removeNetwork(servers []*BayouServer, clients []*BayouClient) {
    for _, client := range clients {
        client.Kill()
    }
    for _, server := range servers {
        server.Kill()
    }
}

/* Tests client functionality */
func TestClient(t *testing.T) {
    servers, clients := createNetwork("TestWrite", 1)
    defer removeNetwork(servers, clients)

    // Test non-conflicting write
    clients[0].ClaimRoom("Frist", 1, 1)
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
