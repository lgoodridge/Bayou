package bayou

import (
	"net/rpc"
	"strconv"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

/* Go object representing a Bayou Client */
type BayouClient struct {
	id	   int
    server *rpc.Client
}

func NewBayouClient(id int, port int) *BayouClient {
	// Connect to the server
    rpcClient, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(port))
    if err != nil {
        Log.Fatal("Failed to connect to server: ", err)
    }

	client := &BayouClient{}
	client.id = id
	client.server = rpcClient
	return client
}

func (client *BayouClient) checkRoom(name string, day int, hour int) []Room {
    startDate := createDate(day, hour)
    endDate := createDate(day, hour + 1)
    var roomData []Room

    updateFun := func(db *sql.DB) {
        ReadItemInDateRange(db, name, startDate, endDate)
    }

    err := client.server.Call("BayouServer.Read", &updateFun, &roomData)
    if err != nil {
        Log.Fatal("Bayou Read RPC Failed: ", err)
    }
    return roomData
}

func (client *BayouClient) claimRoom(name string, day int, hour int) []Room {
	var roomData []Room

    updateFun := func(db *sql.DB) {
        claimRoom(db, name, day, hour)
    }

    err := client.server.Call("BayouServer.Read", &updateFun, &roomData)
    if err != nil {
        Log.Fatal("Bayou Read RPC Failed: ", err)
    }
    return roomData
}
