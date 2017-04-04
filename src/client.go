package bayou

import (
	"net/rpc"
	"strconv"
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

func (client *BayouClient) CheckRoom(name string, day int, hour int) []Room {
    startDate := createDate(day, hour)
    endDate := createDate(day, hour + 1)
    var roomData []Room

    updateFun := func(db *BayouDB) {
		db.ReadItemInDateRange(name, startDate, endDate)
    }

    err := client.server.Call("BayouServer.Read", &updateFun, &roomData)
    if err != nil {
        Log.Fatal("Bayou Read RPC Failed: ", err)
    }
    return roomData
}

func (client *BayouClient) ClaimRoom(name string, day int, hour int) []Room {
	var roomData []Room

    updateFun := func(db *BayouDB) {
        db.ClaimRoom(name, day, hour)
    }

    err := client.server.Call("BayouServer.Read", &updateFun, &roomData)
    if err != nil {
        Log.Fatal("Bayou Read RPC Failed: ", err)
    }
    return roomData
}
