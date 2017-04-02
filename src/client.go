package bayou

import (
	"net/http"
	"net/rpc"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

type Client struct {
    conn *rpc.Client
}

func startBayouClient(port int) *rpc.Client {
    client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(port))
    if err != nil {
        Log.Fatal("Failed to connect to server: ", err)
    }
    return client
}

func checkRoom(name string, day int, hour int) []Room {
    startDate := createDate(day, hour)
    endDate := createDate(day, hour + 1)
    var roomData []Data

    updateFun := func(db *sql.DB) {
//        return checkRoom_helper(db, name, day, hour)
        return ReadItemInDateRange(db, name, startDate, endDate)
    }

    err := client.Call("BayouServer.Read", &updateFun, &roomData)
    if err != nil {
        Log.Fatal("Bayou Read RPC Failed: ", err)
    }
    return roomData
}

func claimRoom(name string, day int, hour int) []Room {
    startDate := createDate(day, hour)
    endDate := createDate(day, hour + 1)

    updateFun := func(db *sql.DB) {
//        return checkRoom_helper(db, name, day, hour)
        return claimRoom(db, name, day, hour)
    }

    err := client.Call("BayouServer.Read", &updateFun, &retData)
    if err != nil {
        Log.Fatal("Bayou Read RPC Failed: ", err)
    }
    return retData
}
