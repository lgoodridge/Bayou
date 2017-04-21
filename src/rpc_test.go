package bayou

import (
    "net"
    "net/http"
    "net/rpc"
    "strconv"
    "strings"
    "sync"
    "testing"
)

/******************************
 *    WORDCOUNT RPC SETUP     *
 ******************************/

/* Dummy type for WordCount test RPC */
type WC int

/* WordCount RPC arguments structure */
type WordCountArgs struct {
    Message string
    f func()
}

/* WordCount RPC reply structure */
type WordCountReply struct {
    Length int
}

/* Counts the words in the sent message */
func (wc *WC) WordCount(args *WordCountArgs, reply *WordCountReply) error {
    args.f()
    reply.Length = len(strings.Split(args.Message, " "))
    return nil
}

/* Start serving WordCount RPCs on the provided port. *
 * Returns the listener for closing when finished.    */
func startWCServer(port int) net.Listener {
    rpcServer := rpc.NewServer()
    wc := new(WC)
    rpcServer.Register(wc)

    // RPCs handlers are registered to the default server mux,
    // so temporarily change it to allow multiple registrations
    oldMux := http.DefaultServeMux
    newMux := http.NewServeMux()
    http.DefaultServeMux = newMux

    // Register RPC handler, and restore default serve mux
    rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
    http.DefaultServeMux = oldMux

    // Listen and serve on the specified port
    listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
    if err != nil {
        Log.Fatal("Listen Failed: ", err)
    }
    go http.Serve(listener, newMux)
    return listener
}

/* Start up a client and connect to localhost *
 * RPC server on the specified port.          *
 * Returns the connected client.              */
func startWCClient(port int) *rpc.Client {
    client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(port))
    if err != nil {
        Log.Fatal("Failed to connect to server: ", err)
    }
    return client
}

/* Sends a WordCount RPC with the provided message.   *
 * If async is true, the call is sent asynchronously. *
 * Returns the number of words in the message.        */
func sendRPC(client *rpc.Client, message string, async bool) int {
    args := WordCountArgs{message, func(){ Log.Fatal("ANON FUNC")}}
    var reply WordCountReply

    // Send RPC synchronously
    if !async {
        err := client.Call("WC.WordCount", &args, &reply)
        if err != nil {
            Log.Fatal("WordCount RPC Failed: ", err)
        }

    // Send RPC asynchronously
    } else {
        wcCall := client.Go("WC.WordCount", &args, &reply, nil)
        // Blocks until call is completed
        <-wcCall.Done
        if wcCall.Error != nil {
            Log.Fatal("WordCount RPC Failed: ", wcCall.Error)
        }
    }

    return reply.Length
}

/********************
 *    RPC TESTS     *
 ********************/

/* Whether an RPC server has been started already */
var serverStarted bool

func init() {
    Log.Println("Tests initialized.")
}

/* Tests a single synchronous and asynchronous WordCount RPC */
func TestRPCBasic(t *testing.T) {
    port := 1234
    closer := startWCServer(port)
    defer closer.Close()

    client := startWCClient(port)
    defer client.Close()

    result := sendRPC(client, "This message has five words.", false)
    assertEqual(t, result, 5, "Expected: 5\tReceived: "+strconv.Itoa(result))
    result = sendRPC(client, "And this message has six words.", true)
    assertEqual(t, result, 6, "Expected: 6\tReceived: "+strconv.Itoa(result))
}

/* Tests concurrent WordCount RPCs */
func TestRPCConcurrent(t *testing.T) {
    port := 2345
    closer := startWCServer(port)
    defer closer.Close()

    client1 := startWCClient(port)
    client2 := startWCClient(port)
    client3 := startWCClient(port)
    defer client1.Close()
    defer client2.Close()
    defer client3.Close()

    var result1 int
    var result2 int
    var result3 int

    var wg sync.WaitGroup
    wg.Add(3)

    // Send RPCs in parallel
    go func(result *int) {
        *result = sendRPC(client1, "Three word message.", true)
        wg.Done()
    }(&result1)
    go func(result *int) {
        *result = sendRPC(client2, "A four word message.", false)
        wg.Done()
    }(&result2)
    go func(result *int) {
        *result = sendRPC(client3, "Lazy message.", true)
        wg.Done()
    }(&result3)

    // Wait until all replies have been received
    wg.Wait()

    assertEqual(t, result1, 3, "Expected: 3\tReceived: "+strconv.Itoa(result1))
    assertEqual(t, result2, 4, "Expected: 4\tReceived: "+strconv.Itoa(result2))
    assertEqual(t, result3, 2, "Expected: 2\tReceived: "+strconv.Itoa(result3))
}

/* Tests multiple servers listening on different ports */
func TestRPCMultipleServers(t *testing.T) {
    port1 := 3456
    port2 := 4567
    closer1 := startWCServer(port1)
    closer2 := startWCServer(port2)
    defer closer1.Close()
    defer closer2.Close()

    client1a := startWCClient(port1)
    client1b := startWCClient(port1)
    client2a := startWCClient(port2)
    client2b := startWCClient(port2)
    defer client1a.Close()
    defer client1b.Close()
    defer client2a.Close()
    defer client2b.Close()

    var result1 int
    var result2 int
    var result3 int
    var result4 int

    var wg sync.WaitGroup
    wg.Add(4)

    // Send RPCs in parallel
    go func(result *int) {
        *result = sendRPC(client1a, "Two piggies, jumping on the bed", true)
        wg.Done()
    }(&result1)
    go func(result *int) {
        *result = sendRPC(client1b, "One fell off and bumped his head.", true)
        wg.Done()
    }(&result2)
    go func(result *int) {
        *result = sendRPC(client2a, "One pig left, he is not having fun", true)
        wg.Done()
    }(&result3)
    go func(result *int) {
        message := "So he got off, and then there were none."
        *result = sendRPC(client2b, message, true)
        wg.Done()
    }(&result4)

    // Wait until all replies have been received
    wg.Wait()

    assertEqual(t, result1, 6, "Expected: 6\tReceived: "+strconv.Itoa(result1))
    assertEqual(t, result2, 7, "Expected: 7\tReceived: "+strconv.Itoa(result3))
    assertEqual(t, result3, 8, "Expected: 8\tReceived: "+strconv.Itoa(result3))
    assertEqual(t, result4, 9, "Expected: 9\tReceived: "+strconv.Itoa(result2))
}
