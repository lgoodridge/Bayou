package bayou

import (
    "log"
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
}

/* WordCount RPC reply structure */
type WordCountReply struct {
    Length int
}

/* Counts the words in the sent message */
func (wc *WC) WordCount(args *WordCountArgs, reply *WordCountReply) error {
    reply.Length = len(strings.Split(args.Message, " "))
    return nil
}

/* Start serving WordCount RPCs on the provided port */
func startWCServer(port int) {
    wc := new(WC)
    rpc.Register(wc)
    // Serve RPC requests on the specified port
    rpc.HandleHTTP()
    listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
    if err != nil {
        log.Fatal("Listen Failed: ", err)
    }
    go http.Serve(listener, nil)
}

/* Start up a client and connect to localhost *
 * RPC server on the specified port.          *
 * Returns the connected client.              */
func startWCClient(port int) *rpc.Client {
    client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(port))
    if err != nil {
        log.Fatal("Failed to connect to server: ", err)
    }
    return client
}

/* Sends a WordCount RPC with the provided message.   *
 * If async is true, the call is sent asynchronously. *
 * Returns the number of words in the message.        */
func sendRPC(client *rpc.Client, message string, async bool) int {
    args := WordCountArgs{message}
    var reply WordCountReply

    // Send RPC synchronously
    if !async {
        err := client.Call("WC.WordCount", &args, &reply)
        if err != nil {
            log.Fatal("WordCount RPC Failed: ", err)
        }

    // Send RPC asynchronously
    } else {
        wcCall := client.Go("WC.WordCount", &args, &reply, nil)
        // Blocks until call is completed
        <-wcCall.Done
        if wcCall.Error != nil {
            log.Fatal("WordCount RPC Failed: ", wcCall.Error)
        }
    }

    return reply.Length
}

/********************
 *    RPC TESTS     *
 ********************/

/* Whether an RPC server has been started already */
var serverStarted bool

/* Port server listens to RPCs on */
var port int

/* Performs setup for the rpc tests */
func init() {
    port = 1234
    startWCServer(port)
}

/* Tests a single synchronous and asynchronous WordCount RPC */
func TestRPCBasic(t *testing.T) {
    client := startWCClient(port)
    defer client.Close()

    result := sendRPC(client, "This message has five words.", false)
    assertEqual(t, result, 5, "Expected: 5\tReceived: "+strconv.Itoa(result))

    result = sendRPC(client, "And this message has six words.", true)
    assertEqual(t, result, 6, "Expected: 6\tReceived: "+strconv.Itoa(result))
}

/* Tests concurrent WordCount RPCs */
func TestRPCConcurrent(t *testing.T) {
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
