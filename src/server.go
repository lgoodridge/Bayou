package bayou

import (
    "fmt"
    "net"
    "net/http"
    "net/rpc"
    "sync"
)

/*****************
 *   CONSTANTS   *
 *****************/

// TODO: Give these actual values, and implement checkpointing
/* Save a database checkpoint after these many tentative writes */
const OPS_PER_CHECKPOINT int = -1
/* Save these many checkpoints (thus, you can rollback a *
 * maximum of NUM_CHECKPOINTS * OPS_PER_CHECKPOINT ops)  */
const NUM_CHECKPOINTS int = -1

/************************
 *   TYPE DEFINITIONS   *
 ************************/

/* Go object representing a Bayou Server */
type BayouServer struct {
    // Unique index into peers array
    id    int
    // Represents the other bayou servers
    peers []*rpc.Client

    // Whether this server is active
    isActive bool

    // Holds the server's stable state
    commitDB *BayouDB
    // Holds all (committed and tentative) server state
    fullDB   *BayouDB

    // Listener for shutting down the RPC server
    rpcListener net.Listener

    // Current logical time
    currentTime VectorClock

    // Various locks
    dbLock      *sync.Mutex
    logLock     *sync.Mutex
    persistLock *sync.Mutex

    // Whether this server is the primary
    IsPrimary bool

    // Index of latest committed log entry
    CommitIndex int

    // Stores ops: committed, lower timestamp entries are at the head
    WriteLog []LogEntry
    // Stores undo operations for all tentative ops
    UndoLog  []LogEntry
    // Operations that conflict and fail to merge are stored here
    ErrorLog []LogEntry

    // Maintains timestamp of latest write discarded from each server
    Omitted VectorClock
}

/* Represents an entry in a Bayou server log */
type LogEntry struct {
    WriteID   int
    Timestamp VectorClock
    Op        Operation
}

/* AntiEntropy RPC arguments structure */
type AntiEntropyArgs struct {
}

/* AntiEntropy RPC reply structure */
type AntiEntropyReply struct {
}

/* Bayou Read RPC arguments structure */
type ReadArgs struct {
    Read     ReadFunc
    ToCommit bool
}

/* Bayou Read RPC arguments structure */
type ReadReply struct {
    Data interface{}
}

/* Bayou Write RPC arguments structure */
type WriteArgs struct {
    WriteID int
    Op      Operation
    Undo    Operation
    Check   DepCheck
    Merge   MergeProc
}

/* Bayou Write RPC reply structure */
type WriteReply struct {
    HasConflict bool
    WasResolved bool
}

/* Update or Undo operation type               *
 * Contains a function operating on the        *
 * database and a description of that function */ 
type Operation struct {
    Query func(*BayouDB)
    Desc  string
}

/* Dependency check function type:   *
 * Takes a database, and returns     *
 * whether the dependencies are held */
type DepCheck func(*BayouDB) bool

/* Merge process function type:      *
 * Takes a database, and returns     *
 * whether the conflict was resolved */
type MergeProc func(*BayouDB) bool

/* Read function type:                       *
 * Takes a database, and returns some result */
type ReadFunc func(*BayouDB) interface{}

/****************************
 *   BAYOU SERVER METHODS   *
 ****************************/

/* Returns a new Bayou Server                *
 * Loads initial data and starts RPC handler */
func NewBayouServer(id int, peers []*rpc.Client, commitDB *BayouDB,
        fullDB *BayouDB, port int) *BayouServer {
    server := &BayouServer{}
    server.id = id
    server.peers = peers
    server.commitDB = commitDB
    server.fullDB = fullDB

    // Set Initial State
    server.isActive = true
    server.currentTime = NewVectorClock(len(peers))
    server.dbLock = &sync.Mutex{}
    server.logLock = &sync.Mutex{}
    server.persistLock = &sync.Mutex{}
    server.IsPrimary = false
    server.CommitIndex = 0
    server.Omitted = NewVectorClock(len(peers))

    // Load persistent data (if there is any)
    server.loadPersist()

    // Start RPC server
    server.startRPCServer(port)

    debugf("Initialized Bayou Server #%d", server.id)
    return server
}

/* Formally "begins" a Bayou Server                                 *
 * Starts inter-server communication and other tasks                *
 * Starts go-routines for long-running work and returns immediately */
func (server *BayouServer) Begin() {
    // TODO: Start Anti-Entropy communication
    debugf("Server $%d begun", server.id)
}

/* "Kills" a Bayou Server, ending inter-server *
 * communication and RPC handling              */
func (server *BayouServer) Kill() {
    server.isActive = false
    server.rpcListener.Close()
}

// TODO
/* Anti-Entropy RPC Handler */
func (server *BayouServer) AntiEntropy(args *AntiEntropyArgs,
        reply *AntiEntropyArgs) {
}

/* Bayou Read RPC Handler                        *
 * Returns result of the user-defined read query *
 * on either the committed or full database      */
func (server *BayouServer) Read(args *ReadArgs, reply *ReadReply) {
    var data interface{}
    if (args.ToCommit) {
        data = args.Read(server.commitDB)
    } else {
        data = args.Read(server.fullDB)
    }
    reply.Data = data
}

// TODO (Lance)
/* Bayou Write RPC Handler */
func (server *BayouServer) Write(args *WriteArgs, reply *WriteReply) {
}

/* Starts serving RPCs on the provided port */
func (server *BayouServer) startRPCServer(port int) {
    rpcServer := rpc.NewServer()
    rpcServer.Register(server)

    // RPCs handlers are registered to the default server mux,
    // so temporarily change it to allow multiple registrations
    oldMux := http.DefaultServeMux
    newMux := http.NewServeMux()
    http.DefaultServeMux = newMux

    // Register RPC handler, and restore default serve mux
    rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
    http.DefaultServeMux = oldMux

    // Listen and serve on the specified port
    var err error
    server.rpcListener, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
    if err != nil {
        Log.Fatal("Listen Failed: ", err)
    }
    go http.Serve(server.rpcListener, newMux)
    
    debugf("Server #%d listening on port %d", server.id, port)
}

/* Applies an operation to the server's database      *
 * If toCommit is true, it is applied to the server's *
 * commit view, else it is applied to the full view   *
 * Returns whether there was a conflict, and if so,   *
 * whether it was resolved                            */
func (server *BayouServer) applyToDB(toCommit bool, op Operation, 
        depcheck DepCheck, merge MergeProc) (hasConflict bool, resolved bool) {
    // Get the server to apply the operation on
    db := server.fullDB
    if toCommit {
        db = server.commitDB
    }

    // If there is no dependency conflicts, apply the operation to
    // the database. If there is, try to apply the merge function
    if (depcheck(db)) {
        op.Query(db)
        hasConflict = false
        resolved = true
    } else {
        hasConflict = true
        if (merge(db)) {
            resolved = true
        } else {
            resolved = false
        }
    }

    return
}

// TODO (Lance)
/* Rolls back the full view to just after    *
 * the log entry with the specified write ID */
func (server *BayouServer) rollbackDB(rollbackPoint int) {
}

// TODO (David)
/* Saves server data to stable storage */
func (server *BayouServer) savePersist() {
}

// TODO (David)
/* Loads server data from stable storage */
func (server *BayouServer) loadPersist() {
}

/*******************
 *   LOG METHODS   *
 *******************/

func logToString(log []LogEntry) string {
    logStr := ""
    for _, entry := range log {
        logStr = logStr + entry.String() + "\n"
    }
    return logStr
}

func (entry LogEntry) String() string {
    return fmt.Sprintf("#%d: ", entry.WriteID) + entry.Op.Desc
}

