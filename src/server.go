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

    // Timestamp of last committed write
    commitClock    VectorClock
    // Timestamp of last tentative write
    tentativeClock VectorClock

    // Various locks
    dbLock      *sync.Mutex
    logLock     *sync.Mutex
    persistLock *sync.Mutex

    // Whether this server is the primary
    IsPrimary bool

    // Stores committed ops: lower timestamps closer to head
    CommitLog    []LogEntry
    // Stores uncommitted operations
    TentativeLog []LogEntry
    // Stores undo operations for all tentative ops
    UndoLog      []LogEntry
    // Operations that conflict and fail to merge are stored here
    ErrorLog     []LogEntry

    // Maintains timestamp of latest write discarded from each server
    Omitted []VectorClock
}

/* Represents an entry in a Bayou server log */
type LogEntry struct {
    WriteID   int
    Timestamp VectorClock
    Op        Operation
    Check     DepCheck
    Merge     MergeProc
}

/* AntiEntropy RPC arguments structure */
type AntiEntropyArgs struct {
}

/* AntiEntropy RPC reply structure */
type AntiEntropyReply struct {
}

/* Bayou Read RPC arguments structure */
type ReadArgs struct {
    Read       ReadFunc
    FromCommit bool
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
    server.commitClock = NewVectorClock(len(peers))
    server.tentativeClock = NewVectorClock(len(peers))
    server.dbLock = &sync.Mutex{}
    server.logLock = &sync.Mutex{}
    server.persistLock = &sync.Mutex{}
    server.IsPrimary = false
    server.CommitLog = make([]LogEntry, 0)
    server.TentativeLog = make([]LogEntry, 0)
    server.UndoLog = make([]LogEntry, 0)
    server.ErrorLog = make([]LogEntry, 0)
    server.Omitted = make([]VectorClock, len(peers))
    for i, _ := range server.Omitted {
        server.Omitted[i] = NewVectorClock(len(peers))
    }

    // Load persistent data (if there is any)
    server.loadPersist()

    // Replay all writes to their respective database
    for _, entry := range server.CommitLog {
        server.applyToDB(true, entry.Op, entry.Check, entry.Merge)
        server.applyToDB(false, entry.Op, entry.Check, entry.Merge)
    }
    for _, entry := range server.TentativeLog {
        server.applyToDB(false, entry.Op, entry.Check, entry.Merge)
    }

    // Update timestamps to the appropiate values
    lastCommitIdx := len(server.CommitLog) - 1
    lastTentativeIdx := len(server.TentativeLog) - 1
    if lastCommitIdx >= 0 {
        server.commitClock = server.CommitLog[lastCommitIdx].Timestamp
    }
    if lastTentativeIdx >= 0 {
        server.tentativeClock = server.TentativeLog[lastTentativeIdx].Timestamp
    }

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
    if (args.FromCommit) {
        data = args.Read(server.commitDB)
    } else {
        data = args.Read(server.fullDB)
    }
    reply.Data = data
}

/* Bayou Write RPC Handler */
func (server *BayouServer) Write(args *WriteArgs, reply *WriteReply) {
    // Update appropiate vector clock(s)
    server.tentativeClock.Inc(server.id)
    writeClock := server.tentativeClock
    if server.IsPrimary {
        server.commitClock.Inc(server.id)
        writeClock = server.commitClock
    }

    // Create entries for each of the logs
    writeEntry := LogEntry{args.WriteID, writeClock, args.Op,
            args.Check, args.Merge}
    undoEntry := LogEntry{args.WriteID, writeClock, args.Op,
            func(db *BayouDB)(bool){return true}, nil}

    server.logLock.Lock()
    defer server.logLock.Unlock()

    // If this server is the primary, commit the write immediately,
    // else add it as a tentative write and its undo operation to the undo log
    if server.IsPrimary {
        server.CommitLog = append(server.CommitLog, writeEntry)
    } else {
        server.TentativeLog = append(server.TentativeLog, writeEntry)
        server.UndoLog = append(server.UndoLog, undoEntry)
    }

    // Apply write to database(s) and send unresolved conflicts to error log
    hasConflict, resolved := server.applyToDB(false, args.Op,
            args.Check, args.Merge)
    if hasConflict && !resolved {
        server.ErrorLog = append(server.ErrorLog, writeEntry)
    }
    if server.IsPrimary {
        server.applyToDB(true, args.Op, args.Check, args.Merge)
    }
    server.savePersist()

    reply.HasConflict = hasConflict
    reply.WasResolved = resolved
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

    server.dbLock.Lock()
    defer server.dbLock.Unlock()

    // If there are no dependency conflicts, apply the operation to
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

/* Rolls back the full view to just after    *
 * the log entry with the specified write ID */
func (server *BayouServer) rollbackDB(rollbackID int) {
    server.logLock.Lock()
    server.dbLock.Lock()
    defer server.logLock.Unlock()
    defer server.dbLock.Unlock()

    // Apply undo operations until we find the target
    targetIndex := -1
    for i := len(server.TentativeLog) - 1; i >= 0; i-- {
        if server.TentativeLog[i].WriteID == rollbackID {
            targetIndex = i
            break
        }
        server.applyToDB(false, server.UndoLog[targetIndex].Op,
            server.UndoLog[targetIndex].Check,
            server.UndoLog[targetIndex].Merge)
    }

    // If the target entry was never found in the tentative log,
    // and its not the latest commit entry, we are trying to
    // rollback committed entries, which is not allowed
    if targetIndex == -1 && len(server.CommitLog) > 0 &&
            server.CommitLog[len(server.CommitLog) - 1].WriteID != rollbackID {
        errMsg := fmt.Sprintf("Programmer Error: Attempted to rollback " +
                "committed entries (Rollback ID: %d)", rollbackID)
        Log.Fatal(errMsg)
    }

    // Truncate the write and undo logs, then save to stable storage
    server.TentativeLog = server.TentativeLog[:targetIndex+1]
    server.UndoLog = server.UndoLog[:targetIndex+1]
    server.savePersist()
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

