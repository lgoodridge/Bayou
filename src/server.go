package bayou

import (
    "fmt"
    "net"
    "net/http"
    "net/rpc"
    "sync"
    "encoding/gob"
    "bytes"
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

    // Maintains timestamp of latest commit agreed upon by each server
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
    SenderID      int
    CommitSet     []LogEntry
    TentativeSet  []LogEntry
    UndoSet       []LogEntry
    OmitTimestamp VectorClock
}

/* AntiEntropy RPC reply structure */
type AntiEntropyReply struct {
    Succeeded     bool
    MustUpdateLog bool
    CommitSet     []LogEntry
    TentativeSet  []LogEntry
    UndoSet       []LogEntry
    OmitTimestamp VectorClock
}

/* Ping RPC arguments structure */
type PingArgs struct {
    SenderID int
}

/* Ping RPC reply structure */
type PingReply struct {
    Alive bool
}

/* Bayou Read RPC arguments structure */
type ReadArgs struct {
    Read       ReadFunc
    FromCommit bool
}

/* Bayou Read RPC reply structure */
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
    server.updateClocks()

    // Start RPC server
    server.startRPCServer(port)

    debugf("Initialized Bayou Server #%d", server.id)
    return server
}

/* Formally "begins" a Bayou Server                  *
 * Starts inter-server communication and other tasks */
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

/* Anti-Entropy RPC Handler                   *
 * Resolve this server's log and the provided *
 * log and return the agreed upon result log  */
func (server *BayouServer) AntiEntropy(args *AntiEntropyArgs,
        reply *AntiEntropyReply) error {
    var useMyLog bool
    var otherCommitClock VectorClock
    var otherTentativeClock VectorClock

    var minOmitTimestamp VectorClock
    timestampsDiffer := false
    myOmitTimestamp := server.Omitted[args.SenderID]

    // If the omit timestamps are not the same, fail
    // immediately and send back the lower timestamp
    if myOmitTimestamp.LessThan(args.OmitTimestamp) {
        timestampsDiffer = true
        minOmitTimestamp = myOmitTimestamp
    } else if args.OmitTimestamp.LessThan(myOmitTimestamp) {
        timestampsDiffer = true
        minOmitTimestamp = args.OmitTimestamp
    }
    if timestampsDiffer {
        debugf("Omit timestamps for server %d and %d do not match!\n" +
                "Receiver: %s\nSender: %s", server.id, args.SenderID,
                myOmitTimestamp.String(), args.OmitTimestamp.String())
        reply.Succeeded = false
        reply.MustUpdateLog = false
        reply.CommitSet = nil
        reply.TentativeSet = nil
        reply.UndoSet = nil
        reply.OmitTimestamp = minOmitTimestamp
        return nil
    }

    // Calculate the other server's commit and tentative clock
    if len(args.CommitSet) == 0 {
        otherCommitClock = args.OmitTimestamp
    } else {
        otherCommitClock = args.CommitSet[len(args.CommitSet) - 1].Timestamp
    }
    if len(args.TentativeSet) == 0 {
        otherTentativeClock = otherCommitClock
    } else {
        otherTentativeClock =
                args.TentativeSet[len(args.TentativeSet) - 1].Timestamp
    }

    server.logLock.Lock()
    defer server.logLock.Unlock()

    // Determine which server's log to follow:
    // Use the log with the greater commit timestamp, or the
    // log with the greater tentative timestamp as a tiebreaker
    if otherCommitClock.LessThan(server.commitClock) {
        useMyLog = true
    } else if server.commitClock.LessThan(otherCommitClock) {
        useMyLog = false
    } else {
        useMyLog = otherTentativeClock.LessThan(server.tentativeClock)
    }

    targetIndex := getLengthAtTime(server.CommitLog, args.OmitTimestamp)
    sharedEndIndex := len(server.CommitLog)
    if useMyLog {
        sharedEndIndex = targetIndex + len(args.CommitSet)
    }

    // Ensure all shared commits are the same
    var myEntry LogEntry
    var otherEntry LogEntry
    for i := targetIndex; i < sharedEndIndex; i++ {
        myEntry = server.CommitLog[i]
        otherEntry = args.CommitSet[i - targetIndex]
        if myEntry.WriteID != otherEntry.WriteID {
            Log.Fatalf("The commit logs of server %d and %d have diverged!\n" +
                    "Receiver: %s\nSender: %s", server.id, args.SenderID,
                    logToString(server.CommitLog[targetIndex:]),
                    logToString(args.CommitSet))
        }
    }

    // Update server state as necessary
    if !useMyLog {
        server.matchLog(args.CommitSet, args.TentativeSet, args.UndoSet,
                args.OmitTimestamp)
    }
    server.Omitted[args.SenderID] = server.commitClock

    // Respond with the chosen results
    if useMyLog {
        reply.CommitSet = make([]LogEntry, len(server.CommitLog) - targetIndex)
        copy(reply.CommitSet, server.CommitLog[targetIndex:])
        reply.TentativeSet = make([]LogEntry, len(server.TentativeLog))
        copy(reply.TentativeSet, server.TentativeLog)
        reply.UndoSet = make([]LogEntry, len(server.UndoLog))
        copy(reply.UndoSet, server.UndoLog)
    } else {
        reply.CommitSet = nil
        reply.TentativeSet = nil
        reply.UndoSet = nil
    }
    reply.Succeeded = true
    reply.MustUpdateLog = useMyLog
    reply.OmitTimestamp = server.commitClock
    return nil
}

/* Ping RPC Handler                             *
 * Test RPC for determining server connectivity *
 * Sets Alive to yes is RPC was received        */
func (server *BayouServer) Ping(args *PingArgs, reply *PingReply) error {
    debugf("Server #%d received ping from %d", server.id, args.SenderID)
    reply.Alive = true
    return nil
}

/* Bayou Read RPC Handler                        *
 * Returns result of the user-defined read query *
 * on either the committed or full database      */
func (server *BayouServer) Read(args *ReadArgs, reply *ReadReply) error {
    server.dbLock.Lock()
    defer server.dbLock.Unlock()

    var data interface{}
    if (args.FromCommit) {
        data = args.Read(server.commitDB)
    } else {
        data = args.Read(server.fullDB)
    }

    reply.Data = data
    return nil
}

/* Bayou Write RPC Handler                       *
 * Returns whether the write had a conflict, and *
 * if so, whether it was successfully resolved   */
func (server *BayouServer) Write(args *WriteArgs, reply *WriteReply) error {
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
    return nil
}

/* Starts serving RPCs on the provided port */
func (server *BayouServer) startRPCServer(port int) {
    rpcServer := rpc.NewServer()

    // Register the server RPCs, temporarily disabling the standard log
    // to ignore the "wrong number of ins" warning from non-RPC methods
    disableStdLog()
    rpcServer.Register(server)
    restoreStdLog()

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

/* Rollsback the database, and applies log entries so that *
 * this server's log matches the provided write sets       */
func (server *BayouServer) matchLog(commitSet []LogEntry,
        tentativeSet []LogEntry, undoSet []LogEntry,
        rollbackTime VectorClock) {
    // Rollback the database to the specified time
    server.rollbackDB(rollbackTime)
    server.updateClocks()

    // Ensure the length of the tentative and undo sets are the same
    if len(tentativeSet) != len(undoSet) {
        Log.Fatalf("Length of tentative and undo sets do not match!\n" +
                "Tentative Set: %s\nUndo Set: %s\n",
                logToString(tentativeSet), logToString(undoSet))
    }

    // Add all entries to the appropiate log, and apply to database
    for _, entry := range commitSet {
        server.CommitLog = append(server.CommitLog, entry)
        server.applyToDB(true, entry.Op, entry.Check, entry.Merge)
    }
    var tentEntry LogEntry
    var undoEntry LogEntry
    for i, _ := range tentativeSet {
        tentEntry = tentativeSet[i]
        undoEntry = undoSet[i]
        server.TentativeLog = append(server.TentativeLog, tentEntry)
        server.UndoLog = append(server.UndoLog, undoEntry)
        server.applyToDB(false, tentEntry.Op, tentEntry.Check, tentEntry.Merge)
    }
    server.updateClocks()
}

/* Applies an operation to the server's database      *
 * If toCommit is true, it is applied to the server's *
 * commit view, else it is applied to the full view   *
 * Returns whether there was a conflict, and if so,   *
 * whether it was resolved                            */
func (server *BayouServer) applyToDB(toCommit bool, op Operation,
        depcheck DepCheck, merge MergeProc) (hasConflict bool, resolved bool) {
    // Get the database to apply the operation on
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

/* Rolls back the full view to the state  *
 * it possessed at the provided timestamp */
func (server *BayouServer) rollbackDB(rollbackTime VectorClock) {
    server.dbLock.Lock()
    defer server.dbLock.Unlock()

    // Apply undo operations until we find the target
    targetIndex := -1
    for i := len(server.TentativeLog) - 1; i >= 0; i-- {
        if rollbackTime.LessThan(server.TentativeLog[i].Timestamp) {
            targetIndex = i
            break
        }
        server.applyToDB(false, server.UndoLog[targetIndex].Op,
            server.UndoLog[targetIndex].Check,
            server.UndoLog[targetIndex].Merge)
    }

    // If all tentative writes occurred after the target time, and
    // the latest commit entry also occurred after the target time,
    // we are trying to rollback committed entries, which is not allowed
    if targetIndex == -1 && len(server.CommitLog) > 0 &&
            !rollbackTime.LessThan(
                    server.CommitLog[len(server.CommitLog) - 1].Timestamp) {
        errMsg := fmt.Sprintf("Programmer Error: Attempted to rollback " +
        "committed entries (Rollback Time: %s)", rollbackTime.String())
        Log.Fatal(errMsg)
    }

    // Truncate the write and undo logs, then save to stable storage
    server.TentativeLog = server.TentativeLog[:targetIndex+1]
    server.UndoLog = server.UndoLog[:targetIndex+1]
    server.savePersist()
}

/* Updates commit and tentative clocks to the        *
 * appropiate values, based on their respective logs */
func (server *BayouServer) updateClocks() {
    lastCommitIdx := len(server.CommitLog) - 1
    lastTentativeIdx := len(server.TentativeLog) - 1
    if lastCommitIdx >= 0 {
        server.commitClock = server.CommitLog[lastCommitIdx].Timestamp
    }
    if lastTentativeIdx >= 0 {
        server.tentativeClock = server.TentativeLog[lastTentativeIdx].Timestamp
    }
}

/* Saves server data to stable storage */
func (server *BayouServer) savePersist() {
    var data bytes.Buffer
//    gob.register(VectorClock)
    enc := gob.NewEncoder(&data)

    err := enc.Encode(server.IsPrimary)
    check(err, "Error encoding: ")

    err = enc.Encode(server.CommitLog)
    check(err, "Error encoding: ")

    err = enc.Encode(server.TentativeLog)
    check(err, "Error encoding: ")

    err = enc.Encode(server.UndoLog)
    check(err, "Error encoding: ")

    err = enc.Encode(server.ErrorLog)
    check(err, "Error encoding: ")

    save(data.Bytes(), server.id)
}

/* Loads server data from stable storage */
func (server *BayouServer) loadPersist() {
    var data bytes.Buffer
    var b  []byte
// TODO: It is likely that each sub group needs
//       to be registered in order to gob correctly
//    gob.register(VectorClock)
    b, err := load(server.id)
    if err != nil {
        debugf("Error : %s \n", err)
        return
    }
    data.Write(b)

    dec := gob.NewDecoder(&data)

    err = dec.Decode(&server.IsPrimary)
    check(err, "Error decoding: ")

    err = dec.Decode(&server.CommitLog)
    check(err, "Error decoding: ")

    err = dec.Decode(&server.TentativeLog)
    check(err, "Error decoding: ")

    err = dec.Decode(&server.UndoLog)
    check(err, "Error decoding: ")

    err = dec.Decode(&server.ErrorLog)
    check(err, "Error decoding: ")
}

