package bayou

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
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
	id	  int
	// Represents the other bayou servers
	peers []*rpc.Client

	// Whether this server is active
	isActive bool

	// Holds the server's stable state
	commitDB *sql.DB
	// Holds all (committed and tentative) server state
	fullDB	 *sql.DB

	// Listener for shutting down the RPC server
	rpcListener net.Listener

	// Current logical time
	currentTime VectorClock

	// Various locks
	dbLock		*sync.Mutex
	logLock		*sync.Mutex
	persistLock *sync.Mutex

	// Whether this server is the primary
	IsPrimary bool

	// Index of latest committed log entry
	CommitIndex int

	// Stores ops: committed, lower timestamp entries are at the head
	WriteLog []LogEntry
	// Stores undo operations for all tentative ops
	UndoLog	 []LogEntry
	// Operations that conflict and fail to merge are stored here
	ErrorLog []LogEntry

	// Maintains timestamp of latest write discarded from each server
	Omitted	VectorClock
}

/* Vector Clock: parallel array holding a monotonically *
 * increasing logical time (int) for each peer server   */
type VectorClock []int

/* Represents an entry in a Bayou server log */
type LogEntry struct {
	WriteID	  int
	Timestamp VectorClock
	Op		  Operation
}

/* AntiEntropy RPC arguments structure */
type AntiEntropyArgs struct {
}

/* AntiEntropy RPC reply structure */
type AntiEntropyReply struct {
}

/* Bayou Read RPC arguments structure */
type ReadArgs struct {
}

/* Bayou Read RPC arguments structure */
type ReadReply struct {
}

/* Bayou Write RPC arguments structure */
type WriteArgs struct {
	WriteID int
	Op	    Operation
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
	Query func(*sql.DB)
	Desc  string
}

/* Dependency check function type:   *
 * Takes a database, and returns     *
 * whether the dependencies are held */
type DepCheck func(*sql.DB) bool

/* Merge process function type:      *
 * Takes a database, and returns     *
 * whether the conflict was resolved */
type MergeProc func(*sql.DB) bool

/* Read function type:                       *
 * Takes a database, and returns some result */
type ReadFunc func(*sql.DB) interface{}

/****************************
 *   BAYOU SERVER METHODS   *
 ****************************/

 /* Returns a new Bayou Server               *
 * Loads initial data and starts RPC handler */
func NewBayouServer(id int, peers []*rpc.Client, commitDB *sql.DB,
		fullDB *sql.DB, port int) *BayouServer {
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

	// TODO: Load persistent data

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

/* Anti-Entropy RPC Handler */
func (server *BayouServer) AntiEntropy(args *AntiEntropyArgs,
		reply *AntiEntropyArgs) {
}

/* Bayou Read RPC Handler */
func (server *BayouServer) Read(args *ReadArgs, reply *ReadReply) {
}

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

/* Rolls back the full view to just after    *
 * the log entry with the specified write ID */
func (server *BayouServer) rollbackDB(rollbackPoint int) {
}


/****************************
 *   VECTOR CLOCK METHODS   *
 ****************************/

/* Returns a new vector clock of the specified length */
func NewVectorClock(length int) VectorClock {
	return make([]int, length)
}

/* Sets the logical time at idx to specified value        *
 * Returns an error if newTime is less than current value */
func (vc VectorClock) SetTime(idx int, newTime int) error {
	if (newTime < vc[idx]) {
		return errors.New("SetTime Failed: New time less than current time")
	}
	vc[idx] = newTime
	return nil
}

/* Increments the logical time at idx */
func (vc VectorClock) Inc(idx int) {
	vc[idx] = vc[idx] + 1
}

/* Returns whether this vector clock is *
 * strictly "less than" the other one   */
func (vc VectorClock) LessThan(other VectorClock) bool {
	if len(vc) != len(other) {
		debugf("WARNING: Vector clocks of different lengths were compared:\n" +
			"This: " + vc.String() + "\tOther: " + other.String())
		return false
	}
	// vc is less than other iff each logical time is less
	// than or equal to the other's logical time for each
	// peer, and at least one of those is strictly less than
	strictly_less_seen := false
	for idx, _ := range vc {
		if !strictly_less_seen && vc[idx] < other[idx] {
			strictly_less_seen = true
		}
		if (vc[idx] > other[idx]) {
			return false
		}
	}
	return strictly_less_seen
}

/* Sets all logical clocks to the max of  *
 * this and the other VC's logical clocks */
func (vc VectorClock) Max(other VectorClock) {
	if len(vc) != len(other) {
		debugf("WARNING: Vector clocks of different lengths were maxed:\n" +
			"This: " + vc.String() + "\tOther: " + other.String())
	}
	// Update logical clock if other one is higher,
	// or append to vector clock if other one is longer
	for idx, _ := range other {
		if idx >= len(vc) {
			vc = append(vc, other[idx])
		} else if vc[idx] < other[idx] {
			vc[idx] = other[idx]
		}
	}
}

func (vc VectorClock) String() string {
	return "VC: " +  strings.Trim(strings.Replace(fmt.Sprint(([]int)(vc)),
		" ", ", ", -1), "[]")
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

