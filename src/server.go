package bayou

/************************
 *   TYPE DEFINITIONS   *
 ************************/

/* Go object implemeting a Bayou Server */
type BayouServer struct {
	id int

	writeLog []LogEntry
	undoLog  []LogEntry

	ommited    map[int]int
	timestamps map[int]int

	PersistLog []LogEntry
}

/* Represents an entry in a Bayou server log */
type LogEntry struct {
}

/* AntiEntropy RPC arguments structure */
type AntiEntropyArgs struct {
	message string
}

/* AntiEntropy RPC reply structure */
type AntiEntropyReply struct {
}
