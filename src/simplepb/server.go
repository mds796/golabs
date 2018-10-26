// Authors:
// Miguel David Salcedo   - NetID: mds796
// Matheus Vieira Portela - NetID: mvp307

package simplepb

//
// This is a outline of primary-backup replication based on a simplifed version of Viewstamp replication.
//
//
//

import (
	"container/heap"
	"labrpc"
	"log"
	"sync"
	"time"
)

// the 3 possible server status
const (
	NORMAL = iota
	VIEWCHANGE
	RECOVERING
)

// PBServer defines the state of a replica server (either primary or backup)
type PBServer struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	me             int                 // this peer's index into peers[]
	currentView    int                 // what this peer believes to be the current active view
	status         int                 // the server's current status (NORMAL, VIEWCHANGE or RECOVERING)
	lastNormalView int                 // the latest view which had a NORMAL status

	log         []interface{} // the log of "commands"
	commitIndex int           // all log entries <= commitIndex are considered to have been committed.

	// ... other state that you might need ...
	opIndex             int             // The operation index in the log assigned to the most recently received request, initially 0.
	operationsToPrepare OperationsQueue // a priority queue of the operations to be added to the log
	prepareTimeout      time.Duration   // The amount of time until not hearing from the primary is grounds for recovery or a view change
}

// PrepareArgs defines the arguments for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC args struct
type PrepareArgs struct {
	View          int         // the primary's current view
	PrimaryCommit int         // the primary's commitIndex
	Index         int         // the index position at which the log entry is to be replicated on backups
	Entry         interface{} // the log entry to be replicated
}

// PrepareReply defines the reply for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC reply struct
type PrepareReply struct {
	View    int  // the backup's current view
	Success bool // whether the Prepare request has been accepted or rejected
}

// RecoveryArgs defines the arguments for the Recovery RPC
type RecoveryArgs struct {
	View   int // the view that the backup would like to synchronize with
	Server int // the server sending the Recovery RPC (for debugging)
}

// RecoveryReply defines the reply for the Recovery RPC
type RecoveryReply struct {
	View          int           // the view of the primary
	Entries       []interface{} // the primary's log including entries replicated up to and including the view.
	PrimaryCommit int           // the primary's commitIndex
	Success       bool          // whether the Recovery request has been accepted or rejected
}

// ViewChangeArgs defines the arguments for the ViewChange RPC
type ViewChangeArgs struct {
	View int // the new view to be changed into
}

// ViewChangeReply defines the reply for the ViewChange RPC
type ViewChangeReply struct {
	LastNormalView int           // the latest view which had a NORMAL status at the server
	Log            []interface{} // the log at the server
	Success        bool          // whether the ViewChange request has been accepted/rejected
}

// StartViewArgs defines the arguments for the StartView RPC
type StartViewArgs struct {
	View int           // the new view which has completed view-change
	Log  []interface{} // the log associated with the new new
}

// StartViewReply defines the reply for the StartView RPC
type StartViewReply struct {
}

// GetPrimary is an auxilary function that returns the server index of the
// primary server given the view number (and the total number of replica servers)
func GetPrimary(view int, nservers int) int {
	return view % nservers
}

// IsPrimary is a predicate that returns true if this server is the primary for its current view, false otherwise.
func (srv *PBServer) IsPrimary() bool {
	return GetPrimary(srv.currentView, len(srv.peers)) == srv.me
}

// IsCommitted is called by tester to check whether an index position
// has been considered committed by this server
func (srv *PBServer) IsCommitted(index int) (committed bool) {
	return srv.commitIndex >= index
}

// ViewStatus is called by tester to find out the current view of this server
// and whether this view has a status of NORMAL.
func (srv *PBServer) ViewStatus() (currentView int, statusIsNormal bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	return srv.currentView, srv.status == NORMAL
}

// GetEntryAtIndex is called by tester to return the command replicated at
// a specific log index. If the server's log is shorter than "index", then
// ok = false, otherwise, ok = true
func (srv *PBServer) GetEntryAtIndex(index int) (ok bool, command interface{}) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.log) > index {
		return true, srv.log[index]
	}
	return false, command
}

// Kill is called by tester to clean up (e.g. stop the current server)
// before moving on to the next test
func (srv *PBServer) Kill() {
	// Your code here, if necessary
}

// Make is called by tester to create and initalize a PBServer
// peers is the list of RPC endpoints to every server (including self)
// me is this server's index into peers.
// startingView is the initial view (set to be zero) that all servers start in
func Make(peers []*labrpc.ClientEnd, me int, startingView int) *PBServer {
	srv := &PBServer{
		peers:               peers,
		me:                  me,
		currentView:         startingView,
		lastNormalView:      startingView,
		status:              NORMAL,
		operationsToPrepare: make(OperationsQueue, 0, 2),
		prepareTimeout:      100 * time.Millisecond,
	}

	heap.Init(&srv.operationsToPrepare)

	// all servers' log are initialized with a dummy command at index 0
	var v interface{}
	srv.log = append(srv.log, v)

	// Your other initialization code here, if there's any
	return srv
}

// Start is invoked by tester on some replica server to replicate a
// command.  Only the primary should process this request by appending
// the command to its log and then return *immediately* (while the log is being replicated to backup servers).
// if this server isn't the primary, returns false.
// Note that since the function returns immediately, there is no guarantee that this command
// will ever be committed upon return, since the primary
// may subsequently fail before replicating the command to all servers
//
// The first return value is the index that the command will appear at
// *if it's eventually committed*. The second return value is the current
// view. The third return value is true if this server believes it is
// the primary.
func (srv *PBServer) Start(command interface{}) (
	index int, view int, ok bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	// do not process command if status is not NORMAL
	// and if i am not the primary in the current view
	if srv.status != NORMAL {
		return -1, srv.currentView, false
	} else if GetPrimary(srv.currentView, len(srv.peers)) != srv.me {
		return -1, srv.currentView, false
	}

	srv.opIndex++
	srv.log = append(srv.log, command)

	// Your code here
	srv.prepareAllReplicas()

	return srv.opIndex, srv.currentView, true
}

// prepares an operation at all replicas.
func (srv *PBServer) prepareAllReplicas() {
	log.Printf("Primary %d preparing op %d\n", srv.me, srv.opIndex)

	args := &PrepareArgs{View: srv.currentView, PrimaryCommit: srv.commitIndex, Index: srv.opIndex, Entry: srv.log[srv.opIndex]}
	commit := &Commit{args: args}

	for peer := range srv.peers {
		if peer != srv.me {
			go srv.prepareReplica(peer, commit)
		}
	}
}

// prepareReplica sends a prepare request to a single replica.
// This function acquires the mutex lock in order to handle configuration changes.
func (srv *PBServer) prepareReplica(peer int, commit *Commit) {
	reply := new(PrepareReply)

	srv.sendPrepare(peer, commit.args, reply)
	srv.CommitOperation(commit, reply)
}

// CommitOperation updates the operation and commits it if enough replies were received
func (srv *PBServer) CommitOperation(commit *Commit, reply *PrepareReply) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// Update reply counts
	if reply.Success {
		commit.replicas++
	} else {
		commit.failures++
	}

	if commit.args.View < srv.currentView || reply.View > srv.currentView {
		// Potentially no longer the primary, make sure this operation will not be committed
	} else if reply.Success && commit.replicas == srv.replicationFactor() {
		// commit the operation
		srv.commitIndex = Max(srv.commitIndex, commit.args.Index)
		log.Printf("Committed the log entry %d\n", commit.args.Index)
	}
}

// Max returns the maximum of a and b
func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (srv *PBServer) replicationFactor() int {
	f := (len(srv.peers) - 1) / 2

	if f <= 0 {
		log.Fatalf("The replication factor f for the PBServer cannot be less than 1. %v <= 0", f)
	}

	return f
}

// example code to send an AppendEntries RPC to a server.
// server is the index of the target server in srv.peers[].
// expects RPC arguments in args.
// The RPC library fills in *reply with RPC reply, so caller should pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
func (srv *PBServer) sendPrepare(server int, args *PrepareArgs, reply *PrepareReply) bool {
	ok := srv.peers[server].Call("PBServer.Prepare", args, reply)
	return ok
}

// Prepare is the RPC handler for the Prepare RPC
func (srv *PBServer) Prepare(args *PrepareArgs, reply *PrepareReply) {
	srv.backupPrepare(args, reply)
}

func (srv *PBServer) sendRecovery(server int, args *RecoveryArgs, reply *RecoveryReply) bool {
	ok := srv.peers[server].Call("PBServer.Recovery", args, reply)
	return ok
}

// Recovery is the RPC handler for the Recovery RPC
func (srv *PBServer) Recovery(args *RecoveryArgs, reply *RecoveryReply) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	isPrimary := GetPrimary(Max(args.View, srv.currentView), len(srv.peers)) == srv.me
	statusNormal := srv.status == NORMAL
	isHealthy := statusNormal && isPrimary
	newerView := srv.currentView >= args.View

	reply.Success = isHealthy && newerView
	reply.PrimaryCommit = srv.commitIndex
	reply.Entries = srv.log
	reply.View = srv.currentView

	log.Printf("Node %d received recover request in view %d from %d", srv.me, srv.currentView, args.Server)

	if reply.Success && srv.opIndex > srv.commitIndex {
		srv.prepareAllReplicas()
	}
}

// Some external oracle prompts the primary of the newView to
// switch to the newView.
// PromptViewChange just kicks start the view change protocol to move to the newView
// It does not block waiting for the view change process to complete.
func (srv *PBServer) PromptViewChange(newView int) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	newPrimary := GetPrimary(newView, len(srv.peers))

	if newPrimary != srv.me { //only primary of newView should do view change
		return
	} else if newView <= srv.currentView {
		return
	}
	vcArgs := &ViewChangeArgs{
		View: newView,
	}
	vcReplyChan := make(chan *ViewChangeReply, len(srv.peers))
	// send ViewChange to all servers including myself
	for i := 0; i < len(srv.peers); i++ {
		go func(server int) {
			var reply ViewChangeReply
			ok := srv.peers[server].Call("PBServer.ViewChange", vcArgs, &reply)
			// fmt.Printf("node-%d (nReplies %d) received reply ok=%v reply=%v\n", srv.me, nReplies, ok, r.reply)
			if ok {
				vcReplyChan <- &reply
			} else {
				vcReplyChan <- nil
			}
		}(i)
	}

	// wait to receive ViewChange replies
	// if view change succeeds, send StartView RPC
	go func() {
		var successReplies []*ViewChangeReply
		var nReplies int
		majority := len(srv.peers)/2 + 1
		for r := range vcReplyChan {
			nReplies++
			if r != nil && r.Success {
				successReplies = append(successReplies, r)
			}
			if nReplies == len(srv.peers) || len(successReplies) == majority {
				break
			}
		}
		ok, log := srv.determineNewViewLog(successReplies)
		if !ok {
			return
		}
		svArgs := &StartViewArgs{
			View: vcArgs.View,
			Log:  log,
		}
		// send StartView to all servers including myself
		for i := 0; i < len(srv.peers); i++ {
			var reply StartViewReply
			go func(server int) {
				// fmt.Printf("node-%d sending StartView v=%d to node-%d\n", srv.me, svArgs.View, server)
				srv.peers[server].Call("PBServer.StartView", svArgs, &reply)
			}(i)
		}
	}()
}

// determineNewViewLog is invoked to determine the log for the newView based on
// the collection of replies for successful ViewChange requests.
// if a quorum of successful replies exist, then ok is set to true.
// otherwise, ok = false.
func (srv *PBServer) determineNewViewLog(successReplies []*ViewChangeReply) (ok bool, newViewLog []interface{}) {
	// Your code here
	lastNormalView := srv.lastNormalView
	newViewLog = srv.log

	// the new log is the one with the highest last normal view. If more than one such log exists, the longest log is used.
	for i := range successReplies {
		reply := successReplies[i]
		if reply.Success && reply.LastNormalView >= lastNormalView && len(reply.Log) > len(newViewLog) {
			newViewLog = reply.Log
			lastNormalView = reply.LastNormalView
		}
	}

	return len(successReplies) >= srv.replicationFactor(), newViewLog
}

// ViewChange is the RPC handler to process ViewChange RPC.
func (srv *PBServer) ViewChange(args *ViewChangeArgs, reply *ViewChangeReply) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	reply.LastNormalView = srv.lastNormalView
	reply.Log = srv.log
	reply.Success = args.View > srv.currentView

	log.Printf("node-%d received ViewChange for view %d with status %v, and log %v.\n", srv.me, args.View, srv.status, srv.log)

	if reply.Success {
		srv.status = VIEWCHANGE
	}
}

// StartView is the RPC handler to process StartView RPC.
func (srv *PBServer) StartView(args *StartViewArgs, reply *StartViewReply) {
	// Your code here
	srv.mu.Lock()
	defer srv.mu.Unlock()

	log.Printf("node-%d received StartView for view %d.\n", srv.me, args.View)

	if args.View > srv.currentView {
		srv.currentView = args.View
		srv.lastNormalView = args.View
		srv.log = args.Log
		srv.opIndex = len(args.Log) - 1
		srv.status = NORMAL
	}
}

// Commit are operations yet to be committed
type Commit struct {
	args     *PrepareArgs
	replicas int // used by the primary to count the number of replicas for an operation
	failures int // used by the primary to count the number of failures for an operation
}
