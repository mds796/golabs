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
	opIndex               int             // The operation index in the log assigned to the most recently received request, initially 0.
	uncommittedOperations OperationsQueue // a priority queue of the operations to be added to the log
	timeLastCommit        time.Time       // The last time a backup updated its commit index
	noCommitThreshold     time.Duration   // The amount of time until not hearing from the primary is grounds for recovery or a view change
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
	CommitIndex    int           // Last commit index
	Success        bool          // whether the ViewChange request has been accepted/rejected
}

// StartViewArgs defines the arguments for the StartView RPC
type StartViewArgs struct {
	View        int           // the new view which has completed view-change
	Log         []interface{} // the log associated with the new new
	CommitIndex int           // Last commit index
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
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.commitIndex >= index {
		return true
	}
	return false
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
		peers:                 peers,
		me:                    me,
		currentView:           startingView,
		lastNormalView:        startingView,
		status:                NORMAL,
		uncommittedOperations: make(OperationsQueue, 0),
		timeLastCommit:        time.Now(),
		noCommitThreshold:     2 * time.Second,
	}

	heap.Init(&srv.uncommittedOperations)

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
func (srv *PBServer) Start(command interface{}) (index int, view int, ok bool) {
	// do not process command if status is not NORMAL
	// and if i am not the primary in the current view
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if !srv.isNormalPrimary() {
		return -1, srv.currentView, false
	}

	// Normally, we would check the client table to see if we have already serviced this request.
	// However, we do not have a last request number and cannot add one to the Start function.
	srv.appendCommand(command)

	// Normally, we would update the client table with the new request number before sending Prepare messages.
	arguments := &PrepareArgs{View: srv.currentView, PrimaryCommit: srv.commitIndex, Index: srv.opIndex, Entry: command}

	log.Printf("Primary %v - Preparing (view: %v op: %v commit: %v entry: %v)", srv.me, srv.currentView, arguments.Index, srv.commitIndex, arguments.Entry)

	replies := make(chan *PrepareReply, len(srv.peers))

	for peer := range srv.peers {
		if peer != srv.me {
			go srv.primarySendPrepare(peer, arguments, replies)
		}
	}

	go srv.primaryAwaitPrepare(arguments, replies)

	return srv.opIndex, srv.currentView, true
}

// assumes the mutex is already locked
func (srv *PBServer) appendCommand(command interface{}) {
	srv.opIndex++
	srv.log = append(srv.log, command)
}

func (srv *PBServer) replicationFactor() int {
	f := (len(srv.peers) - 1) / 2

	if f <= 0 {
		log.Fatalf("The replication factor f for the PBServer cannot be less than 1. %v <= 0", f)
	}

	return f
}

// exmple code to send an AppendEntries RPC to a server.
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

	reply.View = srv.currentView
	reply.Success = srv.isNormalPrimary() && args.View <= srv.currentView

	if reply.Success {
		reply.PrimaryCommit = srv.commitIndex
		reply.Entries = srv.log
	}

	log.Printf("Node %d received recover request in view %d from %d", srv.me, srv.currentView, args.Server)
}

// PromptViewChange starts a view change. Some external oracle prompts the primary of the newView to
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

	vcArgs := &ViewChangeArgs{View: newView}
	vcReplyChan := make(chan *ViewChangeReply, len(srv.peers))

	srv.sendViewChange(vcArgs, vcReplyChan)

	// wait to receive ViewChange replies
	// if view change succeeds, send StartView RPC
	go func() {
		majority := srv.replicationFactor()

		successReplies := make([]*ViewChangeReply, 0, len(srv.peers))
		success := 0
		failure := 0

		for i := 1; success < majority && i < len(srv.peers); i++ {
			r := <-vcReplyChan

			if r != nil {
				success++
				successReplies = append(successReplies, r)
			} else {
				failure++
			}

			if (success+failure == len(srv.peers)) || success >= majority {
				break
			}
		}

		log.Println("Received ViewChange replies from a majority of the replicas.")

		ok, newLog, newCommitIndex := srv.determineNewViewLog(successReplies)
		if !ok {
			log.Printf("Unable to determine the log for the new view. Received %v replies, needed %v.", len(successReplies), srv.replicationFactor())
			return
		}

		log.Printf("Determine %d as the log for the new view %d with commit index %d.", newLog, newView, newCommitIndex)

		svArgs := &StartViewArgs{
			View:        vcArgs.View,
			Log:         newLog,
			CommitIndex: srv.commitIndex,
		}
		// send StartView to all servers including myself
		for i := 0; i < len(srv.peers); i++ {
			var reply StartViewReply
			go func(server int) {
				log.Printf("node-%d sending StartView v=%d to node-%d\n", srv.me, svArgs.View, server)
				srv.peers[server].Call("PBServer.StartView", svArgs, &reply)
			}(i)
		}
	}()
}

func (srv *PBServer) sendViewChange(args *ViewChangeArgs, replies chan *ViewChangeReply) {
	// send ViewChange to all servers including myself
	for i := 0; i < len(srv.peers); i++ {
		go func(server int) {
			reply := new(ViewChangeReply)
			ok := srv.peers[server].Call("PBServer.ViewChange", args, reply)

			if ok && reply.Success {
				replies <- reply
			} else {
				replies <- nil
			}
		}(i)
	}
}

// determineNewViewLog is invoked to determine the log for the newView based on
// the collection of replies for successful ViewChange requests.
// if a quorum of successful replies exist, then ok is set to true.
// otherwise, ok = false.
func (srv *PBServer) determineNewViewLog(successReplies []*ViewChangeReply) (ok bool, newViewLog []interface{}, newCommitIndex int) {
	// Your code here
	lastNormalView := -1
	newCommitIndex = -1
	newViewLog = make([]interface{}, 0)

	// the new log is the one with the highest last normal view. If more than one such log exists, the longest log is used.
	for i := range successReplies {
		reply := successReplies[i]
		if reply.Success && reply.LastNormalView >= lastNormalView && len(reply.Log) > len(newViewLog) {
			newViewLog = reply.Log
			lastNormalView = reply.LastNormalView

			if reply.CommitIndex > newCommitIndex {
				newCommitIndex = reply.CommitIndex
			}
		}
	}

	return len(successReplies) >= srv.replicationFactor(), newViewLog, newCommitIndex
}

// ViewChange is the RPC handler to process ViewChange RPC.
func (srv *PBServer) ViewChange(args *ViewChangeArgs, reply *ViewChangeReply) {
	// Your code here
	srv.mu.Lock()
	defer srv.mu.Unlock()

	reply.LastNormalView = srv.lastNormalView
	reply.Log = srv.log
	reply.CommitIndex = srv.commitIndex
	reply.Success = args.View > srv.currentView

	if reply.Success {
		srv.status = VIEWCHANGE
	}

	log.Printf("node-%d received ViewChange for view %d with status %v.\n", srv.me, args.View, srv.status)
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
		srv.commitIndex = args.CommitIndex
		srv.opIndex = len(args.Log) - 1
		srv.status = NORMAL
	}
}
