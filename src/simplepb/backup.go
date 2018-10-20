package simplepb

import (
	"container/heap"
	"log"
	"time"
)

// Process prepare messages in the backup, doing state transfer if necessary.
// Enqueues messages received out of order to process them in order.
// However, the RPC may still return a response without processing the operation.
// This case is alright since the master commits all previous operations if any future operations return true.
// So, once we have all in order operations the next operation will eventually return true.
func (srv *PBServer) backupPrepare(arguments *PrepareArgs, reply *PrepareReply) {
	log.Printf("Replica %v - [view %v commit %v op %v] - Received prepare (view: %v op: %v commit: %v entry: %v)", srv.me, srv.currentView, srv.commitIndex, srv.opIndex, srv.currentView, arguments.Index, arguments.PrimaryCommit, arguments.Entry)

	if arguments.View < srv.currentView {
		reply.View = srv.currentView
		reply.Success = false

		return
	}

	// Add the prepared operation to the min-heap for uncommitted ops
	prepare := &Prepare{args: arguments, reply: reply, done: make(chan bool, 1)}
	go srv.backupPrepareInOrder(prepare)
	<-prepare.done
}

// Prepares the next set of operations that are in order from the current opIndex
// Does nothing if the next operation is not in the PrepareQueue.
// For example, if we had operations [2, 4, 5] and we just received a request to prepare #1,
// We would process ops 1 and 2, but not 4 and 5. Once we receive the message to prepare 3,
// only then would we prepare 3, 4, and 5 in order.
// Notice that this may cause the primary to timeout waiting for a response, but that's okay.
func (srv *PBServer) backupPrepareInOrder(prepare *Prepare) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	log.Printf("Replica %v - Preparing (view: %v op: %v commit: %v entry: %v)", srv.me, prepare.args.View, prepare.args.Index, prepare.args.PrimaryCommit, prepare.args.Entry)

	if prepare.args.View > srv.currentView {
		// this replica is in an old view
		srv.backupRecover()
	}

	// Set recovery timer if primary does not reply in a timely fashion
	timer := time.AfterFunc(srv.noCommitThreshold, srv.executeRecovery)

	heap.Push(&srv.uncommittedOperations, prepare)
	srv.executeUncommittedOperations()

	// Disable recovery timer if we managed to process all operations in the queue
	if srv.uncommittedOperations.Len() == 0 {
		timer.Stop()
	}
}

func (srv *PBServer) executeUncommittedOperations() {
	// If the next operation in the uncommitted ops queue is the next to be committed,
	// then commit the operations until you reach an operation that is out of order
	//
	// Otherwise, it waits for the next operations. If the next operation finally arrives,
	// we process it and all the following ones.
	//
	// In case the next operation does not arrive in a timely fashion, we consider
	// the message to be lost. In this case, the recovery timer will fire and execute
	// the recovery protocol.
	for srv.uncommittedOperations.Len() > 0 && srv.isNextOperation(srv.uncommittedOperations.Peek()) {
		message := heap.Pop(&srv.uncommittedOperations).(*Prepare)

		srv.opIndex = message.args.Index
		srv.log = append(srv.log, message.args.Entry)

		if message.args.PrimaryCommit > srv.commitIndex {
			srv.commitIndex = message.args.PrimaryCommit
			srv.timeLastCommit = time.Now()
		}

		message.reply.Success = srv.currentView == message.args.View && srv.opIndex >= message.args.Index
		message.reply.View = srv.currentView
		message.done <- true

		log.Printf("Replica %v - Prepared (view: %v op: %v commit: %v entry: %v)", srv.me, message.args.View, message.args.Index, message.args.PrimaryCommit, message.args.Entry)
	}
}

func (srv *PBServer) isNextOperation(prepare *Prepare) bool {
	return prepare.args.Index == (srv.opIndex + 1)
}

func (srv *PBServer) executeRecovery() {
	// Only go into recovery if it wasn't in recovery yet
	// Also, verify the time of the last commit update, since a timer might have been set
	// right after recovery finished
	if time.Since(srv.timeLastCommit).Nanoseconds() > srv.noCommitThreshold.Nanoseconds() {
		srv.backupRecover()
	}
}

// Transfer state from primary to backup.
// Change replica state to RECOVERING, send recovery requests, and update log,
// op index and commit index.
func (srv *PBServer) backupRecover() {
	if srv.status != NORMAL {
		return
	}

	log.Printf("Replica %v - Recovering (view: %v op: %v commit: %v)", srv.me, srv.currentView, srv.opIndex, srv.commitIndex)

	srv.status = RECOVERING

	done := make(chan bool)
	replies := make(chan *RecoveryReply, len(srv.peers))
	arguments := &RecoveryArgs{View: srv.currentView, Server: srv.me}

	// Send recovery requests to all peers. However, only the primary will reply
	for peer := range srv.peers {
		if peer != srv.me {
			go srv.backupSendRecovery(peer, arguments, replies)
		}
	}

	// Process recovery requests
	go srv.backupAwaitRecovery(arguments, replies, done)
	<-done

	srv.status = NORMAL
	srv.lastNormalView = srv.currentView

	// Reply to uncommitted operations that were recovered
	for srv.uncommittedOperations.Len() > 0 && srv.uncommittedOperations.Peek().args.Index <= srv.opIndex {
		message := heap.Pop(&srv.uncommittedOperations).(*Prepare)
		message.reply.Success = srv.currentView == message.args.View && srv.opIndex >= message.args.Index
		message.reply.View = srv.currentView
		message.done <- true
	}

	log.Printf("Replica %v - Recovered (view %v op: %v commit: %v)", srv.me, srv.currentView, srv.opIndex, srv.commitIndex)
}

func (srv *PBServer) backupSendRecovery(peer int, arguments *RecoveryArgs, replies chan *RecoveryReply) {
	reply := new(RecoveryReply)
	completed := srv.sendRecovery(peer, arguments, reply)

	if !completed {
		replies <- nil
	} else {
		replies <- reply
	}
}

// Update log, op index, commit index and server status on receiving one recovery
// reply
func (srv *PBServer) backupAwaitRecovery(arguments *RecoveryArgs, replies chan *RecoveryReply, done chan bool) {
	var reply *RecoveryReply

	for i := 1; reply == nil && i < len(srv.peers); i++ {
		reply = <-replies
	}

	if reply == nil {
		log.Printf("Replica %v - Did not received any recovery reply", srv.me)
		panic("Replica recovery failed")
	} else {
		for i := len(srv.log); i < len(reply.Entries); i++ {
			srv.log = append(srv.log, reply.Entries[i])
			srv.opIndex++
		}

		srv.currentView = reply.View
		srv.commitIndex = reply.PrimaryCommit
		srv.timeLastCommit = time.Now()
	}

	done <- true
}
