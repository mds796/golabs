package simplepb

import (
	"container/heap"
	"log"
)

// Process prepare messages in the backup, doing state transfer if necessary.
// Enqueues messages received out of order to process them in order.
// However, the RPC may still return a response without processing the operation.
// This case is alright since the master commits all previous operations if any future operations return true.
// So, once we have all in order operations the next operation will eventually return true.
func (srv *PBServer) backupPrepare(arguments *PrepareArgs, reply *PrepareReply) {
	log.Printf("Replica %v - [view %v commit %v op %v] - Received prepare (view: %v op: %v commit: %v entry: %v)", srv.me, srv.currentView, srv.commitIndex, srv.opIndex, srv.currentView, arguments.Index, arguments.PrimaryCommit, arguments.Entry)

	// Add the prepared operation to the min-heap for uncommitted ops
	prepare := &Prepare{args: arguments, reply: reply, done: make(chan bool, 1)}

	if srv.isOutOfSync(prepare) {
		recovery := make(chan bool)
		go srv.backupRecover(prepare, recovery)
		<-recovery
	} else {
		go srv.backupPrepareInOrder(prepare)
		<-prepare.done
	}
}

func (srv *PBServer) isOutOfSync(prepare *Prepare) bool {
	return prepare.args.PrimaryCommit > srv.opIndex
}

// Transfer state from primary to backup.
// Change replica state to RECOVERING, send recovery requests, and update log,
// op index and commit index.
func (srv *PBServer) backupRecover(prepare *Prepare, recovery chan bool) {
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

	go srv.backupAwaitRecovery(arguments, replies, done)
	<-done

	log.Printf("Replica %v - Recovered (view %v op: %v commit: %v)", srv.me, srv.currentView, srv.opIndex, srv.commitIndex)
	recovery<- true
}

func (srv *PBServer) backupSendRecovery(peer int, arguments *RecoveryArgs, replies chan *RecoveryReply) {
	reply := new(RecoveryReply)
	completed := srv.sendRecovery(peer, arguments, reply)

	if !completed || !reply.Success {
		replies<- nil
	} else {
		replies<- reply
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

		srv.commitIndex = reply.PrimaryCommit
		srv.status = NORMAL
	}

	done<- true
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
	// heap.Push(&srv.uncommittedOperations, prepare)

	if prepare.args.Index > srv.opIndex && prepare.args.PrimaryCommit >= srv.commitIndex {
		heap.Push(&srv.uncommittedOperations, prepare)
	} else {
		log.Printf("Replica %v - Ignoring prepare (view: %v op: %v commit: %v entry: %v)", srv.me, prepare.args.View, prepare.args.Index, prepare.args.PrimaryCommit, prepare.args.Entry)
	}

	// If the next operation in the uncommitted ops queue is the next to be committed,
	// then commit the operations until you reach an operation that is out of order
	for srv.uncommittedOperations.Len() > 0 && srv.isNextOperation(srv.uncommittedOperations.Peek()) {
		enqueuedPrepare := heap.Pop(&srv.uncommittedOperations).(*Prepare)

		if enqueuedPrepare.args.Index <= srv.opIndex || enqueuedPrepare.args.PrimaryCommit < srv.commitIndex {
			continue
		}

		if enqueuedPrepare.args.Index > srv.opIndex {
			srv.opIndex = enqueuedPrepare.args.Index
		}

		if enqueuedPrepare.args.PrimaryCommit > srv.commitIndex {
			srv.commitIndex = enqueuedPrepare.args.PrimaryCommit
		}

		enqueuedPrepare.reply.Success = srv.currentView == enqueuedPrepare.args.View && srv.opIndex >= enqueuedPrepare.args.Index
		enqueuedPrepare.reply.View = srv.currentView

		srv.log = append(srv.log, enqueuedPrepare.args.Entry)

		enqueuedPrepare.done <- true

		log.Printf("Replica %v - Prepared (view: %v op: %v commit: %v entry: %v)", srv.me, enqueuedPrepare.args.View, enqueuedPrepare.args.Index, enqueuedPrepare.args.PrimaryCommit, enqueuedPrepare.args.Entry)
	}
}

func (srv *PBServer) isNextOperation(prepare *Prepare) bool {
	return prepare.args.Index == (srv.opIndex + 1)
}
