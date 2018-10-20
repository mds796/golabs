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
	srv.prepareReply(arguments, reply)

	if reply.Success {
		// Add the prepared operation to the min-heap for uncommitted ops
		prepare := &Prepare{args: arguments, reply: reply, done: make(chan bool, 1)}
		go srv.backupPrepareInOrder(prepare)
		<-prepare.done
	}
}

func (srv *PBServer) prepareReply(arguments *PrepareArgs, reply *PrepareReply) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	log.Printf("Node %v - [view %v commit %v op %v] - Received prepare (view: %v op: %v commit: %v entry: %v)", srv.me, srv.currentView, srv.commitIndex, srv.opIndex, srv.currentView, arguments.Index, arguments.PrimaryCommit, arguments.Entry)

	reply.View = srv.currentView
	reply.Success = srv.status == NORMAL && !srv.IsPrimary() && arguments.View >= srv.currentView
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

	timer := time.AfterFunc(srv.noCommitThreshold, func() { srv.backupRecover(prepare) })

	// Only go into recovery if it wasn't in recovery yet
	// Also, verify the time of the last commit update, since a timer might have been set
	// right after recovery finished
	if prepare.args.View > srv.currentView {
		go srv.backupRecover(prepare)
		return
	}

	log.Printf("Replica %v - Preparing (view: %v op: %v commit: %v entry: %v)", srv.me, prepare.args.View, prepare.args.Index, prepare.args.PrimaryCommit, prepare.args.Entry)

	heap.Push(&srv.uncommittedOperations, prepare)
	srv.executeUncommittedOperations()

	// Disable recovery timer if we managed to process all operations in the queue
	if srv.uncommittedOperations.Len() == 0 {
		timer.Stop()
	}
}

func (srv *PBServer) backupRecover(prepare *Prepare) {
	if prepare.args.View <= srv.currentView && time.Since(srv.timeLastCommit).Nanoseconds() < srv.noCommitThreshold.Nanoseconds() {
		return
	}

	srv.recover()
	srv.backupPrepareInOrder(prepare)
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

		if message.args.Index > srv.opIndex {
			srv.opIndex = message.args.Index
			srv.log = append(srv.log, message.args.Entry)
		}

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
	return prepare.args.Index <= (srv.opIndex + 1)
}
