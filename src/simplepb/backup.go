package simplepb

import (
	"container/heap"
	"context"
	"log"
)

// Process prepare messages in the backup, doing state transfer if necessary.
// Enqueues messages received out of order to process them in order.
// However, the RPC may still return a response without processing the operation.
// This case is alright since the master commits all previous operations if any future operations return true.
// So, once we have all in order operations the next operation will eventually return true.
func (srv *PBServer) backupPrepare(args *PrepareArgs, reply *PrepareReply) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	log.Printf("Node %v - [view %v commit %v op %v] - Received prepare (view: %v op: %v commit: %v entry: %v)", srv.me, srv.currentView, srv.commitIndex, srv.opIndex, srv.currentView, args.Index, args.PrimaryCommit, args.Entry)

	isPrimary := GetPrimary(Max(args.View, srv.currentView), len(srv.peers)) == srv.me
	statusNormal := srv.status == NORMAL
	isHealthy := statusNormal && !isPrimary
	sameView := args.View == srv.currentView

	reply.View = srv.currentView

	prepare := &Prepare{args: args, reply: reply, done: make(chan bool, 1)}
	heap.Push(&srv.operationsToPrepare, prepare)

	if isHealthy && sameView {
		srv.prepareUncommittedOperations()

		reply.Success = srv.opIndex >= args.Index

		if !reply.Success {
			srv.awaitCompletionOrTimeout(prepare)
		}
	} else if args.View > srv.currentView {
		go srv.StartRecovery()
	}
}

func (srv *PBServer) prepareUncommittedOperations() {
	for srv.hasNextOperation() {
		next := heap.Pop(&srv.operationsToPrepare).(*Prepare)
		next.reply.Success = true
		next.done <- true

		srv.commitIndex = Max(srv.commitIndex, next.args.PrimaryCommit)

		if next.args.Index == srv.opIndex+1 {
			srv.opIndex++
			srv.log = append(srv.log, next.args.Entry)

			log.Printf("Replica %v - Prepared (view: %v op: %v commit: %v entry: %v)", srv.me, next.args.View, next.args.Index, next.args.PrimaryCommit, next.args.Entry)
		}
	}
}

// awaitCompletionOrTimeout waits for the prepare to complete or timeout
func (srv *PBServer) awaitCompletionOrTimeout(prepare *Prepare) {
	srv.mu.Unlock()
	defer srv.mu.Lock() // needed to avoid panic in caller function (defer unlock in caller)

	timeout, _ := context.WithTimeout(context.Background(), srv.prepareTimeout)

	select {
	case <-prepare.done:
		return // do nothing
	case <-timeout.Done():
		go srv.StartRecovery()
	}
}

func (srv *PBServer) hasNextOperation() bool {
	if srv.operationsToPrepare.Len() == 0 {
		return false
	}

	args := srv.operationsToPrepare.Peek().args

	sameView := args.View == srv.currentView
	nextOperation := args.Index <= srv.opIndex+1

	return sameView && nextOperation
}

func (srv *PBServer) prepareReply(arguments *PrepareArgs, reply *PrepareReply) {

	reply.View = srv.currentView
	reply.Success = srv.status == NORMAL && !srv.IsPrimary() && arguments.View >= srv.currentView

	if arguments.PrimaryCommit > srv.commitIndex {
		srv.mu.Lock()
		defer srv.mu.Unlock()

		srv.commitIndex = arguments.PrimaryCommit
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
	for srv.operationsToPrepare.Len() > 0 && srv.isNextOperation(srv.operationsToPrepare.Peek()) {
		message := heap.Pop(&srv.operationsToPrepare).(*Prepare)

		if message.args.Index > srv.opIndex {
			srv.opIndex = message.args.Index
			srv.log = append(srv.log, message.args.Entry)
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
