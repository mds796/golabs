package simplepb

import (
	"container/heap"
	"log"
)

// Process prepare messages imn the backup. Enqueues messages received out of order.
// However, the RPC may still return a response without processing the operation.
// This case is alright since the master commits all previous operations if any future operations return true.
// So, once we have all in order operations the next operation will eventually return true.
func (srv *PBServer) backupPrepare(arguments *PrepareArgs, reply *PrepareReply) {
	log.Printf("Replica %v received operation %v in view %v from primary in view %v.", srv.me, arguments.Index, srv.currentView, arguments.View)

	// Add the prepared operation to the min-heap for uncommitted ops
	prepare := &Prepare{args: arguments, reply: reply, done: make(chan bool, 1)}

	go srv.backupPrepareInOrder(prepare)

	// wait until the requested operation is prepared
	<-prepare.done
	log.Printf("Replica %v prepared operation %v in view %v from primary in view %v.", srv.me, arguments.Index, srv.currentView, arguments.View)
}

// Prepares the next set of operatons that are in order from the current opIndex
// Does nothing if the next operation is not in the PrepareQueue.
// For example, if we had operations [2, 4, 5] and we just received a request to prepare #1,
// We would process ops 1 and 2, but not 4 and 5. Once we receive the message to prepare 3,
// only then would we prepare 3, 4, and 5 in order.
// Notice that this may cause the primary to timeout waiting for a response, but that's okay.
func (srv *PBServer) backupPrepareInOrder(prepare *Prepare) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	heap.Push(&srv.uncommittedOperations, prepare)

	// If the next operation in the uncommitted ops queue is the next to be committed,
	// then commit the operations until you reach an operation that is out of order
	for srv.uncommittedOperations.Len() > 0 && srv.isNextOperation(srv.uncommittedOperations.Peek()) {
		message := heap.Pop(&srv.uncommittedOperations).(*Prepare)

		if message.args.Index > srv.opIndex {
			srv.opIndex = message.args.Index
		}

		if message.args.PrimaryCommit > srv.commitIndex {
			srv.commitIndex = message.args.PrimaryCommit
		}

		message.reply.Success = srv.currentView == message.args.View && srv.opIndex >= message.args.Index
		message.reply.View = srv.currentView

		srv.log = append(srv.log, message.args.Entry)

		message.done <- true
	}
}

func (srv *PBServer) isNextOperation(message *Prepare) bool {
	return message.args.Index == (srv.opIndex + 1)
}
