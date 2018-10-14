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
	srv.mu.Lock()

	log.Printf("Replica %v received operation %v in view %v from primary in view %v.", srv.me, arguments.Index, srv.currentView, arguments.View)

	// Add the prepared operation to the min-heap for uncommitted ops
	prepare := &Prepare{args: arguments, reply: reply, done: make(chan bool, 1)}
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

		message.reply.Success = srv.currentView == arguments.View && srv.opIndex >= message.args.Index
		message.reply.View = srv.currentView

		srv.log = append(srv.log, message.args.Entry)

		message.done <- true
	}

	srv.mu.Unlock()
	<-prepare.done
	log.Printf("Replica %v prepared operation %v in view %v from primary in view %v.", srv.me, arguments.Index, srv.currentView, arguments.View)
}

func (srv *PBServer) isNextOperation(message *Prepare) bool {
	return message.args.Index == (srv.opIndex + 1)
}
