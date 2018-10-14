package simplepb

import (
	"container/heap"
	"log"
)

// Process prepare messages imn the backup. Enqueues messages received out of order.
// However, the RPC may still return a response without processing the operation.
// This case is alright since the master commits all previous operations if any future operations return true.
// So, once we have all in order operations the next operation will eventually return true.
// We can do better, but it would require locks until the operation is processed and may time out in the primary anyways.
func (srv *PBServer) backupPrepare(arguments *PrepareArgs, reply *PrepareReply) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if arguments.Index > srv.commitIndex {
		// Add the prepared operation to the min-heap for uncommitted ops
		heap.Push(&srv.uncommittedOperations, &Prepare{args: arguments, reply: reply})

		// If the next operation in the uncommitted ops queue is the next to be committed,
		// then commit the operations until you reach an operation that is out of order
		for srv.uncommittedOperations.Len() > 0 && srv.isNextOperation(srv.uncommittedOperations.Peek()) {
			srv.opIndex++

			message := heap.Pop(&srv.uncommittedOperations).(*Prepare)
			message.reply.View = srv.currentView
			message.reply.Success = srv.currentView == arguments.View && srv.opIndex >= message.args.Index

			log.Printf("Replica %v prepared operation %v in view %v from primary in view %v.", srv.me, message.args.Index, srv.currentView, message.args.View)

			srv.log = append(srv.log, message.args.Entry)
			if message.args.PrimaryCommit > srv.commitIndex {
				srv.commitIndex = message.args.PrimaryCommit
			}
		}
	} else {
		log.Printf("Replica %v skipped operation %v.", srv.me, arguments.Index)
	}
}

func (srv *PBServer) isNextOperation(message *Prepare) bool {
	return message.args.Index == (srv.opIndex + 1)
}
