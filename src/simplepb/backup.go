package simplepb

import "container/heap"

func (srv *PBServer) backupPrepare() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// Add the prepared operation to the min-heap for uncommitted ops
	heap.Push(&srv.uncommittedOperations, arguments)

	// If the next operation in the uncommitted ops queue is the next to be committed,
	// then commit the operations until you reach an operation that is out of order
	for nextArgs := srv.uncommittedOperations.Peek(); srv.isNextOperation(nextArgs); nextArgs = srv.uncommittedOperations.Peek() {
		heap.Pop(&srv.uncommittedOperations)
		srv.commitIndex++
	}
}

func (srv *PBServer) isNextOperation(arguments *PrepareArgs) bool {
	return arguments.Index == (srv.commitIndex + 1)
}
