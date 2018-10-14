package simplepb

import (
	"container/heap"
	"log"
)

func (srv *PBServer) primaryPrepare(arguments *PrepareArgs) {
	replies := make(chan *PrepareReply, len(srv.peers))

	for peer := range srv.peers {
		if peer != srv.me {
			go srv.primarySendPrepare(peer, arguments, replies)
		}
	}

	go srv.primaryAwaitPrepare(arguments, replies)
}

func (srv *PBServer) primarySendPrepare(peer int, arguments *PrepareArgs, replies chan *PrepareReply) {
	reply := new(PrepareReply)
	completed := srv.sendPrepare(peer, arguments, reply)

	if !completed || !reply.Success {
		log.Printf("Did not receive a reply from peer %v for prepare message %v", peer, *arguments)
		replies <- nil
	} else {
		replies <- reply
	}
}

// Awaits all prepare responses and timeouts.
// Then, counts the number of successful replies. If >= f replies were successful, appends the next operation to the log.
func (srv *PBServer) primaryAwaitPrepare(arguments *PrepareArgs, replies chan *PrepareReply) {
	defer close(replies)

	success := 0
	failure := 0

	// index starts at 1 in order to skip the primary.
	for i := 1; i < len(srv.peers); i++ {
		reply := <-replies
		if reply == nil {
			failure++
		} else {
			success++
		}
	}

	if success >= srv.replicationFactor() {
		srv.primaryAppendToLog(arguments)
	} else {
		log.Printf("Too many failed responses from the replicas, the primary is unable to serve the current operation %v.", srv.opIndex)
	}
}

// Append the command to the log
func (srv *PBServer) primaryAppendToLog(arguments *PrepareArgs) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// Add the prepared operation to the min-heap for uncommitted ops
	heap.Push(&srv.uncommittedOperations, arguments)

	// If the next operation in the uncommitted ops queue is the next to be committed,
	// then commit the operations until you reach an operation that is out of order
	for nextArgs := srv.uncommittedOperations.Peek(); srv.primaryIsNextOperation(nextArgs); nextArgs = srv.uncommittedOperations.Peek() {
		heap.Pop(&srv.uncommittedOperations)
		srv.log = append(srv.log, nextArgs.Entry)
		srv.commitIndex++
	}
}

func (srv *PBServer) primaryIsNextOperation(arguments *PrepareArgs) bool {
	return arguments.Index == (srv.commitIndex + 1)
}
