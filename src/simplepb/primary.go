package simplepb

import (
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
		srv.mu.Lock()
		defer srv.mu.Unlock()

		if srv.commitIndex < arguments.Index {
			srv.commitIndex = arguments.Index
		}
	} else {
		log.Printf("Too many failed responses from the replicas, the primary is unable to serve the current operation %v.", srv.opIndex)
	}
}
