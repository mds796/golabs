package simplepb

import (
	"log"
)

func (srv *PBServer) primaryPrepare(arguments *PrepareArgs) {
	log.Printf("Primary %v - Preparing (view: %v op: %v commit: %v entry: %v)", srv.me, srv.currentView, arguments.Index, srv.commitIndex, arguments.Entry)

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
		log.Printf("Primary %v - Did not received prepare reply from replica %v (view: %v op: %v commit: %v entry: %v)", srv.me, peer, srv.currentView, arguments.Index, srv.commitIndex, arguments.Entry)
		replies <- nil
	} else {
		replies <- reply
	}
}

// Awaits all prepare responses and timeouts.
// Then, counts the number of successful replies. If >= f replies were successful, appends the next operation to the log.
func (srv *PBServer) primaryAwaitPrepare(arguments *PrepareArgs, replies chan *PrepareReply) {
	success := 0
	failure := 0

	// index starts at 1 in order to skip the primary.
	// stops immediately after f successes
	for i := 1; success < srv.replicationFactor() && i < len(srv.peers); i++ {
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
			log.Printf("Primary %v - Updating commit %v -> %v", srv.me, srv.commitIndex, arguments.Index)
			srv.commitIndex = arguments.Index
		}
	} else {
		log.Printf("Primary %v - Failed serving operation %v", srv.me, arguments.Index)
	}
}

func (srv *PBServer) primaryRecovery(arguments *RecoveryArgs, reply *RecoveryReply) {
	reply.View = srv.currentView
	reply.Entries = srv.log
	reply.PrimaryCommit = srv.commitIndex
	reply.Success = true
}