package simplepb

import (
	"log"
)

func (srv *PBServer) isNormalPrimary() bool {
	return srv.status == NORMAL && srv.IsPrimary()
}

func (srv *PBServer) primaryPrepare(opIndex int) {
	// Normally, we would update the client table with the new request number before sending Prepare messages.
	arguments := &PrepareArgs{View: srv.currentView, PrimaryCommit: srv.commitIndex, Index: opIndex, Entry: srv.log[opIndex]}

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

	if !completed {
		replies <- nil
	} else {
		replies <- reply
	}
}

// Awaits all prepare responses and timeouts.
// Then, counts the number of successful replies. If >= f replies were successful, appends the next operation to the log.
func (srv *PBServer) primaryAwaitPrepare(arguments *PrepareArgs, replies chan *PrepareReply) {
	majority := srv.replicationFactor()
	success := 0
	failure := 0

	// index starts at 1 in order to skip the primary.
	// stops immediately after f successes
	for i := 1; success < majority && i < len(srv.peers); i++ {
		reply := <-replies

		if reply == nil || !reply.Success {
			failure++
		} else {
			success++
		}
	}

	if success >= majority {
		srv.mu.Lock()
		defer srv.mu.Unlock()

		if srv.commitIndex < arguments.Index && srv.isNormalPrimary() {
			log.Printf("Primary %v - Updating commit %v -> %v", srv.me, srv.commitIndex, arguments.Index)
			srv.commitIndex = arguments.Index
		}
	} else if !srv.isNormalPrimary() {
		log.Printf("Primary %v - Failed serving operation %v", srv.me, arguments.Index)
	}
}
