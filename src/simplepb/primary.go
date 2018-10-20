package simplepb

import (
	"log"
)

func (srv *PBServer) isNormalPrimary() bool {
	return srv.status == NORMAL && srv.IsPrimary()
}

func (srv *PBServer) primarySendPrepare(peer int, arguments *PrepareArgs, replies chan *PrepareReply) {
	reply := new(PrepareReply)
	completed := srv.sendPrepare(peer, arguments, reply)

	if !completed {
		log.Printf("Primary %v - Did not receive prepare reply from replica %v (view: %v op: %v commit: %v entry: %v)", srv.me, peer, srv.currentView, arguments.Index, srv.commitIndex, arguments.Entry)
		replies <- nil
	} else {
		replies <- reply
	}
}

// Awaits all prepare responses and timeouts.
// Then, counts the number of successful replies. If >= f replies were successful, appends the next operation to the log.
func (srv *PBServer) primaryAwaitPrepare(arguments *PrepareArgs, replies chan *PrepareReply) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

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

		if reply != nil && reply.View > srv.currentView {
			log.Printf("Primary %v - received prepare reply with larger view %d (view: %v op: %v commit: %v entry: %v, status: %d)", srv.me, reply.View, srv.currentView, arguments.Index, srv.commitIndex, arguments.Entry, srv.status)
			go srv.recover()
			return
		}
	}

	if success >= majority {
		if srv.commitIndex < arguments.Index {
			log.Printf("Primary %v - Updating commit %v -> %v", srv.me, srv.commitIndex, arguments.Index)

			srv.commitIndex = arguments.Index
		}
	} else {
		log.Printf("Primary %v - Failed serving operation %v", srv.me, arguments.Index)
	}
}
