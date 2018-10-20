package simplepb

import (
	"log"
	"time"
)

func (srv *PBServer) recover() {
	arguments, replies := srv.startRecovery()
	srv.awaitRecovery(arguments, replies)

	log.Printf("Node %v - Recovered (view %v op: %v commit: %v)", srv.me, srv.currentView, srv.opIndex, srv.commitIndex)
}

func (srv *PBServer) startRecovery() (arguments *RecoveryArgs, replies chan *RecoveryReply) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.status != NORMAL {
		log.Printf("Node %v - not in normal status (view: %v op: %v commit: %v, status: %d)", srv.me, srv.currentView, srv.opIndex, srv.commitIndex, srv.status)
		return
	}

	log.Printf("Node %v - Recovering (view: %v op: %v commit: %v)", srv.me, srv.currentView, srv.opIndex, srv.commitIndex)

	srv.status = RECOVERING

	arguments = &RecoveryArgs{View: srv.currentView, Server: srv.me}
	replies = make(chan *RecoveryReply, len(srv.peers))

	// Send recovery requests to all peers. However, only the primary will reply with success
	for peer := range srv.peers {
		if peer != srv.me {
			go srv.sendRecoveryToPeer(peer, arguments, replies)
		}
	}

	return arguments, replies
}

func (srv *PBServer) sendRecoveryToPeer(peer int, arguments *RecoveryArgs, replies chan *RecoveryReply) {
	reply := new(RecoveryReply)
	completed := srv.sendRecovery(peer, arguments, reply)

	if !completed {
		replies <- nil
	} else {
		replies <- reply
	}
}

// Update log, op index, commit index and server status on receiving one recovery reply
func (srv *PBServer) awaitRecovery(arguments *RecoveryArgs, replies chan *RecoveryReply) {
	var primary *RecoveryReply
	success := 0

	// index starts at 1 in order to skip the current server.
	for i := 1; i < len(srv.peers); i++ {
		reply := <-replies

		if reply != nil {
			success++

			if reply.Success {
				primary = reply
			}
		}
	}

	if success >= srv.replicationFactor() && primary != nil {
		srv.mu.Lock()
		defer srv.mu.Unlock()

		for i := len(srv.log); i < len(primary.Entries); i++ {
			srv.appendCommand(primary.Entries[i])
		}

		srv.currentView = primary.View
		srv.commitIndex = primary.PrimaryCommit
		srv.timeLastCommit = time.Now()
		srv.status = NORMAL
		srv.lastNormalView = srv.currentView

		log.Printf("Node %v - recovered with commit index %d and op index %d.\n", srv.me, srv.commitIndex, srv.opIndex)
	} else {
		log.Printf("Node %v - Did not received sufficient recovery replies (%d) or primary did not reply (%v)", srv.me, success, primary == nil)
	}
}
