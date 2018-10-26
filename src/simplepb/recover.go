package simplepb

import (
	"log"
)

// StartRecovery sends Recover RPC messages to all peers.
func (srv *PBServer) StartRecovery() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.status != NORMAL {
		log.Printf("Node %v - not in normal status (view: %v op: %v commit: %v, status: %d)", srv.me, srv.currentView, srv.opIndex, srv.commitIndex, srv.status)
		return
	}

	log.Printf("Node %v - Recovering (view: %v op: %v commit: %v)", srv.me, srv.currentView, srv.opIndex, srv.commitIndex)

	srv.status = RECOVERING

	args := &RecoveryArgs{View: srv.currentView, Server: srv.me}

	// Send recovery requests to all peers.
	for peer := range srv.peers {
		if peer != srv.me {
			go srv.RecoverFromPeer(peer, args)
		}
	}
}

// Update log, op index, commit index and server status on receiving one recovery reply
func (srv *PBServer) RecoverFromPeer(peer int, args *RecoveryArgs) {
	reply := new(RecoveryReply)
	ok := srv.sendRecovery(peer, args, reply)

	srv.mu.Lock()
	defer srv.mu.Unlock()

	success := ok && reply.Success && srv.status == RECOVERING

	if success && reply.View >= srv.currentView {
		log.Printf("Node %v - will recover with commit index %d and op index %d and log %v.\n", srv.me, srv.commitIndex, srv.opIndex, srv.log)

		if reply.View == srv.currentView {
			for i := len(srv.log); i < len(reply.Entries); i++ {
				srv.opIndex++
				srv.log = append(srv.log, reply.Entries[i])
			}
		} else {
			srv.log = reply.Entries
		}

		srv.status = NORMAL
		srv.opIndex = len(reply.Entries) - 1
		srv.commitIndex = reply.PrimaryCommit
		srv.currentView = reply.View
		srv.lastNormalView = reply.View

		log.Printf("Node %v - recovered with commit index %d and op index %d and log %v.\n", srv.me, srv.commitIndex, srv.opIndex, srv.log)

		go srv.prepareUncommittedOperations()
	}
}
