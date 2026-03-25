//go:build raft || raft_tcp
// +build raft raft_tcp

package nvmeof_raft

import "sync"

// ClientApplyRequest carries a batch of commands from a remote client node.
type ClientApplyRequest struct {
	Commands [][]byte
}

// ClientApplyResponse carries the error string if Apply failed.
type ClientApplyResponse struct {
	Err string
}

// applySerial ensures only one HandleClientApply call drives Apply() at a time.
// Concurrent appendEntries() goroutines all use PrevLogIndex=0 and truncate each
// other's work on the follower, causing log corruption and a permanent stall.
// Serializing here keeps a single appendEntries() flow in flight per Apply().
var applySerial sync.Mutex

// HandleClientApply is the RPC entry point for a dedicated client machine.
// It forwards the command batch to the local Apply pipeline. Only the leader
// can process it — followers return ErrApplyToLeader via rsp.Err.
func (s *Server) HandleClientApply(req *ClientApplyRequest, rsp *ClientApplyResponse) error {
	applySerial.Lock()
	defer applySerial.Unlock()
	_, err := s.Apply(req.Commands)
	if err != nil {
		rsp.Err = err.Error()
	}
	return nil
}
