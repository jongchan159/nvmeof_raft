//go:build raft || raft_tcp
// +build raft raft_tcp

package nvmeof_raft

// ClientApplyRequest carries a batch of commands from a remote client node.
type ClientApplyRequest struct {
	Commands [][]byte
}

// ClientApplyResponse carries the error string if Apply failed.
type ClientApplyResponse struct {
	Err string
}

// HandleClientApply is the RPC entry point for a dedicated client machine.
// It forwards the command batch to the local Apply pipeline. Only the leader
// can process it — followers return ErrApplyToLeader via rsp.Err.
func (s *Server) HandleClientApply(req *ClientApplyRequest, rsp *ClientApplyResponse) error {
	_, err := s.Apply(req.Commands)
	if err != nil {
		rsp.Err = err.Error()
	}
	return nil
}
