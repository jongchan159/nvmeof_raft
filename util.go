//go:build raft
// +build raft

package nvmeof_raft

import "context"

func (s *Server) Id() uint64 {
	return s.id
}

func (s *Server) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == leaderState
}

func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.debug("Shutting down.")
	if s.fd != nil {
		s.fd.Close()
		s.fd = nil
	}
	if s.server != nil {
		s.server.Shutdown(context.Background())
	}
	s.done = true
}

// AllCommitted returns true when all committed entries have been applied
// to the state machine, plus a completion percentage.
func (s *Server) AllCommitted() (bool, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commitIndex == 0 {
		return true, 100
	}
	done := s.lastApplied >= s.commitIndex
	pct := float64(s.lastApplied) / float64(s.commitIndex) * 100
	return done, pct
}
