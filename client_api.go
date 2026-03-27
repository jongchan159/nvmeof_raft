//go:build raft || raft_tcp
// +build raft raft_tcp

package nvmeof_raft

import (
	"sync"
	"time"
)

// ClientApplyRequest carries a batch of commands from a remote client node.
type ClientApplyRequest struct {
	Commands [][]byte
}

// ClientApplyResponse carries the error string if Apply failed.
type ClientApplyResponse struct {
	Err string
}

type pendingApply struct {
	commands [][]byte
	result   chan batchResult
}

type batchResult struct {
	results []ApplyResult
	err     error
}

var (
	applyQueue     chan *pendingApply
	applyQueueOnce sync.Once
)

// collectWindow is how long the batcher waits after the first request
// arrives before closing the collection and calling Apply. This gives
// concurrent client threads time to finish generating their next payload
// and submit, so they get batched into the same Raft round.
// maxBatchBytes caps the total payload per Raft round; once exceeded the
// batcher closes early so excess threads queue for the next round instead
// of inflating the batch size unboundedly (which would increase per-round
// latency faster than batch size, causing throughput to fall with more threads).
const (
	collectWindow = 500 * time.Microsecond
	maxBatchBytes = 512 * 1024 // 512 KB
)

// initApplyBatcher starts the single goroutine that owns all Apply() calls.
// Concurrent HandleClientApply callers submit to applyQueue; the batcher
// waits collectWindow for concurrent callers to accumulate (up to maxBatchBytes),
// merges commands into one Apply() call, and distributes results back.
func (s *Server) initApplyBatcher() {
	applyQueue = make(chan *pendingApply, 1024)
	go func() {
		for {
			// Wait for the first caller.
			first := <-applyQueue
			batch := []*pendingApply{first}
			cmds := append([][]byte{}, first.commands...)
			batchBytes := 0
			for _, c := range first.commands {
				batchBytes += len(c)
			}

			// Wait collectWindow for concurrent callers to arrive, up to maxBatchBytes.
			timer := time.NewTimer(collectWindow)
		collect:
			for {
				select {
				case p := <-applyQueue:
					extra := 0
					for _, c := range p.commands {
						extra += len(c)
					}
					batch = append(batch, p)
					cmds = append(cmds, p.commands...)
					batchBytes += extra
					if batchBytes >= maxBatchBytes {
						timer.Stop()
						break collect
					}
				case <-timer.C:
					break collect
				}
			}
			timer.Stop()

			results, err := s.Apply(cmds)

			// Distribute results back to each caller.
			offset := 0
			for _, p := range batch {
				n := len(p.commands)
				if err != nil {
					p.result <- batchResult{err: err}
				} else {
					p.result <- batchResult{results: results[offset : offset+n]}
				}
				offset += n
			}
		}
	}()
}

// HandleClientApply is the RPC entry point for a dedicated client machine.
// It forwards the command batch to the local Apply pipeline. Only the leader
// can process it — followers return ErrApplyToLeader via rsp.Err.
func (s *Server) HandleClientApply(req *ClientApplyRequest, rsp *ClientApplyResponse) error {
	applyQueueOnce.Do(s.initApplyBatcher)
	p := &pendingApply{
		commands: req.Commands,
		result:   make(chan batchResult, 1),
	}
	applyQueue <- p
	res := <-p.result
	if res.err != nil {
		rsp.Err = res.err.Error()
	}
	return nil
}
