//go:build raft_old
// +build raft_old

package nvmeof_raft

import (
	"fmt"
	"nvmeof_raft/blockcopy"
	"path/filepath"
)

// NVMEOF_DEVICE_PATH is the NVMe-oF block device shared by all computing nodes.
// All nodes (eternity4/5/6) access the same physical storage through this path.
const NVMEOF_DEVICE_PATH = "/dev/nvme0n1"

// leaderPBAForRange resolves the physical block address for a contiguous range
// of log entries starting at logical index `next` for `count` entries.
//
// The leader calls this inside appendEntries() to fill LeaderPbaSrc in the RPC.
// Assumes 1-slot entries (cmdLen fits within one 512B slot), which holds for
// PBA metadata payloads (~64B). For larger payloads, slot tracking must be extended.
//
// Must be called with s.mu held.
func (s *Server) leaderPBAForRange(next, count uint64) (pbaSrc uint64, nbytes uint64, err error) {
	if count == 0 {
		return 0, 0, nil
	}

	// Compute on-disk slot number for logical index `next`.
	// delta: how many entries ahead of headLogIndex is `next`
	delta := next - s.headLogIndex
	slot := (s.headSlot + delta) % RING_SLOTS

	// File byte offset of that slot
	logicalOff := int64(RING_OFFSET) + int64(slot)*BLOCK_UNIT

	// Raw bytes = count entries * 1 slot each (1-slot entry assumption)
	// Round up to 4KB for O_DIRECT alignment
	rawBytes := count * uint64(BLOCK_UNIT)
	nbytes = blockcopy.AlignUp(rawBytes)

	// Resolve logical file offset -> physical block address via FIEMAP
	metaPath := filepath.Join(s.metadataDir, s.Metadata())
	seg, err := blockcopy.GetPBA(metaPath, logicalOff, nbytes)
	if err != nil {
		return 0, 0, fmt.Errorf("leaderPBAForRange(next=%d count=%d): %v", next, count, err)
	}

	return seg.PBA, nbytes, nil
}

// followerPBAForNext resolves the physical block address of the follower's
// metadata file at the position where it will receive the next entries (tailSlot).
//
// Must be called with s.mu held.
func (s *Server) followerPBAForNext(nbytes uint64) (pbaDst uint64, err error) {
	// Follower always writes to tailSlot (next available ring slot)
	logicalOff := int64(RING_OFFSET) + int64(s.tailSlot)*BLOCK_UNIT

	metaPath := filepath.Join(s.metadataDir, s.Metadata())
	seg, err := blockcopy.GetPBA(metaPath, logicalOff, nbytes)
	if err != nil {
		return 0, fmt.Errorf("followerPBAForNext(tailSlot=%d): %v", s.tailSlot, err)
	}

	return seg.PBA, nil
}

// doPBACopy performs the storage-level block copy on behalf of the follower.
//
// Flow:
//  1. Resolve follower's destination PBA (tailSlot position)
//  2. Call WritePBA(device, leaderPbaSrc, followerPbaDst, nbytes)
//  3. Storage node copies the block locally — no log data crosses the network
//
// Must be called with s.mu held.
// Returns nil on success; caller sets rsp.Success=false on error.
func (s *Server) doPBACopy(leaderPbaSrc, logBlockLength uint64) error {
	if leaderPbaSrc == 0 || logBlockLength == 0 {
		return nil // heartbeat: no new entries
	}

	// Convert 512B block count -> bytes, align to 4KB for O_DIRECT
	nbytes := blockcopy.AlignUp(logBlockLength * uint64(BLOCK_UNIT))

	// Resolve follower's destination PBA
	pbaDst, err := s.followerPBAForNext(nbytes)
	if err != nil {
		return fmt.Errorf("doPBACopy: %v", err)
	}

	// Perform storage-level copy via NVMe-oF device
	if err := blockcopy.WritePBA(NVMEOF_DEVICE_PATH, leaderPbaSrc, pbaDst, nbytes); err != nil {
		return fmt.Errorf("doPBACopy: WritePBA(src=0x%X dst=0x%X nbytes=%d): %v",
			leaderPbaSrc, pbaDst, nbytes, err)
	}

	s.debugf("[PBA COPY] src=0x%X dst=0x%X nbytes=%d blocks=%d",
		leaderPbaSrc, pbaDst, nbytes, logBlockLength)
	return nil
}
