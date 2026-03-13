//go:build raft
// +build raft

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
// of ring buffer slots starting at logical index `next`.
//
// The leader calls this inside appendEntries() to fill LeaderPbaSrc in the RPC.
// The caller pre-computes totalSlots (sum of slotsForEntry for each entry)
// and ensures the range does not wrap around the ring buffer.
//
// Must be called with s.mu held.
func (s *Server) leaderPBAForRange(next, totalSlots uint64) (pbaSrc uint64, nbytes uint64, err error) {
	if totalSlots == 0 {
		return 0, 0, nil
	}

	// Compute on-disk slot number for logical index `next`.
	// delta: how many slots ahead of headSlot is the start of `next`
	// We walk from headLogIndex to `next` to get exact slot offset.
	slot := s.headSlot
	for idx := s.headLogIndex; idx < next; idx++ {
		e := s.log[s.logSlice(idx)]
		needed := slotsForEntry(len(e.Command))
		slot = (slot + needed) % RING_SLOTS
	}

	// File byte offset of that slot
	logicalOff := int64(RING_OFFSET) + int64(slot)*BLOCK_UNIT

	// Raw bytes = totalSlots * BLOCK_UNIT, aligned to 4KB for O_DIRECT
	rawBytes := totalSlots * uint64(BLOCK_UNIT)
	nbytes = blockcopy.AlignUp(rawBytes)

	// Resolve logical file offset -> physical block address via FIEMAP
	metaPath := filepath.Join(s.metadataDir, s.Metadata())
	seg, err := blockcopy.L_get_pba(metaPath, logicalOff, nbytes)
	if err != nil {
		return 0, 0, fmt.Errorf("leaderPBAForRange(next=%d totalSlots=%d): %v", next, totalSlots, err)
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
	seg, err := blockcopy.L_get_pba(metaPath, logicalOff, nbytes)
	if err != nil {
		return 0, fmt.Errorf("followerPBAForNext(tailSlot=%d): %v", s.tailSlot, err)
	}

	return seg.PBA, nil
}

// doPBACopy performs the storage-level block copy on behalf of the follower.
//
// Flow:
//  1. Resolve follower's destination PBA (tailSlot position)
//  2. Call R_write_pba(device, leaderPbaSrc, followerPbaDst, nbytes)
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
	if err := blockcopy.R_write_pba(NVMEOF_DEVICE_PATH, leaderPbaSrc, pbaDst, nbytes); err != nil {
		return fmt.Errorf("doPBACopy: R_write_pba(src=0x%X dst=0x%X nbytes=%d): %v",
			leaderPbaSrc, pbaDst, nbytes, err)
	}

	s.debugf("[PBA COPY] src=0x%X dst=0x%X nbytes=%d blocks=%d",
		leaderPbaSrc, pbaDst, nbytes, logBlockLength)
	return nil
}
