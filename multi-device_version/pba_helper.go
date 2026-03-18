//go:build raft
// +build raft

package nvmeof_raft

import (
	"encoding/binary"
	"fmt"
	"nvmeof_raft/blockcopy"
	"path/filepath"
)

// NVMEOF_DEVICE_PATH is the NVMe-oF block device shared by all computing nodes.
// All nodes (eternity4/5/6) access the same physical storage through this path.
//const NVMEOF_DEVICE_PATH = "/dev/nvme0n1"

// leaderPBAForRange resolves the physical block address for a contiguous range
// of ring buffer slots starting at `startSlot` for `totalSlots` slots.
//
// The caller (appendEntries) pre-computes startSlot from logSlotMap and
// ensures the range does not wrap around the ring buffer boundary.
//
// Must be called with s.mu held.
func (s *Server) leaderPBAForRange(startSlot, totalSlots uint64) (pbaSrc uint64, nbytes uint64, err error) {
	if totalSlots == 0 {
		return 0, 0, nil
	}

	// File byte offset of the start slot
	logicalOff := int64(RING_OFFSET) + int64(startSlot)*BLOCK_UNIT

	// Raw bytes = totalSlots * BLOCK_UNIT, aligned to 4KB for O_DIRECT
	rawBytes := totalSlots * uint64(BLOCK_UNIT)
	nbytes = blockcopy.AlignUp(rawBytes)

	// Resolve logical file offset -> physical block address via FIEMAP
	metaPath := filepath.Join(s.metadataDir, s.Metadata())
	seg, err := blockcopy.L_get_pba(metaPath, logicalOff, nbytes)
	if err != nil {
		return 0, 0, fmt.Errorf("leaderPBAForRange(startSlot=%d totalSlots=%d): %v",
			startSlot, totalSlots, err)
	}

	return seg.PBA + s.partitionOffsetBytes, nbytes, nil
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

	return seg.PBA + s.partitionOffsetBytes, nil
}

// doPBACopy performs the storage-level block copy on behalf of the follower.
// Uses follower-pull: follower reads from the leader's device and writes to its own.
// This allows leader and follower to reside on separate physical NVMe devices.
//
// Flow:
//  1. Resolve follower's destination PBA (tailSlot position)
//  2. Call R_write_pba(leaderDevicePath, myDevicePath, pbaSrc, pbaDst, nbytes)
//     - pread from leader's device at pbaSrc
//     - pwrite to follower's device at pbaDst
//  3. No log data crosses the network
//
// Must be called with s.mu held.
// Returns nil on success; caller sets rsp.Success=false on error.
func (s *Server) doPBACopy(leaderDevicePath string, leaderPbaSrc, logBlockLength uint64) error {
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

	// Follower pulls: read from leader's device, write to own device
	if err := blockcopy.R_write_pba(leaderDevicePath, s.devicePath, leaderPbaSrc, pbaDst, nbytes); err != nil {
		return fmt.Errorf("doPBACopy: R_write_pba(src=0x%X dst=0x%X nbytes=%d): %v",
			leaderPbaSrc, pbaDst, nbytes, err)
	}

	s.debugf("[PBA COPY] leaderDev=%s myDev=%s src=0x%X dst=0x%X nbytes=%d blocks=%d",
		leaderDevicePath, s.devicePath, leaderPbaSrc, pbaDst, nbytes, logBlockLength)
	return nil
}

// readEntryDirect reads one entry from the device using O_DIRECT,
// bypassing page cache entirely. Used after doPBACopy on follower.
func (s *Server) readEntryDirect(headerSlot uint64) (Entry, uint64) {
	metaPath := filepath.Join(s.metadataDir, s.Metadata())

	logicalOff := int64(RING_OFFSET) + int64(headerSlot)*BLOCK_UNIT
	seg, err := blockcopy.L_get_pba(metaPath, logicalOff, uint64(BLOCK_UNIT))
	if err != nil {
		panic(fmt.Sprintf("readEntryDirect: L_get_pba slot %d: %v", headerSlot, err))
	}
	rawPBA := seg.PBA + s.partitionOffsetBytes

	// Align PBA down to 4KB boundary for O_DIRECT
	alignedPBA := rawPBA &^ (PAGE_SIZE - 1)    // round down to 4KB
	offsetInPage := int64(rawPBA - alignedPBA) // where our data starts within the 4KB block

	buf := blockcopy.DirectRead(s.devicePath, alignedPBA, PAGE_SIZE)

	// Parse header at the correct offset within the 4KB block
	header := buf[offsetInPage : offsetInPage+BLOCK_UNIT]
	term := binary.LittleEndian.Uint64(header[0:])
	cmdLen := binary.LittleEndian.Uint64(header[8:])
	numSlots := binary.LittleEndian.Uint64(header[16:])

	cmd := make([]byte, cmdLen)
	if cmdLen > 0 {
		copied := 0
		for i := uint64(1); i < numSlots; i++ {
			pSlot := (headerSlot + i) % RING_SLOTS
			pOff := int64(RING_OFFSET) + int64(pSlot)*BLOCK_UNIT
			pSeg, err := blockcopy.L_get_pba(metaPath, pOff, uint64(BLOCK_UNIT))
			if err != nil {
				panic(fmt.Sprintf("readEntryDirect: payload slot %d: %v", pSlot, err))
			}
			pRawPBA := pSeg.PBA + s.partitionOffsetBytes
			pAlignedPBA := pRawPBA &^ (PAGE_SIZE - 1)
			pOffInPage := int64(pRawPBA - pAlignedPBA)

			pBuf := blockcopy.DirectRead(s.devicePath, pAlignedPBA, PAGE_SIZE)
			copied += copy(cmd[copied:], pBuf[pOffInPage:pOffInPage+BLOCK_UNIT])
		}
	}

	return Entry{Term: term, Command: cmd}, (headerSlot + numSlots) % RING_SLOTS
}