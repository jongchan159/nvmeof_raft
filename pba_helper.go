//go:build raft
// +build raft

package nvmeof_raft

import (
	"encoding/binary"
	"fmt"
	"nvmeof_raft/blockcopy"
	"path/filepath"
	"syscall"
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
	if err := blockcopy.R_write_pba(s.devicePath, leaderPbaSrc, pbaDst, nbytes); err != nil {
		return fmt.Errorf("doPBACopy: R_write_pba(src=0x%X dst=0x%X nbytes=%d): %v",
			leaderPbaSrc, pbaDst, nbytes, err)
	}

	s.debugf("[PBA COPY] src=0x%X dst=0x%X nbytes=%d blocks=%d",
		leaderPbaSrc, pbaDst, nbytes, logBlockLength)
	return nil
}

func (s *Server) invalidateCache(offset int64, length int64) {
	// POSIX_FADV_DONTNEED: 해당 영역의 페이지 캐시를 drop
	syscall.Syscall6(
		syscall.SYS_FADVISE64,
		uintptr(s.fd.Fd()),
		uintptr(offset),
		uintptr(length),
		uintptr(4), // POSIX_FADV_DONTNEED = 4
		0, 0,
	)
}

// readEntryFromDevice reads one entry directly from the NVMe device
// using O_DIRECT, bypassing the filesystem page cache entirely.
// Used by follower after doPBACopy to read back the copied data.
func (s *Server) readEntryFromDevice(headerSlot uint64) (Entry, uint64) {
	// Resolve PBA for this slot
	logicalOff := int64(RING_OFFSET) + int64(headerSlot)*BLOCK_UNIT
	metaPath := filepath.Join(s.metadataDir, s.Metadata())

	// Read header (4KB aligned for O_DIRECT)
	seg, err := blockcopy.L_get_pba(metaPath, logicalOff, uint64(BLOCK_UNIT))
	if err != nil {
		panic(fmt.Sprintf("readEntryFromDevice: get PBA for slot %d: %v", headerSlot, err))
	}

	headerBuf := blockcopy.DirectRead(s.devicePath, seg.PBA, PAGE_SIZE)

	term := binary.LittleEndian.Uint64(headerBuf[0:])
	cmdLen := binary.LittleEndian.Uint64(headerBuf[8:])
	numSlots := binary.LittleEndian.Uint64(headerBuf[16:])

	cmd := make([]byte, cmdLen)
	if cmdLen > 0 {
		// Read payload slots
		copied := 0
		for i := uint64(1); i < numSlots; i++ {
			pSlot := (headerSlot + i) % RING_SLOTS
			pOff := int64(RING_OFFSET) + int64(pSlot)*BLOCK_UNIT
			pSeg, err := blockcopy.L_get_pba(metaPath, pOff, uint64(BLOCK_UNIT))
			if err != nil {
				panic(fmt.Sprintf("readEntryFromDevice: get PBA for payload slot %d: %v", pSlot, err))
			}
			payloadBuf := blockcopy.DirectRead(s.devicePath, pSeg.PBA, PAGE_SIZE)
			copied += copy(cmd[copied:], payloadBuf[:BLOCK_UNIT])
		}
	}

	return Entry{Term: term, Command: cmd}, (headerSlot + numSlots) % RING_SLOTS
}
