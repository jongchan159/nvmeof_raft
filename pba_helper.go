//go:build raft_save
// +build raft_save

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
//
// Flow:
//  1. Resolve follower's destination PBA (tailSlot position)
//  2. Call R_write_pba(device, leaderPbaSrc, followerPbaDst, nbytes)
//  3. Storage node copies the block locally — no log data crosses the network
//
// Must be called with s.mu held.
// Returns nil on success; caller sets rsp.Success=false on error.
func (s *Server) doPBACopy(leaderPbaSrc, logBlockLength, dstSlot uint64) error {
    if leaderPbaSrc == 0 || logBlockLength == 0 {
        return nil
    }

    nbytes := blockcopy.AlignUp(logBlockLength * uint64(BLOCK_UNIT))

    // Read from leader's PBA via device (O_DIRECT read works over NVMe-oF)
    buf, err := blockcopy.DirectRead(s.devicePath, leaderPbaSrc, nbytes)
    if err != nil {
        return fmt.Errorf("doPBACopy: DirectRead(pba=0x%X, nbytes=%d): %v", leaderPbaSrc, nbytes, err)
    }

    // Write to follower's ring file at the OLD tail slot (before pointer advance)
    fileOff := int64(RING_OFFSET) + int64(dstSlot)*BLOCK_UNIT
    n, err := s.fd.WriteAt(buf[:nbytes], fileOff)
    if err != nil {
        return fmt.Errorf("doPBACopy: WriteAt(offset=%d, nbytes=%d): %v", fileOff, nbytes, err)
    }
    if n != int(nbytes) {
        return fmt.Errorf("doPBACopy: short write %d/%d", n, nbytes)
    }
    s.fd.Sync()

    s.debugf("[PBA COPY] src=0x%X -> fileOff=%d nbytes=%d blocks=%d",
        leaderPbaSrc, fileOff, nbytes, logBlockLength)
    return nil
}

// readEntryDirect reads one entry from the device using O_DIRECT,
// bypassing page cache entirely. Used after doPBACopy on follower.
func (s *Server) readEntryDirect(headerSlot uint64) (Entry, uint64, error) {
	metaPath := filepath.Join(s.metadataDir, s.Metadata())

	logicalOff := int64(RING_OFFSET) + int64(headerSlot)*BLOCK_UNIT
	seg, err := blockcopy.L_get_pba(metaPath, logicalOff, uint64(BLOCK_UNIT))
	if err != nil {
		return Entry{}, 0, fmt.Errorf("readEntryDirect: L_get_pba slot %d: %v", headerSlot, err)
	}
	rawPBA := seg.PBA + s.partitionOffsetBytes

	// Align PBA down to 4KB boundary for O_DIRECT
	alignedPBA := rawPBA &^ (PAGE_SIZE - 1)    // round down to 4KB
	offsetInPage := int64(rawPBA - alignedPBA) // where our data starts within the 4KB block

	buf, err := blockcopy.DirectRead(s.devicePath, alignedPBA, PAGE_SIZE)
	if err != nil {
		return Entry{}, 0, fmt.Errorf("readEntryDirect: DirectRead header slot %d pba=0x%X: %v", headerSlot, alignedPBA, err)
	}

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
				return Entry{}, 0, fmt.Errorf("readEntryDirect: L_get_pba payload slot %d: %v", pSlot, err)
			}
			pRawPBA := pSeg.PBA + s.partitionOffsetBytes
			pAlignedPBA := pRawPBA &^ (PAGE_SIZE - 1)
			pOffInPage := int64(pRawPBA - pAlignedPBA)

			pBuf, err := blockcopy.DirectRead(s.devicePath, pAlignedPBA, PAGE_SIZE)
			if err != nil {
				return Entry{}, 0, fmt.Errorf("readEntryDirect: DirectRead payload slot %d pba=0x%X: %v", pSlot, pAlignedPBA, err)
			}
			copied += copy(cmd[copied:], pBuf[pOffInPage:pOffInPage+BLOCK_UNIT])
		}
	}

	return Entry{Term: term, Command: cmd}, (headerSlot + numSlots) % RING_SLOTS, nil
}
