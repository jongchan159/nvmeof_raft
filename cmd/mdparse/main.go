package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

/*
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>

static int aligned_pread(int fd, void *buf, size_t nbytes, uint64_t offset) {
	ssize_t r = pread(fd, buf, nbytes, (off_t)offset);
	if (r != (ssize_t)nbytes) return -1;
	return 0;
}
*/
import "C"

const (
	BLOCK_UNIT  = 512
	PAGE_SIZE   = 4096
	HEADER_SIZE = BLOCK_UNIT
	RING_OFFSET = HEADER_SIZE

	SLOTS_PER_PAGE = PAGE_SIZE / BLOCK_UNIT
	NUM_PAGES      = 4
	TOTAL_SLOTS    = NUM_PAGES * SLOTS_PER_PAGE
	RING_SLOTS     = TOTAL_SLOTS - 1
	RING_FILE_SIZE = TOTAL_SLOTS * BLOCK_UNIT // 16384
)

type FileHeader struct {
	CurrentTerm  uint64
	VotedFor     uint64
	TailLogIndex uint64
	TailSlot     uint64
	CommitIndex  uint64
	LastApplied  uint64
}

type SlotHeader struct {
	Term     uint64
	CmdLen   uint64
	NumSlots uint64
}

func parseHeader(data []byte) FileHeader {
	return FileHeader{
		CurrentTerm:  binary.LittleEndian.Uint64(data[0:8]),
		VotedFor:     binary.LittleEndian.Uint64(data[8:16]),
		TailLogIndex: binary.LittleEndian.Uint64(data[16:24]),
		TailSlot:     binary.LittleEndian.Uint64(data[24:32]),
		CommitIndex:  binary.LittleEndian.Uint64(data[32:40]),
		LastApplied:  binary.LittleEndian.Uint64(data[40:48]),
	}
}

func parseSlotHeader(data []byte) SlotHeader {
	return SlotHeader{
		Term:     binary.LittleEndian.Uint64(data[0:8]),
		CmdLen:   binary.LittleEndian.Uint64(data[8:16]),
		NumSlots: binary.LittleEndian.Uint64(data[16:24]),
	}
}

// directRead reads one 4KB page from device using O_DIRECT + posix_memalign
func directRead(devFd int, pba uint64) ([]byte, error) {
	var buf unsafe.Pointer
	ret := C.posix_memalign(&buf, C.size_t(PAGE_SIZE), C.size_t(PAGE_SIZE))
	if ret != 0 {
		return nil, fmt.Errorf("posix_memalign failed: %d", ret)
	}
	defer C.free(buf)

	alignedPBA := pba &^ (PAGE_SIZE - 1)

	r := C.aligned_pread(C.int(devFd), buf, C.size_t(PAGE_SIZE), C.uint64_t(alignedPBA))
	if r != 0 {
		return nil, fmt.Errorf("pread failed at PBA 0x%X", alignedPBA)
	}

	return C.GoBytes(buf, C.int(PAGE_SIZE)), nil
}

// getFIEMAP returns physical byte address for each 4KB page of the file
func getFIEMAP(filePath string, numPages int) ([]uint64, error) {
	fd, err := syscall.Open(filePath, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(fd)

	pbas := make([]uint64, numPages)
	for i := 0; i < numPages; i++ {
		logical := uint64(i * PAGE_SIZE)

		type fiemapExtent struct {
			feLogical    uint64
			fePhysical   uint64
			feLength     uint64
			feReserved64 [2]uint64
			feFlags      uint32
			feReserved   [3]uint32
		}
		type fiemap struct {
			fmStart       uint64
			fmLength      uint64
			fmFlags       uint32
			fmMappedCount uint32
			fmExtentCount uint32
			fmReserved    uint32
			fmExtents     [1]fiemapExtent
		}

		var fm fiemap
		fm.fmStart = logical
		fm.fmLength = PAGE_SIZE
		fm.fmExtentCount = 1

		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL,
			uintptr(fd), uintptr(0xC020660B), uintptr(unsafe.Pointer(&fm)))
		if errno != 0 {
			return nil, fmt.Errorf("FIEMAP page %d: %v", i, errno)
		}
		if fm.fmMappedCount == 0 {
			return nil, fmt.Errorf("no extent for page %d", i)
		}

		ext := fm.fmExtents[0]
		pbas[i] = ext.fePhysical + (logical - ext.feLogical)
	}
	return pbas, nil
}

// readAllFromDevice reads entire 16KB metadata file from device via FIEMAP + O_DIRECT
func readAllFromDevice(metaFile, devicePath string, partOffset uint64) ([]byte, error) {
	numPages := RING_FILE_SIZE / PAGE_SIZE
	pbas, err := getFIEMAP(metaFile, numPages)
	if err != nil {
		return nil, fmt.Errorf("FIEMAP: %v", err)
	}

	devFd, err := syscall.Open(devicePath, syscall.O_RDONLY|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, fmt.Errorf("open device: %v (try sudo)", err)
	}
	defer syscall.Close(devFd)

	fileData := make([]byte, RING_FILE_SIZE)
	for i := 0; i < numPages; i++ {
		devPBA := pbas[i] + partOffset
		page, err := directRead(devFd, devPBA)
		if err != nil {
			return nil, fmt.Errorf("page %d (PBA 0x%X): %v", i, devPBA, err)
		}
		copy(fileData[i*PAGE_SIZE:(i+1)*PAGE_SIZE], page)
	}
	return fileData, nil
}

// ============================================================
// Display
// ============================================================
func printHeader(hdr FileHeader) {
	fmt.Println("  ┌─────────────────────────────────────────┐")
	fmt.Println("  │            FILE HEADER (512B)           │")
	fmt.Println("  ├─────────────────────────────────────────┤")
	fmt.Printf("  │  currentTerm  : %-24d│\n", hdr.CurrentTerm)
	fmt.Printf("  │  votedFor     : %-24d│\n", hdr.VotedFor)
	fmt.Printf("  │  tailLogIndex : %-24d│\n", hdr.TailLogIndex)
	fmt.Printf("  │  tailSlot     : %-24d│\n", hdr.TailSlot)
	fmt.Printf("  │  commitIndex  : %-24d│\n", hdr.CommitIndex)
	fmt.Printf("  │  lastApplied  : %-24d│\n", hdr.LastApplied)
	fmt.Println("  └─────────────────────────────────────────┘")
	fmt.Println()
}

func printSlots(fileData []byte, n int, hdr FileHeader) {
	fmt.Printf("  RING BUFFER (%d usable slots)\n", RING_SLOTS)
	fmt.Printf("  tailSlot=%d (next write position)\n", hdr.TailSlot)
	fmt.Println()

	fmt.Println("  Slot  Offset   Term       CmdLen  NumSlots  Command")
	fmt.Println("  ────  ──────   ────       ──────  ────────  ───────")

	entryCount := 0
	slot := uint64(0)
	for slot < RING_SLOTS {
		offset := RING_OFFSET + int(slot)*BLOCK_UNIT
		if offset+BLOCK_UNIT > n {
			break
		}

		slotData := fileData[offset : offset+BLOCK_UNIT]
		sh := parseSlotHeader(slotData)

		// Skip empty slots
		if sh.Term == 0 && sh.CmdLen == 0 && sh.NumSlots == 0 {
			slot++
			continue
		}

		// Validate numSlots
		if sh.NumSlots == 0 || sh.NumSlots > RING_SLOTS {
			slot++
			continue
		}

		entryCount++

		marker := "  "
		if slot == hdr.TailSlot {
			marker = "→ "
		}

		var cmdPreview string
		if sh.CmdLen > 0 && sh.NumSlots > 1 {
			// Payload in following slots
			var cmdBytes []byte
			for i := uint64(1); i < sh.NumSlots; i++ {
				paySlot := (slot + i) % RING_SLOTS
				payOffset := RING_OFFSET + int(paySlot)*BLOCK_UNIT
				if payOffset+BLOCK_UNIT <= n {
					cmdBytes = append(cmdBytes, fileData[payOffset:payOffset+BLOCK_UNIT]...)
				}
			}
			if uint64(len(cmdBytes)) > sh.CmdLen {
				cmdBytes = cmdBytes[:sh.CmdLen]
			}
			cmdPreview = sanitize(string(cmdBytes), 40)
		} else if sh.CmdLen > 0 && sh.NumSlots == 1 {
			// Inline in header [24:]
			end := 24 + sh.CmdLen
			if end > BLOCK_UNIT {
				end = BLOCK_UNIT
			}
			cmdPreview = sanitize(string(slotData[24:end]), 40)
		} else {
			cmdPreview = "(no-op)"
		}

		fmt.Printf("  %s%-4d  0x%04X   %-10d %-7d %-9d %s\n",
			marker, slot, offset, sh.Term, sh.CmdLen, sh.NumSlots, cmdPreview)

		// Walk past this entry (header + payload slots)
		slot += sh.NumSlots
	}

	if entryCount == 0 {
		fmt.Println("  (all slots empty)")
	}
	fmt.Println()
	fmt.Printf("  Summary: %d entries found\n", entryCount)
}

func sanitize(s string, maxLen int) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 32 && r < 127 {
			b.WriteRune(r)
		} else {
			b.WriteRune('.')
		}
	}
	result := b.String()
	if len(result) > maxLen {
		result = result[:maxLen] + "..."
	}
	return result
}

// ============================================================
// Main
// ============================================================
// readFromPBA reads 16KB ring buffer directly from a device PBA address.
// The PBA should point to the start of the ring file (header slot).
func readFromPBA(devicePath string, pba uint64) ([]byte, error) {
	devFd, err := syscall.Open(devicePath, syscall.O_RDONLY|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, fmt.Errorf("open device: %v (try sudo)", err)
	}
	defer syscall.Close(devFd)

	fileData := make([]byte, RING_FILE_SIZE)
	numPages := RING_FILE_SIZE / PAGE_SIZE
	for i := 0; i < numPages; i++ {
		pagePBA := pba + uint64(i*PAGE_SIZE)
		page, err := directRead(devFd, pagePBA)
		if err != nil {
			return nil, fmt.Errorf("page %d (PBA 0x%X): %v", i, pagePBA, err)
		}
		copy(fileData[i*PAGE_SIZE:(i+1)*PAGE_SIZE], page)
	}
	return fileData, nil
}

func main() {
	device := flag.String("device", "", "NVMe device path (required, e.g., /dev/nvme1n1)")
	partOffset := flag.Uint64("partition-offset", 0, "Partition start offset in bytes")
	pbaAddr := flag.String("pba", "", "Direct PBA address (hex, e.g., 0x488108000). Reads 16KB ring buffer from this address.")
	flag.Parse()

	if *device == "" {
		fmt.Println("Usage:")
		fmt.Println()
		fmt.Println("  Mode 1: FIEMAP-based (reads PBA from file extent mapping)")
		fmt.Println("    sudo mdparse --device=/dev/nvmeXn1 --partition-offset=N <md_*.dat | directory>")
		fmt.Println()
		fmt.Println("  Mode 2: Direct PBA (reads 16KB from specified device address)")
		fmt.Println("    sudo mdparse --device=/dev/nvmeXn1 --pba=0x488108000")
		fmt.Println("    (Use dst PBA from [PBA COPY] log, rounded down to 4KB: dst & ~0xFFF)")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  sudo mdparse --device=/dev/nvme2n1 --partition-offset=10738466816 /mnt/nvmeof_raft/metadata5/md_2.dat")
		fmt.Println("  sudo mdparse --device=/dev/nvme2n1 --pba=0x488108000")
		os.Exit(1)
	}

	// ---- Mode 2: Direct PBA ----
	if *pbaAddr != "" {
		// Parse hex address
		pbaStr := *pbaAddr
		if strings.HasPrefix(pbaStr, "0x") || strings.HasPrefix(pbaStr, "0X") {
			pbaStr = pbaStr[2:]
		}
		pba, err := strconv.ParseUint(pbaStr, 16, 64)
		if err != nil {
			fmt.Printf("Error: invalid PBA address %q: %v\n", *pbaAddr, err)
			os.Exit(1)
		}

		// Align down to 4KB boundary (ring file header starts at page boundary)
		alignedPBA := pba &^ (PAGE_SIZE - 1)
		// Back up to the start of the ring file: header is at -RING_OFFSET from slot 0
		// If pba points to a slot, we need the header which is RING_OFFSET before slot 0
		// Compute the file-start PBA: round down to nearest RING_FILE_SIZE boundary
		// Simpler: subtract the slot offset within the file
		slotByteOffset := pba - alignedPBA // offset within 4KB page
		_ = slotByteOffset

		fmt.Println("==================================================")
		fmt.Printf("  Metadata Parser (Direct PBA via %s)\n", *device)
		fmt.Println("==================================================")
		fmt.Println()
		fmt.Printf("  PBA: 0x%X (aligned: 0x%X)\n", pba, alignedPBA)
		fmt.Printf("  Reading %d bytes (%d pages) from device\n", RING_FILE_SIZE, RING_FILE_SIZE/PAGE_SIZE)
		fmt.Println()

		// Read starting from the aligned PBA
		// User should provide the PBA of the ring file header (slot -1)
		// or the PBA from [PBA COPY] dst log minus RING_OFFSET
		data, err := readFromPBA(*device, alignedPBA)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			os.Exit(1)
		}

		hdr := parseHeader(data[:HEADER_SIZE])
		printHeader(hdr)
		printSlots(data, len(data), hdr)

		fmt.Println("==================================================")
		return
	}

	// ---- Mode 1: FIEMAP-based ----
	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Error: provide md_*.dat file path or directory")
		os.Exit(1)
	}

	target := args[0]
	stat, err := os.Stat(target)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("==================================================")
	fmt.Printf("  Metadata Parser (FIEMAP + O_DIRECT via %s)\n", *device)
	fmt.Println("==================================================")
	fmt.Println()

	parseOne := func(path string) {
		fmt.Printf("  Source: %s\n", path)
		fmt.Printf("  Device: %s  Offset: %d (0x%X)\n", *device, *partOffset, *partOffset)
		fmt.Println()

		data, err := readAllFromDevice(path, *device, *partOffset)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			return
		}

		hdr := parseHeader(data[:HEADER_SIZE])
		printHeader(hdr)
		printSlots(data, len(data), hdr)
		fmt.Println("  --------------------------------------------------")
		fmt.Println()
	}

	if stat.IsDir() {
		filepath.Walk(target, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && strings.HasPrefix(info.Name(), "md_") && strings.HasSuffix(info.Name(), ".dat") {
				parseOne(path)
			}
			return nil
		})
	} else {
		parseOne(target)
	}

	fmt.Println("==================================================")
}