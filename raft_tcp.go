//go:build raft_tcp
// +build raft_tcp

package nvmeof_raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
	"nvmeof_raft/blockcopy"
	"nvmeof_raft/rdmacm"
)


// ============================================================
// Constants
// ============================================================
const BLOCK_UNIT = 512
const PAGE_SIZE = 4096
const HEADER_SIZE = BLOCK_UNIT
const RING_OFFSET = HEADER_SIZE

const SLOTS_PER_PAGE = PAGE_SIZE / BLOCK_UNIT  // 8
const NUM_PAGES = 2048*16                         // modify this constant to extent disk space
const TOTAL_SLOTS = NUM_PAGES * SLOTS_PER_PAGE
const RING_SLOTS = TOTAL_SLOTS - 1
// ============================================================ 

func Assert(msg string, a, b interface{}) {
	if a != b {
		panic(fmt.Sprintf("%s. Got a = %#v, b = %#v", msg, a, b))
	}
}

type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

type ApplyResult struct {
	Result []byte
	Error  error
}

type Entry struct {
	Command []byte
	Term    uint64
	result  chan ApplyResult
}

type RPCMessage struct {
	Term uint64
}

type RequestVoteRequest struct {
	RPCMessage
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	RPCMessage
	VoteGranted bool
}

type AppendEntriesRequest struct {
	RPCMessage
	LeaderId     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64

	// Raw Block Copy Metadata
	LeaderPbaSrc   uint64 // physical block address on leader's ring
	LogBlockLength uint64 // number of 512B blocks to copy
	NumEntries     uint64 // how many log entries this batch contains
	SlotsPerEntry  uint64 // average slots per entry (for follower pointer advance)
	StartSlot      uint64 // ring slot where the first entry starts (handles wrap alignment)
}

type AppendEntriesResponse struct {
	RPCMessage
	Success bool
}

// ============================================================
// LogEntryState — leader-only, in-memory ring buffer protection
// ============================================================
type LogEntryState uint8

const (
	SlotUnallocated   LogEntryState = iota // never written or already freed
	LeaderAppended                         // written by Apply(), not yet sent
	LeaderReplicating                      // sent to followers, waiting for quorum
	LeaderCommitted                        // quorum achieved, safe to overwrite
)

func (st LogEntryState) String() string {
	switch st {
	case SlotUnallocated:
		return "UNALLOCATED"
	case LeaderAppended:
		return "APPENDED"
	case LeaderReplicating:
		return "REPLICATING"
	case LeaderCommitted:
		return "COMMITTED"
	default:
		return "UNKNOWN"
	}
}

type ClusterMember struct {
	Id         uint64
	Address    string
	nextIndex  uint64
	matchIndex uint64
	votedFor   uint64
	rpcClient  *rpc.Client
}

type ServerState string

const (
	leaderState    ServerState = "leader"
	followerState              = "follower"
	candidateState             = "candidate"
)

// slotRange tracks which ring slots a single log entry occupies.
type slotRange struct {
	start    uint64		// first slot index of a log entry
	numSlots uint64		// number of slots used by a log entry
}

type Server struct {
	done   bool
	server *http.Server
	Debug  bool

	mu          sync.Mutex
	currentTerm uint64
	log         []Entry // log[0] is sentinel (Term=0, Command=nil)

	id               uint64
	address          string
	electionTimeout  time.Time
	HeartbeatMs      int
	heartbeatTimeout time.Time
	statemachine     StateMachine
	metadataDir      string
	fd               *os.File

	commitIndex  uint64
	lastApplied  uint64
	state        ServerState
	cluster      []ClusterMember
	clusterIndex int

	// NVMe-oF Setting
	devicePath           string // NVMe-oF block device path
	partitionOffsetBytes uint64 // byte offset of partition start on whole device

	// ---- Single-pointer ring buffer ----
	// Only tailSlot advances forward. Overwrite protection is handled
	// by slotStates[] which is leader-only and in-memory.
	tailLogIndex uint64 // absolute logical index of next entry to write
	tailSlot     uint64 // next ring slot to write

	// Leader-only: per-slot state array for overwrite protection.
	// Reset when this node becomes leader. Followers ignore it entirely.
	slotStates [RING_SLOTS]LogEntryState

	// Leader-only: maps logIndex -> slot range for state transitions.
	logSlotMap map[uint64]slotRange

	// Condition variable: Apply() blocks here when ring is full.
	ringNotFull *sync.Cond
}

// ============================================================
// Helpers
// ============================================================
func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [Id: %d, Term: %d] %s",
		time.Now().Format(time.RFC3339Nano), s.id, s.currentTerm, msg)
}

func (s *Server) debug(msg string) {
	if s.Debug {
		fmt.Println(s.debugmsg(msg))
	}
}

func (s *Server) debugf(msg string, args ...interface{}) {
	if s.Debug {
		s.debug(fmt.Sprintf(msg, args...))
	}
}

func (s *Server) warn(msg string) {
	fmt.Println("[WARN] " + s.debugmsg(msg))
}

func (s *Server) warnf(msg string, args ...interface{}) {
	fmt.Println("[WARN] " + fmt.Sprintf(msg, args...))
}

func Server_assert(s *Server, msg string, a, b interface{}) {
	Assert(s.debugmsg(msg), a, b)
}

// ============================================================
// Ring buffer helpers
// ============================================================

// slotsForEntry returns 1 (header) + ceil(cmdLen/512) payload slots.
func slotsForEntry(cmdLen int) uint64 {
	if cmdLen == 0 {
		return 1 // header only (no-op)
	}
	return 1 + uint64((cmdLen+BLOCK_UNIT-1)/BLOCK_UNIT)
}

// slotOffset returns file byte offset for a given slot number.
func slotOffset(slot uint64) int64 {
	return int64(RING_OFFSET) + int64(slot)*BLOCK_UNIT
}

// oldestLogIndex returns the absolute index of the oldest entry in s.log.
func (s *Server) oldestLogIndex() uint64 {
	real := uint64(len(s.log) - 1) // exclude sentinel
	if real == 0 {
		return s.tailLogIndex
	}
	return s.tailLogIndex - real
}

// logSlice converts absolute logIdx to s.log slice index.
func (s *Server) logSlice(logIdx uint64) uint64 {
	return 1 + (logIdx - s.oldestLogIndex())
}

// ============================================================
// Slot state management (leader-only)
// ============================================================

// canOverwrite checks if `count` slots starting at `slot` are writable.
func (s *Server) canOverwrite(slot, count uint64) bool {
	for i := uint64(0); i < count; i++ {
		st := s.slotStates[(slot+i)%RING_SLOTS]
		if st != SlotUnallocated && st != LeaderCommitted {
			return false
		}
	}
	return true
}

// canWriteAll checks if all commands can be written to the ring, respecting
// wrap-alignment (entries that don't fit before ring end skip to slot 0).
func (s *Server) canWriteAll(commands [][]byte) bool {
	slot := s.tailSlot
	for _, cmd := range commands {
		needed := slotsForEntry(len(cmd))
		if slot+needed > RING_SLOTS {
			slot = 0
		}
		if !s.canOverwrite(slot, needed) {
			return false
		}
		slot += needed
	}
	return true
}

// markSlots sets state for a contiguous range of slots.
func (s *Server) markSlots(slot, count uint64, state LogEntryState) {
	for i := uint64(0); i < count; i++ {
		s.slotStates[(slot+i)%RING_SLOTS] = state
	}
}

// initSlotStates resets all slot states and slot map. Called on becoming leader.
func (s *Server) initSlotStates() {
	for i := range s.slotStates {
		s.slotStates[i] = SlotUnallocated
	}
	s.logSlotMap = make(map[uint64]slotRange)
}

// ============================================================
// Ring buffer I/O
// ============================================================

// encodeEntry encodes one entry into dst (must have len >= numSlots*BLOCK_UNIT).
// Returns the number of slots used.
func encodeEntry(e Entry, dst []byte) uint64 {
	needed := slotsForEntry(len(e.Command))
	// Header slot
	binary.LittleEndian.PutUint64(dst[0:], e.Term)
	binary.LittleEndian.PutUint64(dst[8:], uint64(len(e.Command)))
	binary.LittleEndian.PutUint64(dst[16:], needed)
	// Payload slots
	written := 0
	for i := uint64(1); i < needed; i++ {
		off := int(i) * BLOCK_UNIT
		written += copy(dst[off:off+BLOCK_UNIT], e.Command[written:])
	}
	return needed
}

// ============================================================
// Persist / Restore
// ============================================================
// File header layout (512B):
//   [ 0: 7] currentTerm
//   [ 8:15] votedFor
//   [16:23] tailLogIndex
//   [24:31] tailSlot
//   [32:39] commitIndex
//   [40:47] lastApplied

// persistCircular writes Raft metadata and new log entries directly to the
// NVMe-oF storage device, bypassing the filesystem layer entirely.
// Log entries are written via DirectWrite at the FIEMAP-resolved device PBA.
// The header (term, votedFor, indices) is also written directly to device.
func (s *Server) persistCircular(writeLog bool, nNewEntries int) {
	t := time.Now()

	if nNewEntries == 0 && writeLog {
		nNewEntries = len(s.log) - 1
	}

	metaPath := path.Join(s.metadataDir, s.Metadata())
	s.debugf("persistCircular: metaPath = %s", metaPath)

	// Write new entries directly to storage (bypassing filesystem).
	// FIEMAP resolves the file offset -> physical device address for each run,
	// then DirectWrite sends data straight to the block device via O_DIRECT.
	if writeLog && nNewEntries > 0 {
		start := len(s.log) - nNewEntries
		if start < 1 {
			start = 1
		}

		var runBuf []byte    // accumulated encoded bytes for current contiguous run
		var runOff int64     // file offset where the current run starts

		flushRun := func() {
			if len(runBuf) > 0 {
				// Resolve physical device address for this run via FIEMAP
				seg, err := blockcopy.L_get_pba(metaPath, runOff, uint64(len(runBuf)))
				if err != nil {
					panic(fmt.Errorf("persistCircular: L_get_pba(off=%d): %v", runOff, err))
				}
				pba := seg.PBA + s.partitionOffsetBytes
				// Write directly to storage, bypassing the filesystem
				if err := blockcopy.DirectWrite(s.devicePath, pba, runBuf); err != nil {
					panic(fmt.Errorf("persistCircular: DirectWrite(pba=0x%X): %v", pba, err))
				}
				runBuf = runBuf[:0]
			}
		}

		for i := start; i < len(s.log); i++ {
			logIdx := s.tailLogIndex - uint64(len(s.log)-i)
			e := s.log[i]
			needed := slotsForEntry(len(e.Command))

			// Alignment: if entry would cross ring boundary, flush and reset to slot 0
			if s.tailSlot+needed > RING_SLOTS {
				flushRun()
				s.tailSlot = 0
			}

			if len(runBuf) == 0 {
				runOff = slotOffset(s.tailSlot)
			}

			// Encode entry directly into run buffer
			entryBytes := int(needed) * BLOCK_UNIT
			off := len(runBuf)
			runBuf = append(runBuf, make([]byte, entryBytes)...)
			encodeEntry(e, runBuf[off:])

			startSlot := s.tailSlot
			s.tailSlot += needed

			if s.state == leaderState {
				s.logSlotMap[logIdx] = slotRange{start: startSlot, numSlots: needed}
				s.markSlots(startSlot, needed, LeaderAppended)
			}
		}
		flushRun()
	}

	// Write header directly to storage via FIEMAP-resolved device PBA.
	// Header is at logical offset 0 in the metadata file.
	var header [HEADER_SIZE]byte
	binary.LittleEndian.PutUint64(header[0:], s.currentTerm)
	binary.LittleEndian.PutUint64(header[8:], s.getVotedFor())
	binary.LittleEndian.PutUint64(header[16:], s.tailLogIndex)
	binary.LittleEndian.PutUint64(header[24:], s.tailSlot)
	binary.LittleEndian.PutUint64(header[32:], s.commitIndex)
	binary.LittleEndian.PutUint64(header[40:], s.lastApplied)

	s.debugf("persistCirculr: metaPath: %s", metaPath)
	headerSeg, err := blockcopy.L_get_pba(metaPath, 0, HEADER_SIZE)
	if err != nil {
		panic(fmt.Errorf("persistCircular: L_get_pba header: %v", err))
	}
	headerPBA := headerSeg.PBA + s.partitionOffsetBytes
	if err := blockcopy.DirectWrite(s.devicePath, headerPBA, header[:]); err != nil {
		panic(fmt.Errorf("persistCircular: DirectWrite header(pba=0x%X): %v", headerPBA, err))
	}

	s.debugf("Persisted in %s. Term:%d LogLen:%d (%d new) VotedFor:%d",
		time.Now().Sub(t), s.currentTerm, len(s.log)-1, nNewEntries, s.getVotedFor())
}

func (s *Server) restoreCircular() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fd == nil {
		if err := os.MkdirAll(s.metadataDir, 0755); err != nil {
			panic(err)
		}
		var err error
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, s.Metadata()),
			os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
	}

    // Pre-allocate full ring file so all slots have physical blocks (FIEMAP)
    const RING_FILE_SIZE = TOTAL_SLOTS * BLOCK_UNIT
    stat, err := s.fd.Stat()
    if err != nil {
        panic(err)
    }
    if stat.Size() < RING_FILE_SIZE {
        zeros := make([]byte, RING_FILE_SIZE-stat.Size())
        s.fd.Seek(stat.Size(), 0)
        s.fd.Write(zeros)
        s.fd.Sync()
    }

	s.fd.Seek(0, 0)
	var header [HEADER_SIZE]byte
	n, err := io.ReadFull(s.fd, header[:])

	if err == io.EOF || err == io.ErrUnexpectedEOF || n < HEADER_SIZE {
		s.currentTerm = 0
		s.setVotedFor(0)
		s.tailLogIndex = 1
		s.tailSlot = 0
		s.commitIndex = 0
		s.lastApplied = 1
		s.log = nil
		s.ensureLog()
		s.initSlotStates()
		return
	}
	if err != nil {
		panic(err)
	}

	s.currentTerm = binary.LittleEndian.Uint64(header[0:])
	s.setVotedFor(binary.LittleEndian.Uint64(header[8:]))
	//s.tailLogIndex = binary.LittleEndian.Uint64(header[16:])
	//s.tailSlot = binary.LittleEndian.Uint64(header[24:])
	// s.commitIndex = binary.LittleEndian.Uint64(header[32:])
	// s.lastApplied = binary.LittleEndian.Uint64(header[40:])
	s.tailLogIndex = 1
	s.tailSlot = 0
	s.commitIndex = 0
	s.lastApplied = 1

	// On restart, start with empty in-memory log.
	// Leader election will resync all entries via appendEntries.
	// The ring buffer on disk preserves data for PBA copy correctness.
	s.log = nil
	s.ensureLog()
	s.initSlotStates()

	s.debugf("Restored: term=%d tailLogIndex=%d tailSlot=%d commit=%d applied=%d",
		s.currentTerm, s.tailLogIndex, s.tailSlot, s.commitIndex, s.lastApplied)

	if s.Debug {
		metaPath := path.Join(s.metadataDir, s.Metadata())
		for slot := uint64(0); slot < 8; slot++ {
			off := int64(RING_OFFSET) + int64(slot)*BLOCK_UNIT
			seg, err := blockcopy.L_get_pba(metaPath, off, BLOCK_UNIT)
			if err != nil {
				fmt.Printf("[INIT PBA] slot=%d offset=%d ERROR: %v\n", slot, off, err)
			} else {
				devPBA := seg.PBA + s.partitionOffsetBytes
				fmt.Printf("[INIT PBA] slot=%d offset=%d FIEMAP=0x%X devPBA=0x%X\n",
					slot, off, seg.PBA, devPBA)
			}
		}
	}
}

// ============================================================
// advanceCommitIndex
// ============================================================
func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// (1) Leader: raise commitIndex based on quorum
	if s.state == leaderState {
		lastLogIndex := s.tailLogIndex - 1
		for i := lastLogIndex; i > s.commitIndex; i-- {
			quorum := len(s.cluster)/2 + 1
			for j := range s.cluster {
				if quorum == 0 {
					break
				}
				if j == s.clusterIndex || s.cluster[j].matchIndex >= i {
					quorum--
				}
			}
			if quorum == 0 {
				oldCommit := s.commitIndex
				s.commitIndex = i
				s.debugf("New commit index: %d.", i)

				// Transition slot states -> Committed and wake blocked Apply()
				for idx := oldCommit + 1; idx <= i; idx++ {
					if sr, ok := s.logSlotMap[idx]; ok {
						s.markSlots(sr.start, sr.numSlots, LeaderCommitted)
						delete(s.logSlotMap, idx)
					}
				}
				s.ringNotFull.Broadcast()
				break
			}
		}
	}

	// (2) Apply committed entries to state machine
	oldest := s.oldestLogIndex()
	for s.lastApplied >= oldest &&
		s.lastApplied < s.tailLogIndex &&
		s.lastApplied <= s.commitIndex {

		logEntry := s.log[s.logSlice(s.lastApplied)]
		if len(logEntry.Command) > 0 {
			s.debugf("Entry applied: %d.", s.lastApplied)
			res, err := s.statemachine.Apply(logEntry.Command)
			if logEntry.result != nil {
				logEntry.result <- ApplyResult{Result: res, Error: err}
			}
		}
		s.lastApplied++
	}
}

func (s *Server) ensureLog() {
	if len(s.log) == 0 {
		s.log = append(s.log, Entry{})
	}
}

func (s *Server) setVotedFor(id uint64) {
	s.cluster[s.clusterIndex].votedFor = id
}

func (s *Server) getVotedFor() uint64 {
	return s.cluster[s.clusterIndex].votedFor
}

func (s *Server) Metadata() string {
	return fmt.Sprintf("md_%d.dat", s.id)
}

func NewServer(
	clusterConfig []ClusterMember,
	statemachine StateMachine,
	metadataDir string,
	clusterIndex int,
	devicePath string, // nvme device
	partitionOffsetBytes uint64, // partition offset
) *Server {
	var cluster []ClusterMember
	for _, c := range clusterConfig {
		if c.Id == 0 {
			panic("Id must not be 0.")
		}
		cluster = append(cluster, c)
	}

	srv := &Server{
		id:                   cluster[clusterIndex].Id,
		address:              cluster[clusterIndex].Address,
		cluster:              cluster,
		statemachine:         statemachine,
		metadataDir:          metadataDir,
		clusterIndex:         clusterIndex,
		devicePath:           devicePath,
		partitionOffsetBytes: partitionOffsetBytes,

		HeartbeatMs: 300,
		mu:          sync.Mutex{},
		logSlotMap:  make(map[uint64]slotRange),
	}
	srv.ringNotFull = sync.NewCond(&srv.mu)
	return srv
}

// ============================================================
// RequestVote
// ============================================================
func (s *Server) requestVote() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}
		go func(i int) {
			s.mu.Lock()
			lastLogIndex := s.tailLogIndex - 1
			var lastLogTerm uint64
			if len(s.log) > 1 {
				lastLogTerm = s.log[len(s.log)-1].Term
			}
			req := RequestVoteRequest{
				RPCMessage:   RPCMessage{Term: s.currentTerm},
				CandidateId:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			s.mu.Unlock()

			var rsp RequestVoteResponse
			if !s.rpcCall(i, "Server.HandleRequestVoteRequest", req, &rsp) {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()
			if s.updateTerm(rsp.RPCMessage) {
				return
			}
			if rsp.Term != req.Term {
				return
			}
			if rsp.VoteGranted {
				s.debugf("Vote granted by %d.", s.cluster[i].Id)
				s.cluster[i].votedFor = s.id
			}
		}(i)
	}
}

func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)
	rsp.VoteGranted = false
	rsp.Term = s.currentTerm

	if req.Term < s.currentTerm {
		return nil
	}

	var lastLogTerm uint64
	if len(s.log) > 1 {
		lastLogTerm = s.log[len(s.log)-1].Term
	}
	logLen := s.tailLogIndex - 1

	logOk := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= logLen)
	grant := req.Term == s.currentTerm && logOk &&
		(s.getVotedFor() == 0 || s.getVotedFor() == req.CandidateId)

	if grant {
		s.setVotedFor(req.CandidateId)
		rsp.VoteGranted = true
		s.resetElectionTimeout()
		s.persistCircular(false, 0)
	}
	return nil
}

func (s *Server) updateTerm(msg RPCMessage) bool {
	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.state = followerState
		s.setVotedFor(0)
		s.resetElectionTimeout()
		s.persistCircular(false, 0)
		return true
	}
	return false
}

// ============================================================
// HandleAppendEntriesRequest (follower)
// ============================================================
func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	s.debugf("[RECV AE] leader=%d term=%d prev=(%d,%d) commit=%d pba=0x%X blocks=%d entries=%d",
		req.LeaderId, req.Term, req.PrevLogIndex, req.PrevLogTerm,
		req.LeaderCommit, req.LeaderPbaSrc, req.LogBlockLength, req.NumEntries)

	if req.Term == s.currentTerm && s.state == candidateState {
		s.state = followerState
	}

	rsp.Term = s.currentTerm
	rsp.Success = false

	if s.state != followerState {
		return nil
	}
	if req.Term < s.currentTerm {
		return nil
	}

	s.resetElectionTimeout()

	// PrevLog consistency check
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex >= s.tailLogIndex {
			s.debugf("PrevLog fail: follower tail=%d < prev=%d", s.tailLogIndex, req.PrevLogIndex)
			return nil
		}
		oldest := s.oldestLogIndex()
		if req.PrevLogIndex >= oldest {
			localTerm := s.log[s.logSlice(req.PrevLogIndex)].Term
			if localTerm != req.PrevLogTerm {
				s.debugf("PrevLog fail: term mismatch idx=%d local=%d req=%d",
					req.PrevLogIndex, localTerm, req.PrevLogTerm)
				return nil
			}
		}
	}

	// Truncate conflicting suffix
	newTail := req.PrevLogIndex + 1
	if newTail < s.tailLogIndex {
		oldest := s.oldestLogIndex()
		keepCount := newTail - oldest
		s.log = s.log[:1+keepCount]
		// Rewind tailSlot: walk from slot 0 through kept entries
		if sr, ok := s.logSlotMap[newTail-1]; ok {
			s.tailSlot = (sr.start + sr.numSlots) % RING_SLOTS
		} else if keepCount == 0 {
			// No entries left — reset tailSlot to 0
			s.tailSlot = 0
		}
		// Clean up slot map
		for idx := newTail; idx < s.tailLogIndex; idx++ {
			delete(s.logSlotMap, idx)
		}
		s.tailLogIndex = newTail
	}

	// Storage-level block copy
	if req.NumEntries > 0 && req.LeaderPbaSrc != 0 {
		// Sync follower tailSlot to leader's StartSlot.
		if s.tailSlot != req.StartSlot {
			s.debugf("[WRAP ALIGN] follower tailSlot %d -> leader StartSlot %d",
				s.tailSlot, req.StartSlot)
			s.tailSlot = req.StartSlot
		}
		oldTailSlot := s.tailSlot

		exactSlots := req.LogBlockLength

		if err := s.doPBACopy(req.LeaderPbaSrc, req.LogBlockLength, oldTailSlot); err != nil {
			s.warnf("doPBACopy failed: %v", err)
			return nil
		}

		// Read back all entries directly from storage (bypassing filesystem).
		// FIEMAP resolves the slot's file offset -> device PBA, then DirectRead
		// fetches the data via O_DIRECT without touching the page cache.
		totalReadBytes := int(exactSlots) * BLOCK_UNIT
		readFileOff := int64(RING_OFFSET) + int64(oldTailSlot)*BLOCK_UNIT
		metaPath := path.Join(s.metadataDir, s.Metadata())
		readSeg, err := blockcopy.L_get_pba(metaPath, readFileOff, uint64(totalReadBytes))
		if err != nil {
			s.warnf("readback L_get_pba failed: %v", err)
			return nil
		}
		readPBA := readSeg.PBA + s.partitionOffsetBytes
		buf, err := blockcopy.DirectRead(s.devicePath, readPBA, uint64(totalReadBytes))
		if err != nil {
			s.warnf("readback DirectRead failed: %v", err)
			return nil
		}

		// Parse entries from buffer and append to in-memory log
		currentSlot := oldTailSlot
		bufOff := 0
		baseLogIdx := s.tailLogIndex
		for i := uint64(0); i < req.NumEntries; i++ {
			logIdx := baseLogIdx + i
			header := buf[bufOff : bufOff+BLOCK_UNIT]
			term := binary.LittleEndian.Uint64(header[0:])
			cmdLen := binary.LittleEndian.Uint64(header[8:])
			numSlots := binary.LittleEndian.Uint64(header[16:])

			cmd := make([]byte, cmdLen)
			copied := 0
			for j := uint64(1); j < numSlots; j++ {
				src := buf[bufOff+int(j)*BLOCK_UNIT : bufOff+int(j)*BLOCK_UNIT+BLOCK_UNIT]
				copied += copy(cmd[copied:], src)
			}

			e := Entry{Term: term, Command: cmd}
			s.debugf("[READBACK] slot=%d term=%d cmdLen=%d", currentSlot, e.Term, len(e.Command))
			s.log = append(s.log, e)
			s.logSlotMap[logIdx] = slotRange{start: currentSlot, numSlots: numSlots}
			currentSlot += numSlots
			bufOff += int(numSlots) * BLOCK_UNIT
		}

		// Advance ring pointers only after successful readback
		s.tailSlot = oldTailSlot + exactSlots
		s.tailLogIndex += req.NumEntries

		s.debugf("[PBA SYNC] tailLogIndex=%d tailSlot=%d logLen=%d",
			s.tailLogIndex, s.tailSlot, len(s.log)-1)
	}

	// Update commit index
	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = minUint64(req.LeaderCommit, s.tailLogIndex-1)
	}

	s.persistCircular(false, 0)
	rsp.Success = true
	return nil
}

// ============================================================
// Apply (leader only, with backpressure)
// ============================================================
var ErrApplyToLeader = errors.New("Cannot apply message to follower, apply to leader.")

func (s *Server) Apply(commands [][]byte) ([]ApplyResult, error) {
	s.debugf("[APPLY] enter Apply..")
	s.mu.Lock()
	if s.state != leaderState {
		s.mu.Unlock()
		return nil, ErrApplyToLeader
	}

	// Backpressure: block until ring has enough writable slots for all commands.
	for !s.canWriteAll(commands) {
		s.debugf("Ring full. Blocking Apply()...")
		s.ringNotFull.Wait()
		if s.state != leaderState {
			s.mu.Unlock()
			return nil, ErrApplyToLeader
		}
	}

	s.debugf("[APPLY] make result..")
	resultChans := make([]chan ApplyResult, len(commands))
	for i, command := range commands {
		resultChans[i] = make(chan ApplyResult)
		s.log = append(s.log, Entry{
			Term:    s.currentTerm,
			Command: command,
			result:  resultChans[i],
		})
		s.tailLogIndex++
	}

	s.debugf("[APPLY] calling persistCircular..")
	s.persistCircular(true, len(commands))
	s.mu.Unlock()

	s.debugf("[APPLY] calling appendEntries...")
	s.appendEntries()
	s.debugf("[APPLY] appendEntries returned, waiting for results...")

	results := make([]ApplyResult, len(commands))
	var wg sync.WaitGroup
	wg.Add(len(commands))
	for i, ch := range resultChans {
		go func(i int, c chan ApplyResult) {
			results[i] = <-c
			wg.Done()
		}(i, ch)
	}
	wg.Wait()
	return results, nil
}

// ============================================================
// RPC helper
// ============================================================

// rdmaDialHTTP opens an RDMA connection to address and performs the HTTP
// handshake that net/rpc's HTTP transport expects, returning an *rpc.Client.
func rdmaDialHTTP(address string) (*rpc.Client, error) {
	conn, err := rdmacm.Dial(address)
	if err != nil {
		return nil, err
	}
	_, err = io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
	if err != nil {
		conn.Close()
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == "200 Connected to Go RPC" {
		return rpc.NewClient(conn), nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return nil, err
}

func (s *Server) rpcCall(i int, name string, req, rsp interface{}) bool {
	s.mu.Lock()
	if s.cluster[i].rpcClient == nil {
		client, err := rpc.DialHTTP("tcp", s.cluster[i].Address)
		//client, err := rdmaDialHTTP(s.cluster[i].Address)
		
		if err != nil {
			s.mu.Unlock()
			s.warnf("Dial failed to %d: %v", s.cluster[i].Id, err)
			return false
		}
		s.cluster[i].rpcClient = client
	}
	client := s.cluster[i].rpcClient
	s.mu.Unlock()

	if err := client.Call(name, req, rsp); err != nil {
		s.warnf("Error calling %s on %d: %v", name, s.cluster[i].Id, err)
		s.mu.Lock()
		s.cluster[i].rpcClient = nil
		s.mu.Unlock()
		return false
	}
	return true
}

// ============================================================
// appendEntries (leader -> followers)
// ============================================================
const MAX_APPEND_ENTRIES_BATCH = 8000

func (s *Server) appendEntries() {
	s.mu.Lock()
	if s.state != leaderState {
		s.mu.Unlock()
		return
	}
	term := s.currentTerm
	leaderId := s.cluster[s.clusterIndex].Id
	leaderCommit := s.commitIndex
	s.mu.Unlock()

	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}
		go func(fi int) {
			s.mu.Lock()

			next := s.cluster[fi].nextIndex
			last := s.tailLogIndex - 1
			oldest := s.oldestLogIndex()

			if next > last+1 {
				next = last + 1
				s.cluster[fi].nextIndex = next
			}
			// Clamp: if next fell below what we have in memory, reset to oldest
			if next < oldest {
				next = oldest
				s.cluster[fi].nextIndex = next
			}

			var prevLogIndex uint64
			var prevLogTerm uint64
			if next > 0 {
				prevLogIndex = next - 1
				oldest := s.oldestLogIndex()
				if prevLogIndex >= oldest && prevLogIndex < s.tailLogIndex {
					prevLogTerm = s.log[s.logSlice(prevLogIndex)].Term
				}
			}

			var lenEntries uint64
			if last >= next {
				lenEntries = last - next + 1
				if lenEntries > MAX_APPEND_ENTRIES_BATCH {
					lenEntries = MAX_APPEND_ENTRIES_BATCH
				}
			}

			// Compute total slots and find start slot from logSlotMap
			var totalSlots uint64
			var slotsPerEntry uint64
			var startSlot uint64

			if lenEntries > 0 {
				// Get start slot from first entry's slot map
				if sr, ok := s.logSlotMap[next]; ok {
					startSlot = sr.start
				}

				for k := uint64(0); k < lenEntries; k++ {
					logIdx := next + k
					if sr, ok := s.logSlotMap[logIdx]; ok {
						totalSlots += sr.numSlots
					} else {
						e := s.log[s.logSlice(logIdx)]
						totalSlots += slotsForEntry(len(e.Command))
					}
				}
				slotsPerEntry = (totalSlots + lenEntries - 1) / lenEntries

				// Limit batch at ring wrap-around for contiguous FIEMAP
				slotsUntilEnd := RING_SLOTS - startSlot
				if totalSlots > slotsUntilEnd {
					totalSlots = 0
					lenEntries = 0
					for k := uint64(0); ; k++ {
						logIdx := next + k
						if logIdx > last {
							break
						}
						var needed uint64
						if sr, ok := s.logSlotMap[logIdx]; ok {
							needed = sr.numSlots
						} else {
							e := s.log[s.logSlice(logIdx)]
							needed = slotsForEntry(len(e.Command))
						}
						if totalSlots+needed > slotsUntilEnd {
							break
						}
						totalSlots += needed
						lenEntries++
					}
					if lenEntries > 0 {
						slotsPerEntry = (totalSlots + lenEntries - 1) / lenEntries
					}
				}

				// Mark slots as Replicating
				for k := uint64(0); k < lenEntries; k++ {
					if sr, ok := s.logSlotMap[next+k]; ok {
						s.markSlots(sr.start, sr.numSlots, LeaderReplicating)
					}
				}
			}

			var logBlockLength uint64
			if totalSlots > 0 {
				logBlockLength = totalSlots
			}

			var leaderPbaSrc uint64
			if lenEntries > 0 {
				pbaSrc, pbaBytes, err := s.leaderPBAForRange(startSlot, totalSlots)
				if err != nil {
					s.mu.Unlock()
					s.warnf("leaderPBAForRange failed: %v", err)
					return
				}
				leaderPbaSrc = pbaSrc
				logBlockLength = pbaBytes / uint64(BLOCK_UNIT)
			}

			req := AppendEntriesRequest{
				RPCMessage:     RPCMessage{Term: term},
				LeaderId:       leaderId,
				PrevLogIndex:   prevLogIndex,
				PrevLogTerm:    prevLogTerm,
				LeaderCommit:   leaderCommit,
				LeaderPbaSrc:   leaderPbaSrc,
				LogBlockLength: logBlockLength,
				NumEntries:     lenEntries,
				SlotsPerEntry:  slotsPerEntry,
				StartSlot:      startSlot,
			}
			s.mu.Unlock()

			var rsp AppendEntriesResponse
			if !s.rpcCall(fi, "Server.HandleAppendEntriesRequest", req, &rsp) {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}
			if rsp.Term != req.Term && s.state == leaderState {
				return
			}

			if rsp.Success {
				newNext := maxUint64(req.PrevLogIndex+lenEntries+1, 1)
				if newNext > s.cluster[fi].nextIndex {
					s.cluster[fi].nextIndex = newNext
				}
				s.cluster[fi].matchIndex = s.cluster[fi].nextIndex - 1
			} else {
				s.cluster[fi].nextIndex = maxUint64(s.cluster[fi].nextIndex-1, 1)
				s.debugf("Back off to %d for %d.", s.cluster[fi].nextIndex, s.cluster[fi].Id)
			}
		}(i)
	}
}

// ============================================================
// Election / Leadership
// ============================================================
func (s *Server) resetElectionTimeout() {
	interval := time.Duration(rand.Intn(s.HeartbeatMs*2) + s.HeartbeatMs*2)
	s.electionTimeout = time.Now().Add(interval * time.Millisecond)
}

func (s *Server) timeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if time.Now().After(s.electionTimeout) {
		s.debug("Timed out, starting new election.")
		s.state = candidateState
		s.currentTerm++
		for i := range s.cluster {
			if i == s.clusterIndex {
				s.cluster[i].votedFor = s.id
			} else {
				s.cluster[i].votedFor = 0
			}
		}
		s.resetElectionTimeout()
		s.persistCircular(false, 0)
		s.requestVote()
	}
}

func (s *Server) becomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	quorum := len(s.cluster)/2 + 1
	for i := range s.cluster {
		if s.cluster[i].votedFor == s.id && quorum > 0 {
			quorum--
		}
	}

	if quorum == 0 {
		s.debug("New leader.")
		s.state = leaderState
		s.initSlotStates()

		// No-op entry (Raft paper Section 8)
		s.log = append(s.log, Entry{Term: s.currentTerm, Command: nil})
		s.tailLogIndex++
		s.persistCircular(true, 1)

		for i := range s.cluster {
			s.cluster[i].nextIndex = s.tailLogIndex - 1
			s.cluster[i].matchIndex = 0
		}
		s.heartbeatTimeout = time.Now()
	}
}

func (s *Server) heartbeat() {
	s.mu.Lock()
	if time.Now().Before(s.heartbeatTimeout) {
		s.mu.Unlock()
		return
	}
	s.heartbeatTimeout = time.Now().Add(time.Duration(s.HeartbeatMs) * time.Millisecond)
	s.mu.Unlock()
	s.appendEntries()
}

func (s *Server) Start() {
	s.mu.Lock()
	s.state = followerState
	s.done = false
	s.mu.Unlock()

	s.restoreCircular()

	rpcServer := rpc.NewServer()
	rpcServer.Register(s)
	l, err := net.Listen("tcp", s.address)
	//l, err := rdmacm.Listen(s.address)
	if err != nil {
		panic(err)
	}
	fmt.Println("LISTENING:", s.address)

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)
	s.server = &http.Server{Handler: mux}
	go s.server.Serve(l)

	go func() {
		s.mu.Lock()
		s.resetElectionTimeout()
		s.mu.Unlock()

		for {
			s.mu.Lock()
			if s.done {
				s.mu.Unlock()
				return
			}
			st := s.state
			s.mu.Unlock()

			switch st {
			case leaderState:
				s.heartbeat()
				s.advanceCommitIndex()
			case followerState:
				s.timeout()
				s.advanceCommitIndex()
			case candidateState:
				s.timeout()
				s.becomeLeader()
			}
		}
	}()
}

// ============================================================
// PBA helpers (storage-direct versions, inlined from pba_helper.go)
// ============================================================

// leaderPBAForRange resolves the physical block address for a contiguous range
// of ring buffer slots starting at `startSlot` for `totalSlots` slots.
// Uses FIEMAP to map the metadata file offset to a device PBA.
//
// Must be called with s.mu held.
func (s *Server) leaderPBAForRange(startSlot, totalSlots uint64) (pbaSrc uint64, nbytes uint64, err error) {
	if totalSlots == 0 {
		return 0, 0, nil
	}

	logicalOff := int64(RING_OFFSET) + int64(startSlot)*BLOCK_UNIT

	rawBytes := totalSlots * uint64(BLOCK_UNIT)
	nbytes = blockcopy.AlignUp(rawBytes)

	metaPath := path.Join(s.metadataDir, s.Metadata())
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
	logicalOff := int64(RING_OFFSET) + int64(s.tailSlot)*BLOCK_UNIT

	metaPath := path.Join(s.metadataDir, s.Metadata())
	seg, err := blockcopy.L_get_pba(metaPath, logicalOff, nbytes)
	if err != nil {
		return 0, fmt.Errorf("followerPBAForNext(tailSlot=%d): %v", s.tailSlot, err)
	}

	return seg.PBA + s.partitionOffsetBytes, nil
}

// doPBACopy performs a storage-level block copy on behalf of the follower,
// bypassing the filesystem entirely.
//
// Flow:
//  1. Resolve follower's destination PBA at dstSlot via FIEMAP
//  2. Call R_write_pba(device, leaderPbaSrc, followerPbaDst, nbytes)
//     — storage node copies the block at the device level, no fd.WriteAt involved
//
// Must be called with s.mu held.
func (s *Server) doPBACopy(leaderPbaSrc, logBlockLength, dstSlot uint64) error {
	if leaderPbaSrc == 0 || logBlockLength == 0 {
		return nil
	}

	nbytes := blockcopy.AlignUp(logBlockLength * uint64(BLOCK_UNIT))

	// Resolve follower's destination PBA at dstSlot via FIEMAP
	logicalOff := int64(RING_OFFSET) + int64(dstSlot)*BLOCK_UNIT
	metaPath := path.Join(s.metadataDir, s.Metadata())
	seg, err := blockcopy.L_get_pba(metaPath, logicalOff, nbytes)
	if err != nil {
		return fmt.Errorf("doPBACopy: L_get_pba(slot=%d): %v", dstSlot, err)
	}
	followerPbaDst := seg.PBA + s.partitionOffsetBytes

	// Device-to-device block copy: bypasses filesystem, no fd.WriteAt
	if err := blockcopy.R_write_pba(s.devicePath, leaderPbaSrc, followerPbaDst, nbytes); err != nil {
		return fmt.Errorf("doPBACopy: R_write_pba(src=0x%X, dst=0x%X, nbytes=%d): %v",
			leaderPbaSrc, followerPbaDst, nbytes, err)
	}

	s.debugf("[PBA COPY] src=0x%X -> dst=0x%X nbytes=%d blocks=%d",
		leaderPbaSrc, followerPbaDst, nbytes, logBlockLength)
	return nil
}

// readEntryDirect reads one entry from the device using O_DIRECT,
// bypassing page cache entirely. Used after doPBACopy on follower.
func (s *Server) readEntryDirect(headerSlot uint64) (Entry, uint64, error) {
	metaPath := path.Join(s.metadataDir, s.Metadata())

	logicalOff := int64(RING_OFFSET) + int64(headerSlot)*BLOCK_UNIT
	seg, err := blockcopy.L_get_pba(metaPath, logicalOff, uint64(BLOCK_UNIT))
	if err != nil {
		return Entry{}, 0, fmt.Errorf("readEntryDirect: L_get_pba slot %d: %v", headerSlot, err)
	}
	rawPBA := seg.PBA + s.partitionOffsetBytes

	// Align PBA down to 4KB boundary for O_DIRECT
	alignedPBA := rawPBA &^ (PAGE_SIZE - 1)
	offsetInPage := int64(rawPBA - alignedPBA)

	buf, err := blockcopy.DirectRead(s.devicePath, alignedPBA, PAGE_SIZE)
	if err != nil {
		return Entry{}, 0, fmt.Errorf("readEntryDirect: DirectRead header slot %d pba=0x%X: %v", headerSlot, alignedPBA, err)
	}

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
