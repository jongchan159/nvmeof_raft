//go:build raft
// +build raft

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
	"path/filepath"
	"sync"
	"time"
)

// Assert checks if two values are equal (Go 1.10 compatible - no generics)
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
	// Entries      []Entry
	LeaderCommit uint64

	// Raw Block Copy Metadata
	LeaderPbaSrc   uint64
	LogBlockLength uint64
}

// Raw Block Copy Struct
type LogEntryState uint8

const (
	// 1) requested and appended from client
	LeaderAppended LogEntryState = iota

	// 2) send to followers and still replicating
	LeaderReplicating

	// 3) achieve quroum and complete replication (commit)
	LeaderCommitted
)

func (s LogEntryState) String() string {
	switch s {
	case LeaderAppended:
		return "APPENDED"
	case LeaderReplicating:
		return "REPLICATING"
	case LeaderCommitted:
		return "COMMITTED"
	default:
		return "UNALLOCATED"
	}
}

type AppendEntriesResponse struct {
	RPCMessage
	Success bool
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

type Server struct {
	done   bool
	server *http.Server
	Debug  bool

	mu          sync.Mutex
	currentTerm uint64
	log         []Entry // log[0] is sentinel node for ring buffer

	id               uint64
	address          string
	electionTimeout  time.Time
	heartbeatMs      int
	heartbeatTimeout time.Time
	statemachine     StateMachine
	metadataDir      string
	fd               *os.File

	commitIndex  uint64
	lastApplied  uint64
	state        ServerState
	cluster      []ClusterMember
	clusterIndex int

	// Raw Block Copy Variables
	nextPbaStart   uint64
	logBlockLength uint64
	logBasePba     uint64

	// Circular log fields
	// LogIndex -> Logical Index of Raft's Log (absoulte index -> increment only)
	// Slot -> Physical Index of Disk (ring buffer -> wrap-around)
	headLogIndex uint64 // logical index of oldest live entry
	tailLogIndex uint64 // next logical index to write (== len(log))
	headSlot     uint64 // ring slot number of headLogIndex
	tailSlot     uint64 // next ring slot to write
}

// minUint64 returns minimum of two uint64 (Go 1.10 compatible)
func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// maxUint64 returns maximum of two uint64 (Go 1.10 compatible)
func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// maxInt returns maximum of two int (Go 1.10 compatible)
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [Id: %d, Term: %d] %s", time.Now().Format(time.RFC3339Nano), s.id, s.currentTerm, msg)
}

func (s *Server) debug(msg string) {
	if !s.Debug {
		return
	}
	fmt.Println(s.debugmsg(msg))
}

func (s *Server) debugf(msg string, args ...interface{}) {
	if !s.Debug {
		return
	}
	s.debug(fmt.Sprintf(msg, args...))
}

func (s *Server) warn(msg string) {
	fmt.Println("[WARN] " + s.debugmsg(msg))
}

func (s *Server) warnf(msg string, args ...interface{}) {
	fmt.Println(fmt.Sprintf(msg, args...))
}

func Server_assert(s *Server, msg string, a, b interface{}) {
	Assert(s.debugmsg(msg), a, b)
}

func NewServer(
	clusterConfig []ClusterMember,
	statemachine StateMachine,
	metadataDir string,
	clusterIndex int,
) *Server {
	var cluster []ClusterMember
	for _, c := range clusterConfig {
		if c.Id == 0 {
			panic("Id must not be 0.")
		}
		cluster = append(cluster, c)
	}

	return &Server{
		id:           cluster[clusterIndex].Id,
		address:      cluster[clusterIndex].Address,
		cluster:      cluster,
		statemachine: statemachine,
		metadataDir:  metadataDir,
		clusterIndex: clusterIndex,
		heartbeatMs:  300,
		mu:           sync.Mutex{},
	}
}

// Constant for circular log entries
const BLOCK_UNIT = 512 // 섹터 크기 (bytes)
const PAGE_SIZE = 4096 // 페이지 크기 (bytes)

const SLOTS_PER_PAGE = PAGE_SIZE / BLOCK_UNIT  // 8 슬롯 = 1페이지
const NUM_PAGES = 4                            // 4페이지 = 16KB
const TOTAL_SLOTS = NUM_PAGES * SLOTS_PER_PAGE // 32슬롯
const RING_SLOTS = TOTAL_SLOTS - 1             // 31슬롯 (링버퍼용)
const HEADER_SIZE = BLOCK_UNIT                 // 512B
const RING_OFFSET = HEADER_SIZE                // 512 (헤더 다음부터)

// 파일 총 크기 = TOTAL_SLOTS * BLOCK_UNIT
//             = 32 * 512 = 16KB  ← 4KB 배수 확인

// Header slot layout (512B):
// [ 0: 7] term     (8B)
// [ 8:15] cmdLen   (8B)
// [16:23] numSlots (8B)  total slots this entry occupies (1 + payload slots)
// [24:511] padding (488B)
//
// Payload slot layout (512B * n):
// [0:511] command bytes, last slot zero-padded

// ============================================================
// Sentinel design:
//   s.log[0] is always the sentinel entry (Term=0, Command=nil).
//   Sentinel is NEVER written to the ring buffer on disk.
//   headLogIndex counts real (non-sentinel) entries discarded.
//   Mapping: s.log[1 + (logIdx - headLogIndex)] for logIdx >= headLogIndex
//            s.log[0] always == sentinel
// ============================================================

// ============================================================
// Helper functions
// ============================================================

// slotsForEntry returns 1 (header) + n (payload) slots needed.
func slotsForEntry(cmdLen int) uint64 {
	payloadSlots := uint64((cmdLen + BLOCK_UNIT - 1) / BLOCK_UNIT)
	return 1 + payloadSlots
}

// slotOffset returns the file byte offset for a given slot number.
func slotOffset(slot uint64) int64 {
	return int64(RING_OFFSET) + int64(slot)*BLOCK_UNIT
}

// usedSlots returns the number of currently occupied slots.
func (s *Server) usedSlots() uint64 {
	return (s.tailSlot - s.headSlot + RING_SLOTS) % RING_SLOTS
}

// freeSlots returns the number of available slots.
func (s *Server) freeSlots() uint64 {
	return RING_SLOTS - s.usedSlots()
}

// logSlice converts an absolute logical index to s.log slice index.
// Sentinel is at s.log[0]; real entries start at s.log[1].
// logIdx must be >= headLogIndex.
func (s *Server) logSlice(logIdx uint64) uint64 {
	return 1 + (logIdx - s.headLogIndex)
}

// Head management functions
// advanceHead discards the oldest real entry from the ring buffer.
// Invariants maintained:
//   - headLogIndex is absolute and never resets
//   - s.log[0] is always the sentinel
//   - s.log[1] is the entry at logical index headLogIndex (before call)
//   - s.log[1 + (logIdx - headLogIndex)] maps logIdx after call
func (s *Server) advanceHead() {
	if s.headLogIndex >= s.tailLogIndex {
		panic("advanceHead: ring buffer is already empty")
	}

	// Read numSlots from header only (no full entry reconstruction needed)
	var header [BLOCK_UNIT]byte
	s.fd.Seek(slotOffset(s.headSlot), 0)
	s.fd.Read(header[:])
	numSlots := binary.LittleEndian.Uint64(header[16:]) // jump head all the slots of a log

	// Advance disk ring pointer
	s.headSlot = (s.headSlot + numSlots) % RING_SLOTS
	s.headLogIndex++

	// Trim s.log: drop s.log[1] (oldest real entry), keep s.log[0] (sentinel).
	// Before: s.log = [sentinel, oldest, ..., newest]
	// After:  s.log = [sentinel, 2nd-oldest, ..., newest]
	if len(s.log) > 1 {
		s.log = append(s.log[:1], s.log[2:]...)
	}
}

// ============================================================
// Ring buffer I/O
// ============================================================
// writeEntryToRing writes one entry (except for sentinel one) to the ring buffer.
// Advances head if not enough space.
func (s *Server) writeEntryToRing(e Entry) {
	needed := slotsForEntry(len(e.Command)) // needed slot to write on ring buffer

	// Reclaim oldest entries until enough space is available
	for needed > s.freeSlots() {
		s.advanceHead()
		// TODO: preserve old entries such as snapshot or compaction
	}

	// Write header slot
	var header [BLOCK_UNIT]byte
	binary.LittleEndian.PutUint64(header[0:], e.Term)                 // tern
	binary.LittleEndian.PutUint64(header[8:], uint64(len(e.Command))) // cmdLen
	binary.LittleEndian.PutUint64(header[16:], needed)                // numSlots
	// header[24:] remains zero padding
	s.fd.Seek(slotOffset(s.tailSlot), 0)
	s.fd.Write(header[:])

	// Write payload slots
	cmdWritten := 0
	for i := uint64(1); i < needed; i++ {
		slot := (s.tailSlot + i) % RING_SLOTS // allow wrap-around
		var payload [BLOCK_UNIT]byte
		n := copy(payload[:], e.Command[cmdWritten:])
		cmdWritten += n
		// payload[n:] remains zero padding
		s.fd.Seek(slotOffset(slot), 0)
		s.fd.Write(payload[:])
	}

	// update tail pointer
	s.tailSlot = (s.tailSlot + needed) % RING_SLOTS
	//s.tailLogIndex++
}

// readEntryFromSlot reads one entry starting at headerSlot.
// Returns the entry and the next slot after it.
func (s *Server) readEntryFromSlot(headerSlot uint64) (Entry, uint64) {
	// Read header slot
	var header [BLOCK_UNIT]byte
	s.fd.Seek(slotOffset(headerSlot), 0)
	s.fd.Read(header[:])

	term := binary.LittleEndian.Uint64(header[0:])
	cmdLen := binary.LittleEndian.Uint64(header[8:])
	numSlots := binary.LittleEndian.Uint64(header[16:])

	// Read payload slots
	cmd := make([]byte, cmdLen)
	copied := 0
	for i := uint64(1); i < numSlots; i++ {
		slot := (headerSlot + i) % RING_SLOTS
		var payload [BLOCK_UNIT]byte
		s.fd.Seek(slotOffset(slot), 0)
		s.fd.Read(payload[:])
		copied += copy(cmd[copied:], payload[:])
	}

	nextSlot := (headerSlot + numSlots) % RING_SLOTS
	return Entry{Term: term, Command: cmd}, nextSlot
}

// ============================================================
// Persist / Restore
// ============================================================
// persistCircular writes new entries to the ring buffer and updates the header.
// Sentinel (s.log[0]) is never written to disk.
// Header block layout:
//
//	[ 0: 7] currentTerm
//	[ 8:15] votedFor
//	[16:23] headLogIndex
//	[24:31] tailLogIndex
//	[32:39] headSlot
//	[40:47] tailSlot
//	[48:55] commitIndex
//	[56:63] lastApplied
func (s *Server) persistCircular(writeLog bool, nNewEntries int) {
	t := time.Now()

	if nNewEntries == 0 && writeLog {
		nNewEntries = len(s.log) - 1 // sentinel
	}

	// Write new real entries to ring buffer (skip sentinel at s.log[0])
	if writeLog && nNewEntries > 0 {
		start := len(s.log) - nNewEntries
		if start < 1 {
			start = 1 // do not write sentinel (s.log[0])
		}
		for i := start; i < len(s.log); i++ {
			s.writeEntryToRing(s.log[i])
		}

		// update tailLogIndex only here
		s.tailLogIndex = s.headLogIndex + uint64(len(s.log)-1)
	}

	// Write header block
	s.fd.Seek(0, 0)
	var header [HEADER_SIZE]byte // 512
	binary.LittleEndian.PutUint64(header[0:], s.currentTerm)
	binary.LittleEndian.PutUint64(header[8:], s.getVotedFor())
	binary.LittleEndian.PutUint64(header[16:], s.headLogIndex)
	binary.LittleEndian.PutUint64(header[24:], s.tailLogIndex)
	binary.LittleEndian.PutUint64(header[32:], s.headSlot)
	binary.LittleEndian.PutUint64(header[40:], s.tailSlot)
	binary.LittleEndian.PutUint64(header[48:], s.commitIndex)
	binary.LittleEndian.PutUint64(header[56:], s.lastApplied)
	// header[64:511] -> reservation (zero-padding)

	if _, err := s.fd.Write(header[:]); err != nil {
		panic(err)
	}
	if err := s.fd.Sync(); err != nil {
		panic(err)
	}

	s.debugf("Persisted in %s. Term: %d. LogLen: %d (%d new). VotedFor: %d.",
		time.Now().Sub(t), s.currentTerm, len(s.log), nNewEntries, s.getVotedFor())
}

// restoreCircular reads the header and replays the ring buffer into s.log.
// s.log[0] is always the sentinel after restore.
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
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0644,
		)
		if err != nil {
			panic(err)
		}
	}

	s.fd.Seek(0, 0)
	var header [HEADER_SIZE]byte
	n, err := io.ReadFull(s.fd, header[:])

	// Empty or corrupted: initialize fresh state
	if err == io.EOF || err == io.ErrUnexpectedEOF || n < HEADER_SIZE {
		s.currentTerm = 0
		s.setVotedFor(0)
		s.headLogIndex = 0
		s.tailLogIndex = 0
		s.headSlot = 0
		s.tailSlot = 0
		s.commitIndex = 0
		s.lastApplied = 0
		s.log = nil
		s.ensureLog() // insert sentinel
		return
	}
	if err != nil {
		panic(err)
	}

	s.currentTerm = binary.LittleEndian.Uint64(header[0:])
	s.setVotedFor(binary.LittleEndian.Uint64(header[8:]))
	s.headLogIndex = binary.LittleEndian.Uint64(header[16:])
	s.tailLogIndex = binary.LittleEndian.Uint64(header[24:])
	s.headSlot = binary.LittleEndian.Uint64(header[32:])
	s.tailSlot = binary.LittleEndian.Uint64(header[40:])
	s.commitIndex = binary.LittleEndian.Uint64(header[48:])
	s.lastApplied = binary.LittleEndian.Uint64(header[56:])

	// Rebuild s.log: sentinel first, then real entries from ring buffer
	s.log = nil
	s.ensureLog() // s.log[0] = sentinel

	// ring buffer replay: head -> tail
	slot := s.headSlot
	for idx := s.headLogIndex; idx < s.tailLogIndex; idx++ {
		e, nextSlot := s.readEntryFromSlot(slot)
		s.log = append(s.log, e)
		slot = nextSlot
	}

	// Verify invariant
	expected := s.tailLogIndex - s.headLogIndex
	actual := uint64(len(s.log) - 1) // subtract sentinel
	if actual != expected {
		panic(fmt.Sprintf("restoreCircular: invariant violated: expected %d real entries, got %d", expected, actual))
	}
}

// ============================================================
// advanceCommitIndex
// (1) Leader: raise commitIndex based quorum
// (2) Apply the committed entries to state machine
// ============================================================
func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Raise commitIndex
	if s.state == leaderState {
		// tailLogIndex-1 == absolute index of last real entry
		lastLogIndex := s.tailLogIndex - 1
		for i := lastLogIndex; i > s.commitIndex; i-- { // check in reverse order
			quorum := len(s.cluster)/2 + 1
			for j := range s.cluster {
				if quorum == 0 {
					break
				}
				isLeader := j == s.clusterIndex
				if isLeader || s.cluster[j].matchIndex >= i {
					quorum--
				}
			}
			if quorum == 0 {
				s.commitIndex = i
				s.debugf("New commit index: %d.", i)
				break
			}
		}
	}

	// Apply to state machine
	if s.lastApplied >= s.headLogIndex &&
		s.lastApplied < s.tailLogIndex &&
		s.lastApplied < s.commitIndex {
		// logSlice(): absolute logIdx → s.log slice index
		logEntry := s.log[s.logSlice(s.lastApplied)]

		if len(logEntry.Command) > 0 {
			s.debugf("Entry applied: %d.", s.lastApplied)
			res, err := s.statemachine.Apply(logEntry.Command)
			if logEntry.result != nil {
				logEntry.result <- ApplyResult{Result: res, Error: err}
			}
		}
		s.lastApplied++

		// Advance head up to minMatchIndex (all followers have replicated)
		// need snapshot to consider the dead node
		minMatch := s.commitIndex // slowest follower
		for j := range s.cluster {
			if j == s.clusterIndex {
				continue
			}
			if s.cluster[j].matchIndex < minMatch {
				minMatch = s.cluster[j].matchIndex
			}
		}
		for s.headLogIndex < minMatch {
			s.advanceHead()
		}
	}
}

/*
func (s *Server) persist(writeLog bool, nNewEntries int) {
	t := time.Now()
	if nNewEntries == 0 && writeLog {
		nNewEntries = len(s.log)
	}
	s.fd.Seek(0, 0)
	var page [PAGE_SIZE]byte
	binary.LittleEndian.PutUint64(page[:8], s.currentTerm)
	binary.LittleEndian.PutUint64(page[8:16], s.getVotedFor())
	binary.LittleEndian.PutUint64(page[16:24], uint64(len(s.log)))
	n, err := s.fd.Write(page[:])
	if err != nil {
		panic(err)
	}
	Server_assert(s, "Wrote full page", n, PAGE_SIZE)
	if writeLog && nNewEntries > 0 {
		newLogOffset := maxInt(len(s.log)-nNewEntries, 0)
		s.fd.Seek(int64(PAGE_SIZE+ENTRY_SIZE*newLogOffset), 0)
		bw := bufio.NewWriter(s.fd)
		var entryBytes [ENTRY_SIZE]byte
		for i := newLogOffset; i < len(s.log); i++ {
			if len(s.log[i].Command) > ENTRY_SIZE-ENTRY_HEADER {
				panic(fmt.Sprintf("Command is too large (%d). Must be at most %d bytes.", len(s.log[i].Command), ENTRY_SIZE-ENTRY_HEADER))
			}
			binary.LittleEndian.PutUint64(entryBytes[:8], s.log[i].Term)
			binary.LittleEndian.PutUint64(entryBytes[8:16], uint64(len(s.log[i].Command)))
			copy(entryBytes[16:], []byte(s.log[i].Command))
			n, err := bw.Write(entryBytes[:])
			if err != nil {
				panic(err)
			}
			Server_assert(s, "Wrote full page", n, ENTRY_SIZE)
		}
		err = bw.Flush()
		if err != nil {
			panic(err)
		}
	}
	if err = s.fd.Sync(); err != nil {
		panic(err)
	}
	s.debugf("Persisted in %s. Term: %d. Log Len: %d (%d new). Voted For: %d.", time.Now().Sub(t), s.currentTerm, len(s.log), nNewEntries, s.getVotedFor())
}*/

func (s *Server) ensureLog() {
	if len(s.log) == 0 {
		s.log = append(s.log, Entry{})
	}
}

func (s *Server) setVotedFor(id uint64) {
	for i := range s.cluster {
		if i == s.clusterIndex {
			s.cluster[i].votedFor = id
			return
		}
	}
	Server_assert(s, "Invalid cluster", true, false)
}

func (s *Server) getVotedFor() uint64 {
	for i := range s.cluster {
		if i == s.clusterIndex {
			return s.cluster[i].votedFor
		}
	}
	Server_assert(s, "Invalid cluster", true, false)
	return 0
}

func (s *Server) Metadata() string {
	return fmt.Sprintf("md_%d.dat", s.id)
}

/*
func (s *Server) restore() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fd == nil {
		if err := os.MkdirAll(s.metadataDir, 0755); err != nil {
			panic(err)
		}

		var err error
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, s.Metadata()),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0644,
		)
		if err != nil {
			panic(err)
		}
	}

	if _, err := s.fd.Seek(0, 0); err != nil {
		panic(err)
	}

	var page [PAGE_SIZE]byte
	n, err := io.ReadFull(s.fd, page[:])

	// File is empty or corrupted - initialize
	if err == io.EOF || err == io.ErrUnexpectedEOF || n < PAGE_SIZE {
		s.currentTerm = 0
		s.setVotedFor(0)
		s.log = nil
		s.ensureLog()
		return
	}
	if err != nil {
		panic(err)
	}

	s.currentTerm = binary.LittleEndian.Uint64(page[:8])
	s.setVotedFor(binary.LittleEndian.Uint64(page[8:16]))
	lenLog := binary.LittleEndian.Uint64(page[16:24])

	s.log = nil
	if lenLog > 0 {
		if _, err := s.fd.Seek(int64(PAGE_SIZE), 0); err != nil {
			panic(err)
		}
		for i := uint64(0); i < lenLog; i++ {
			var entryBytes [ENTRY_SIZE]byte
			_, err := io.ReadFull(s.fd, entryBytes[:])

			// Entry is incomplete - treat as corrupted file
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				s.currentTerm = 0
				s.setVotedFor(0)
				s.log = nil
				s.ensureLog()
				return
			}
			if err != nil {
				panic(err)
			}

			term := binary.LittleEndian.Uint64(entryBytes[:8])
			l := binary.LittleEndian.Uint64(entryBytes[8:16])
			if l > ENTRY_SIZE-ENTRY_HEADER {
				// Invalid command length - corrupted file
				s.currentTerm = 0
				s.setVotedFor(0)
				s.log = nil
				s.ensureLog()
				return
			}

			cmd := make([]byte, l)
			copy(cmd, entryBytes[16:16+l])

			s.log = append(s.log, Entry{Term: term, Command: cmd})
		}
	}

	s.ensureLog()
}*/

func (s *Server) requestVote() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}
		go func(i int) {
			s.mu.Lock()
			s.debugf("Requesting vote from %d.", s.cluster[i].Id)
			lastLogIndex := s.tailLogIndex - 1
			lastLogTerm := s.log[s.logSlice(lastLogIndex)].Term
			req := RequestVoteRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,
				},
				CandidateId:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			s.mu.Unlock()

			var rsp RequestVoteResponse
			ok := s.rpcCall(i, "Server.HandleRequestVoteRequest", req, &rsp)
			if !ok {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			dropStaleResponse := rsp.Term != req.Term
			if dropStaleResponse {
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

	s.debugf("Received vote request from %d.", req.CandidateId)
	rsp.VoteGranted = false
	rsp.Term = s.currentTerm

	if req.Term < s.currentTerm {
		s.debugf("Not granting vote request from %d.", req.CandidateId)
		Server_assert(s, "VoteGranted = false", rsp.VoteGranted, false)
		return nil
	}

	lastLogTerm := s.log[s.logSlice(s.tailLogIndex-1)].Term
	logLen := s.tailLogIndex - 1

	logOk := req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= logLen)

	grant := req.Term == s.currentTerm && logOk && (s.getVotedFor() == 0 || s.getVotedFor() == req.CandidateId)

	if grant {
		s.debugf("Voted for %d.", req.CandidateId)
		s.setVotedFor(req.CandidateId)
		rsp.VoteGranted = true
		s.resetElectionTimeout()
		s.persistCircular(false, 0)
	} else {
		s.debugf("Not granting vote request from %d.", +req.CandidateId)
	}

	return nil
}

func (s *Server) updateTerm(msg RPCMessage) bool {
	transitioned := false
	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.state = followerState
		s.setVotedFor(0)
		transitioned = true
		s.debug("Transitioned to follower")
		s.resetElectionTimeout()
		s.persistCircular(false, 0)
	}
	return transitioned
}

func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	s.debugf("[RECV AppendEntries] from leader=%d term=%d prev=(%d,%d) commit=%d pba=%d blocks=%d",
		req.LeaderId, req.Term, req.PrevLogIndex, req.PrevLogTerm, req.LeaderCommit, req.LeaderPbaSrc, req.LogBlockLength)

	if req.Term == s.currentTerm && s.state == candidateState {
		s.state = followerState
	}

	rsp.Term = s.currentTerm
	rsp.Success = false

	if s.state != followerState {
		s.debugf("Non-follower cannot append entries.")
		return nil
	}

	if req.Term < s.currentTerm {
		s.debugf("Dropping request from old leader %d: term %d.", req.LeaderId, req.Term)
		return nil
	}

	s.resetElectionTimeout()

	if req.LeaderCommit > s.commitIndex {
		// Storage-level block copy: data never crosses the network
		if err := s.doPBACopy(req.LeaderPbaSrc, req.LogBlockLength); err != nil {
			s.warnf("doPBACopy failed: %v", err)
			rsp.Success = false
			return nil
		}

		// if err := s.syncFromNVMe(); err != nil {
		// 	s.warnf("syncFromNVMe failed: %v", err)
		// }
		s.commitIndex = minUint64(req.LeaderCommit, s.tailLogIndex-1)
	}

	rsp.Success = true
	return nil
}

var ErrApplyToLeader = errors.New("Cannot apply message to follower, apply to leader.")

func (s *Server) Apply(commands [][]byte) ([]ApplyResult, error) {
	s.mu.Lock()
	if s.state != leaderState {
		s.mu.Unlock()
		return nil, ErrApplyToLeader
	}

	s.debugf("Processing %d new entry!", len(commands))

	resultChans := make([]chan ApplyResult, len(commands))
	for i, command := range commands {
		resultChans[i] = make(chan ApplyResult)
		s.log = append(s.log, Entry{
			Term:    s.currentTerm,
			Command: command,
			result:  resultChans[i],
		})
	}

	s.persistCircular(true, len(commands))
	s.debug("Waiting to be applied!")

	s.mu.Unlock()

	s.appendEntries()

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

func nvmeBaseDir(id uint64) string {
	switch id {
	case 4:
		return "/mnt/ocfs2for4"
	case 6:
		return "/mnt/ocfs2for6"
	case 9:
		return "/mnt/ocfs2for9"
	default:
		return "/mnt/ocfs2for4"
	}
}

/*
func appendToFollowerNVMe(followerID uint64, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	targetPath := filepath.Join(nvmeBaseDir(followerID), "raft.log")
	f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)

	for _, e := range entries {
		if err := binary.Write(bw, binary.LittleEndian, e.Term); err != nil {
			return err
		}
		if err := binary.Write(bw, binary.LittleEndian, uint64(len(e.Command))); err != nil {
			return err
		}
		if _, err := bw.Write(e.Command); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}*/

func (s *Server) syncFromNVMe() error {
	myPath := filepath.Join(nvmeBaseDir(s.id), "raft.log")
	f, err := os.Open(myPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	rd := bufio.NewReader(f)

	curIndex := uint64(len(s.log))
	var count uint64 = 0
	for {
		var term uint64
		if err := binary.Read(rd, binary.LittleEndian, &term); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		var l uint64
		if err := binary.Read(rd, binary.LittleEndian, &l); err != nil {
			return err
		}

		buf := make([]byte, l)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return err
		}

		count++
		if count >= curIndex {
			s.log = append(s.log, Entry{
				Term:    term,
				Command: buf,
				result:  nil,
			})
		}
	}
	return nil
}

func (s *Server) rpcCall(i int, name string, req, rsp interface{}) bool {
	s.mu.Lock()
	if s.cluster[i].rpcClient == nil {
		client, err := rpc.DialHTTP("tcp", s.cluster[i].Address)
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
		// Clear the client on error to force reconnection next time
		s.mu.Lock()
		s.cluster[i].rpcClient = nil
		s.mu.Unlock()
		return false
	}
	return true
}

const MAX_APPEND_ENTRIES_BATCH = 8000

func (s *Server) appendEntries() {
	s.debugf("LEADER appendEntries() called")

	s.mu.Lock()
	if s.state != leaderState {
		s.debugf("LEADER is not leaderState, early Return")
		s.mu.Unlock()
		return
	}
	term := s.currentTerm
	leaderId := s.cluster[s.clusterIndex].Id
	leaderCommit := s.commitIndex
	s.mu.Unlock()

	s.debugf("appendEntries: clusterSize=%d selfIndex=%d", len(s.cluster), s.clusterIndex)
	for i := range s.cluster {
		s.debugf("appendEntries: spawn goroutine to idx=%d id=%d addr=%s", i, s.cluster[i].Id, s.cluster[i].Address)
		if i == s.clusterIndex {
			continue
		}
		go func(followerIdx int) {
			s.mu.Lock()

			next := s.cluster[followerIdx].nextIndex
			last := s.tailLogIndex - 1

			// Guard: fix nextIndex if out of bounds
			if next > last+1 {
				s.debugf("Fixing nextIndex for %d: %d -> %d (last=%d)", s.cluster[followerIdx].Id, next, last+1, last)
				next = last + 1
				s.cluster[followerIdx].nextIndex = next
			}

			prevLogIndex := next - 1
			var prevLogTerm uint64
			if prevLogIndex >= s.headLogIndex {
				prevLogTerm = s.log[s.logSlice(prevLogIndex)].Term
			} else {
				// prevLogIndex가 이미 링버퍼에서 삭제된 경우
				// slow follower가 너무 뒤처진 것 → 별도 처리 필요 (현재는 0으로 fallback)
				// polarFS의 스냅샷 방식 가능 -> 하지만 지금 단계에서 구현 X
				prevLogTerm = 0
			}

			var lenEntries uint64 = 0
			if last >= next {
				remain := last - next + 1
				lenEntries = remain
				if lenEntries > MAX_APPEND_ENTRIES_BATCH {
					lenEntries = MAX_APPEND_ENTRIES_BATCH
				}
			}

			bytesToCopy := lenEntries * uint64(BLOCK_UNIT)
			var logBlockLength uint64 = 0
			if bytesToCopy > 0 {
				logBlockLength = (bytesToCopy + 512 - 1) / 512
			}

			// Test dummy data
			// leaderPbaSrc := uint64(0xdeadbeef)
			// logBlockLength = uint64(5678)

			// get real pba
			var leaderPbaSrc uint64 = 0
			if lenEntries > 0 {
				pbaSrc, pbaBytes, err := s.leaderPBAForRange(next, lenEntries)
				if err != nil {
					s.mu.Unlock()
					s.warnf("leaderPBAForRange failed (follower=%d next=%d): %v",
						s.cluster[followerIdx].Id, next, err)
					return
				}
				leaderPbaSrc = pbaSrc
				logBlockLength = pbaBytes / uint64(BLOCK_UNIT)
			}

			req := AppendEntriesRequest{
				RPCMessage:   RPCMessage{Term: term},
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				// Entries:       nil,
				LeaderCommit: leaderCommit,

				LeaderPbaSrc:   leaderPbaSrc,
				LogBlockLength: logBlockLength,
			}

			s.mu.Unlock()

			var rsp AppendEntriesResponse
			s.debugf("Control RPC to %d term %d.", s.cluster[followerIdx].Id, req.Term)
			ok := s.rpcCall(followerIdx, "Server.HandleAppendEntriesRequest", req, &rsp)
			if !ok {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			dropStaleResponse := rsp.Term != req.Term && s.state == leaderState
			if dropStaleResponse {
				return
			}

			if rsp.Success {
				prev := s.cluster[followerIdx].nextIndex
				s.cluster[followerIdx].nextIndex = maxUint64(req.PrevLogIndex+lenEntries+1, 1)
				s.cluster[followerIdx].matchIndex = s.cluster[followerIdx].nextIndex - 1
				s.debugf("Accepted by %d. Prev: %d → Next: %d, Match: %d.",
					s.cluster[followerIdx].Id, prev, s.cluster[followerIdx].nextIndex, s.cluster[followerIdx].matchIndex)
			} else {
				s.cluster[followerIdx].nextIndex = maxUint64(s.cluster[followerIdx].nextIndex-1, 1)
				s.debugf("Back off to %d for %d.", s.cluster[followerIdx].nextIndex, s.cluster[followerIdx].Id)
			}
		}(i)
	}
}

/*
func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == leaderState {
		lastLogIndex := s.tailLogIndex - 1
		for i := lastLogIndex; i > s.commitIndex; i-- {
			quorum := len(s.cluster)/2 + 1
			for j := range s.cluster {
				if quorum == 0 {
					break
				}
				isLeader := j == s.clusterIndex
				if isLeader || s.cluster[j].matchIndex >= i {
					quorum--
				}
			}
			if quorum == 0 {
				s.commitIndex = i
				s.debugf("New commit index: %d.", i)
				break
			}
		}
	}

	if s.lastApplied < s.commitIndex {
		// Convert absolute logical index → s.log slice index
		sliceIdx := s.lastApplied - s.headLogIndex
		logEntry := s.log[sliceIdx]

		if len(logEntry.Command) > 0 {
			s.debugf("Entry applied: %d.", s.lastApplied)
			res, err := s.statemachine.Apply(logEntry.Command)
			if logEntry.result != nil {
				logEntry.result <- ApplyResult{Result: res, Error: err}
			}
		}
		s.lastApplied++

		// Advance head up to minMatchIndex across all followers
		minMatch := s.commitIndex
		for j := range s.cluster {
			if j == s.clusterIndex {
				continue
			}
			if s.cluster[j].matchIndex < minMatch {
				minMatch = s.cluster[j].matchIndex
			}
		}
		// headLogIndex is absolute, so this loop is always correct
		for s.headLogIndex < minMatch {
			s.advanceHead()
		}
	}
}
*/

func (s *Server) resetElectionTimeout() {
	interval := time.Duration(rand.Intn(s.heartbeatMs*2) + s.heartbeatMs*2)
	s.debugf("New interval: %s.", interval*time.Millisecond)
	s.electionTimeout = time.Now().Add(interval * time.Millisecond)
}

func (s *Server) timeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	hasTimedOut := time.Now().After(s.electionTimeout)
	if hasTimedOut {
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
		for i := range s.cluster {
			s.cluster[i].nextIndex = s.tailLogIndex + 1
		}

		s.debug("New leader.")
		s.state = leaderState

		s.log = append(s.log, Entry{Term: s.currentTerm, Command: nil})
		s.persistCircular(true, 1)
		s.heartbeatTimeout = time.Now()
	}
}

func (s *Server) heartbeat() {
	s.mu.Lock()
	if time.Now().Before(s.heartbeatTimeout) {
		s.mu.Unlock()
		return
	}
	s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
	s.mu.Unlock()

	s.debug("Sending heartbeat")
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
			state := s.state
			s.mu.Unlock()

			switch state {
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

// Test helpers
func (s *Server) ForceLeaderForTest() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = leaderState
	for i := range s.cluster {
		s.cluster[i].nextIndex = s.tailLogIndex + 1
		s.cluster[i].matchIndex = 0
	}
}

func (s *Server) TriggerAppendEntriesOnceForTest() {
	s.appendEntries()
}

func (s *Server) ForceFollowerNoElectionForTest() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = followerState
	s.electionTimeout = time.Now().Add(24 * time.Hour)
}
