//go:build !raft
// +build !raft

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

	//sync "github.com/sasha-s/go-deadlock"
	"time"
	// "nvmeof_raft/blockcopy"
)

func Assert[T comparable](msg string, a, b T) {
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
	Term    uint64 // Set by the primary so it can learn about the result of
	// applying this command to the state machine
	result chan ApplyResult
}

type RPCMessage struct {
	Term uint64
}

type RequestVoteRequest struct {
	RPCMessage
	// Candidate requesting vote
	CandidateId uint64
	// Index of candidate's last log entry
	LastLogIndex uint64
	// Term of candidate's last log entry
	LastLogTerm uint64
}

type RequestVoteResponse struct {
	RPCMessage
	// True means candidate received vote
	VoteGranted bool
}

type AppendEntriesRequest struct {
	RPCMessage
	// So follower can redirect clients
	LeaderId uint64
	// Index of log entry immediately preceding new ones
	PrevLogIndex uint64
	// Term of prevLogIndex entry
	PrevLogTerm uint64
	// Log entries to store. Empty for heartbeat.
	Entries []Entry
	// Leader's commitIndex
	LeaderCommit uint64

	// Blockcopy Metadata
	// Physical block address of start point of unsent logs
	LeaderPbaSrc uint64
	// A number of blocks of unsent logs
	LogBlockCount uint64
}

type AppendEntriesResponse struct {
	RPCMessage
	// true if follower contained entry matching prevLogIndex and
	// prevLogTerm
	Success bool
}

type ClusterMember struct {
	Id      uint64
	Address string
	// Index of the next log entry to send
	nextIndex uint64
	// Highest log entry known to be replicated
	matchIndex uint64
	// Who was voted for in the most recent term
	votedFor uint64
	// TCP connection
	rpcClient *rpc.Client
}

type ServerState string

const (
	leaderState    ServerState = "leader"
	followerState              = "follower"
	candidateState             = "candidate"
)

type Server struct {
	// These variables for shutting down.
	done   bool
	server *http.Server
	Debug  bool

	mu sync.Mutex
	// ----------- PERSISTENT STATE -----------
	// The current term
	currentTerm uint64
	log         []Entry
	// votedFor is stored in cluster []ClusterMember below,
	// mapped by clusterIndex below

	// ----------- READONLY STATE -----------
	// Unique identifier for this Server
	id uint64
	// The TCP address for RPC
	address string
	// When to start elections after no append entry messages
	electionTimeout time.Time
	// How often to send empty messages
	heartbeatMs int
	// When to next send empty message
	heartbeatTimeout time.Time
	// User-provided state machine
	statemachine StateMachine
	// Metadata directory
	metadataDir string
	// Metadata store
	fd *os.File

	// ----------- VOLATILE STATE -----------
	// Index of highest log entry known to be committed
	commitIndex uint64
	// Index of highest log entry applied to state machine
	lastApplied uint64
	// Candidate, follower, or leader state
	state ServerState
	// Servers in the cluster, including this one
	cluster []ClusterMember
	// Index of this server
	clusterIndex int

	//-------- Blockcopy Raft variable --------
	// Physical block address of start point of unsent logs
	nextPbaStart uint64
	// A number of blocks of unsent logs
	logBlockCount uint64
	// Leader's base of log PBA
	logBasePba uint64
}

func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T ~int | ~uint64](a, b T) T {
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

func (s *Server) debugf(msg string, args ...any) {
	if !s.Debug {
		return
	}
	s.debug(fmt.Sprintf(msg, args...))
}

func (s *Server) warn(msg string) {
	fmt.Println("[WARN] " + s.debugmsg(msg))
}

func (s *Server) warnf(msg string, args ...any) {
	fmt.Println(fmt.Sprintf(msg, args...))
}

func Server_assert[T comparable](s *Server, msg string, a, b T) {
	Assert(s.debugmsg(msg), a, b)
}

func NewServer(
	clusterConfig []ClusterMember,
	statemachine StateMachine,
	metadataDir string,
	clusterIndex int,
) *Server {
	//sync.Opts.DeadlockTimeout = 2000 * time.Millisecond
	// Explicitly make a copy of the cluster because we'll be
	// modifying it in this server.
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

const PAGE_SIZE = 4096
const ENTRY_HEADER = 16
const ENTRY_SIZE = 128

// Weird thing to note is that writing to a deleted disk is not an
// error on Linux. So if these files are deleted, you won't know about
// that until the process restarts.
// Must be called within s.mu.Lock()
func (s *Server) persist(writeLog bool, nNewEntries int) {
	t := time.Now()
	if nNewEntries == 0 && writeLog {
		nNewEntries = len(s.log)
	}
	s.fd.Seek(0, 0)
	var page [PAGE_SIZE]byte
	// Bytes 0 - 8: Current term
	// Bytes 8 - 16: Voted for
	// Bytes 16 - 24: Log length
	// Bytes 4096 - N: Log
	binary.LittleEndian.PutUint64(page[:8], s.currentTerm)
	binary.LittleEndian.PutUint64(page[8:16], s.getVotedFor())
	binary.LittleEndian.PutUint64(page[16:24], uint64(len(s.log)))
	n, err := s.fd.Write(page[:])
	if err != nil {
		panic(err)
	}
	Server_assert(s, "Wrote full page", n, PAGE_SIZE)
	if writeLog && nNewEntries > 0 {
		newLogOffset := max(len(s.log)-nNewEntries, 0)
		s.fd.Seek(int64(PAGE_SIZE+ENTRY_SIZE*newLogOffset), 0)
		bw := bufio.NewWriter(s.fd)
		var entryBytes [ENTRY_SIZE]byte
		for i := newLogOffset; i < len(s.log); i++ {
			// Bytes 0 - 8: Entry term
			// Bytes 8 - 16: Entry command length
			// Bytes 16 - ENTRY_SIZE: Entry command
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
}

func (s *Server) ensureLog() {
	if len(s.log) == 0 {
		// Always has at least one log entry.
		s.log = append(s.log, Entry{})
	}
}

// Must be called within s.mu.Lock()
func (s *Server) setVotedFor(id uint64) {
	for i := range s.cluster {
		if i == s.clusterIndex {
			s.cluster[i].votedFor = id
			return
		}
	}
	Server_assert(s, "Invalid cluster", true, false)
}

// Must be called within s.mu.Lock()
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

func (s *Server) restore() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fd == nil {
		var err error
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, s.Metadata()),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0755)
		if err != nil {
			panic(err)
		}
	}
	s.fd.Seek(0, 0)
	// Bytes 0 - 8: Current term
	// Bytes 8 - 16: Voted for
	// Bytes 16 - 24: Log length
	// Bytes 4096 - N: Log
	var page [PAGE_SIZE]byte
	n, err := s.fd.Read(page[:])
	if err == io.EOF {
		s.ensureLog()
		return
	} else if err != nil {
		panic(err)
	}
	Server_assert(s, "Read full page", n, PAGE_SIZE)
	s.currentTerm = binary.LittleEndian.Uint64(page[:8])
	s.setVotedFor(binary.LittleEndian.Uint64(page[8:16]))
	lenLog := binary.LittleEndian.Uint64(page[16:24])
	s.log = nil
	if lenLog > 0 {
		s.fd.Seek(int64(PAGE_SIZE), 0)
		var e Entry
		for i := 0; uint64(i) < lenLog; i++ {
			var entryBytes [ENTRY_SIZE]byte
			n, err := s.fd.Read(entryBytes[:])
			if err != nil {
				panic(err)
			}
			Server_assert(s, "Read full entry", n, ENTRY_SIZE)
			// Bytes 0 - 8: Entry term
			// Bytes 8 - 16: Entry command length
			// Bytes 16 - ENTRY_SIZE: Entry command
			e.Term = binary.LittleEndian.Uint64(entryBytes[:8])
			lenValue := binary.LittleEndian.Uint64(entryBytes[8:16])
			e.Command = entryBytes[16 : 16+lenValue]
			s.log = append(s.log, e)
		}
	}
	s.ensureLog()
}

func (s *Server) requestVote() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}
		go func(i int) {
			s.mu.Lock()
			s.debugf("Requesting vote from %d.", s.cluster[i].Id)
			lastLogIndex := uint64(len(s.log) - 1)
			lastLogTerm := s.log[len(s.log)-1].Term
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
				// Will retry later
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

	lastLogTerm := s.log[len(s.log)-1].Term
	logLen := uint64(len(s.log) - 1)

	logOk := req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= logLen)

	grant := req.Term == s.currentTerm && logOk && (s.getVotedFor() == 0 || s.getVotedFor() == req.CandidateId)

	if grant {
		s.debugf("Voted for %d.", req.CandidateId)
		s.setVotedFor(req.CandidateId)
		rsp.VoteGranted = true
		s.resetElectionTimeout()
		s.persist(false, 0)
	} else {
		s.debugf("Not granting vote request from %d.", +req.CandidateId)
	}

	return nil
}

// Must be called within a s.mu.Lock()
func (s *Server) updateTerm(msg RPCMessage) bool {
	transitioned := false
	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.state = followerState
		s.setVotedFor(0)
		transitioned = true
		s.debug("Transitioned to follower")
		s.resetElectionTimeout()
		s.persist(false, 0)
	}
	return transitioned
}

func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	// RPC test
	s.debugf("[RECV AppendEntries] from leader=%d term=%d prev=(%d,%d) commit=%d pba=%d blocks=%d",
		req.LeaderId, req.Term, req.PrevLogIndex, req.PrevLogTerm, req.LeaderCommit, req.LeaderPbaSrc, req.LogBlockCount)

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

	// ★ 기존엔 여기서 req.Entries를 s.log에 append 했음 → 제거
	// ★ 커밋 인덱스만 리더 힌트로 갱신
	if req.LeaderCommit > s.commitIndex {
		// NVMe에서 최신 레코드를 끌어와 s.log 동기화
		if err := s.syncFromNVMe(); err != nil {
			s.warnf("syncFromNVMe failed: %v", err)
			// 실패해도 다음 라운드에서 재시도
		}
		// s.log 길이 기반으로 안전 커밋
		s.commitIndex = min(req.LeaderCommit, uint64(len(s.log)-1))
	}

	// ★ 더 이상 persist()로 md_* 파일 쓰지 않음 — 필요시 메타만 유지하려면 남겨도 됨
	// s.persist(...)
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

	s.persist(true, len(commands))
	s.debug("Waiting to be applied!")

	s.mu.Unlock()

	s.appendEntries()

	// TODO: What happens if this takes too long?
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

func appendToFollowerNVMe(followerID uint64, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	targetPath := filepath.Join(nvmeBaseDir(followerID), "raft.log")
	f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)

	for _, e := range entries {
		if err := binary.Write(bw, binary.LittleEndian, e.Term); err != nil {
			f.Close()
			return err
		}
		if err := binary.Write(bw, binary.LittleEndian, uint64(len(e.Command))); err != nil {
			f.Close()
			return err
		}
		if _, err := bw.Write(e.Command); err != nil {
			f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (s *Server) syncFromNVMe() error {
	myPath := filepath.Join(nvmeBaseDir(s.id), "raft.log")
	f, err := os.Open(myPath)
	if err != nil {
		if os.IsNotExist(err) {
			// 아직 기록이 없을 수 있음
			return nil
		}
		return err
	}
	defer f.Close()

	rd := bufio.NewReader(f)

	// s.log[0]은 sentinel(빈 엔트리)라고 가정 → 파일 첫 레코드는 인덱스 1
	curIndex := uint64(len(s.log)) // 다음 append될 인덱스
	// 이미 s.log에 curIndex-1까지 들어있으니, 파일 레코드를 그만큼 스킵
	// 간단 구현: 파일 전체를 읽되, count를 세다가 curIndex부터 append
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
			// 새 레코드
			s.log = append(s.log, Entry{
				Term:    term,
				Command: buf,
				result:  nil,
			})
		}
	}
	return nil
}

func (s *Server) rpcCall(i int, name string, req, rsp any) bool {
	s.mu.Lock()
	c := s.cluster[i]
	var err error
	var rpcClient *rpc.Client = c.rpcClient
	if c.rpcClient == nil {
		c.rpcClient, err = rpc.DialHTTP("tcp", c.Address)
		rpcClient = c.rpcClient
	}
	s.mu.Unlock()

	if err == nil {
		err = rpcClient.Call(name, req, rsp)
	}

	if err != nil {
		s.warnf("Error calling %s on %d: %s.", name, c.Id, err)
	}
	return err == nil
}

const MAX_APPEND_ENTRIES_BATCH = 8_000

func (s *Server) appendEntries() {
	// leader variable snapshot (shared)
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
		if i == s.clusterIndex { // except for self(Leader)
			continue
		}
		go func(i int) { // followers' goroutine
			s.mu.Lock()

			next := s.cluster[i].nextIndex
			prevLogIndex := next - 1

			var prevLogTerm uint64
			if prevLogIndex > 0 {
				prevLogTerm = s.log[prevLogIndex].Term
			} else {
				prevLogTerm = 0
			}

			// entries length
			var lenEntries uint64 = 0
			last := uint64(len(s.log) - 1)
			if last >= next {
				remain := last - next + 1
				lenEntries = remain
				if lenEntries > MAX_APPEND_ENTRIES_BATCH {
					lenEntries = MAX_APPEND_ENTRIES_BATCH
				}
			}

			// block count for each follower
			bytesToCopy := lenEntries * uint64(ENTRY_SIZE)
			var logBlockCount uint64 = 0
			if bytesToCopy > 0 {
				logBlockCount = (bytesToCopy + 512 - 1) / 512
			}

			// leader's PBA for each follower
			var leaderPbaSrc uint64 = 0
			// if bytesToCopy > 0 {
			//     logicalOff := int64(PAGE_SIZE) + int64(ENTRY_SIZE)*int64(next)
			//     pba, err := s.l_get_pba(logicalOff, bytesToCopy)
			//     if err != nil {
			//         s.mu.Unlock()
			//         s.warnf("l_get_pba failed (follower=%d, next=%d): %v", s.cluster[i].Id, next, err)
			//         return
			//     }
			//     leaderPbaSrc = pba
			// }

			// var entries []Entry
			// if uint64(len(s.log)-1) >= s.cluster[i].nextIndex {
			// 	s.debugf("len: %d, next: %d, server: %d", len(s.log), next, s.cluster[i].Id)
			// 	entries = s.log[next:]
			// }
			// if len(entries) > MAX_APPEND_ENTRIES_BATCH {
			// 	entries = entries[:MAX_APPEND_ENTRIES_BATCH]
			// }
			// lenEntries := uint64(len(entries))
			// -> variable size entry -> inconsistent

			// ★★★ NVMe에 먼저 직접 기록 ★★★
			// -> 기록을 리더가 아닌 팔로워가 하도록 로드 분배
			// if err := appendToFollowerNVMe(s.cluster[i].Id, entries); err != nil {
			// 	s.mu.Unlock()
			// 	s.warnf("NVMe mirror write to %d failed: %v", s.cluster[i].Id, err)
			// 	// 실패하면 이번 라운드는 스킵 (다음 tick에서 재시도)
			// 	return
			// }

			// test dummy data
			leaderPbaSrc = uint64(0xdeadbeef)
			logBlockCount = uint64(1234)

			req := AppendEntriesRequest{
				RPCMessage:   RPCMessage{Term: term},
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: leaderCommit,

				// Blockcopy Metadata
				LeaderPbaSrc:  leaderPbaSrc,
				LogBlockCount: logBlockCount,
			}

			s.mu.Unlock()

			var rsp AppendEntriesResponse
			s.debugf("Control RPC to %d term %d.", s.cluster[i].Id, req.Term)
			ok := s.rpcCall(i, "Server.HandleAppendEntriesRequest", req, &rsp)
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

			// TODO: response
			if rsp.Success {
				prev := s.cluster[i].nextIndex
				s.cluster[i].nextIndex = max(req.PrevLogIndex+lenEntries+1, 1)
				s.cluster[i].matchIndex = s.cluster[i].nextIndex - 1
				s.debugf("Accepted by %d. Prev: %d → Next: %d, Match: %d.", s.cluster[i].Id, prev, s.cluster[i].nextIndex, s.cluster[i].matchIndex)
			} else {
				s.cluster[i].nextIndex = max(s.cluster[i].nextIndex-1, 1)
				s.debugf("Back off to %d for %d.", s.cluster[i].nextIndex, s.cluster[i].Id)
			}
		}(i)
	}
}

func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Leader can update commitIndex on quorum.
	if s.state == leaderState {
		lastLogIndex := uint64(len(s.log) - 1)
		for i := lastLogIndex; i > s.commitIndex; i-- {
			quorum := len(s.cluster)/2 + 1
			for j := range s.cluster {
				if quorum == 0 {
					break
				}
				isLeader := j == s.clusterIndex // Leader always has the log.
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

	if s.lastApplied <= s.commitIndex {
		log := s.log[s.lastApplied]

		// len(log.Command) == 0 is a noop committed by the leader.
		if len(log.Command) > 0 {
			s.debugf("Entry applied: %d.", s.lastApplied)
			// TODO: what if Apply() takes too long?
			res, err := s.statemachine.Apply(log.Command)

			// Will be nil for follower entries and for no-op entries.
			// Not nil for all user submitted messages.
			if log.result != nil {
				log.result <- ApplyResult{
					Result: res,
					Error:  err,
				}
			}
		}
		s.lastApplied++
	}
}

// Must be called within a s.mu.Lock()
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
		s.persist(false, 0)
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
		// Reset all cluster state
		for i := range s.cluster {
			s.cluster[i].nextIndex = uint64(len(s.log) + 1)
			// Yes, even matchIndex is reset. Figure 2
			// from Raft shows both nextIndex and
			// matchIndex are reset after every election.
			s.cluster[i].matchIndex = 0
		}

		s.debug("New leader.")
		s.state = leaderState

		// From Section 8 Client Interaction:
		// > First, a leader must have the latest information on
		// > which entries are committed. The Leader
		// > Completeness Property guarantees that a leader has
		// > all committed entries, but at the start of its
		// > term, it may not know which those are. To find out,
		// > it needs to commit an entry from its term. Raft
		// > handles this by having each leader commit a blank
		// > no-op entry into the log at the start of its ter m.
		s.log = append(s.log, Entry{Term: s.currentTerm, Command: nil})
		s.persist(true, 1)
		// Triggers s.appendEntries() in the next tick of the
		// main state loop.
		s.heartbeatTimeout = time.Now()

		// TODO: log consistency - truncate/overwrite
	}
}

func (s *Server) heartbeat() {
	s.mu.Lock()
	defer s.mu.Unlock()

	timeForHeartbeat := time.Now().After(s.heartbeatTimeout)
	if timeForHeartbeat {
		s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
		s.debug("Sending heartbeat")
		s.appendEntries()
	}
}

// Make sure rand is seeded
func (s *Server) Start() {
	s.mu.Lock()
	s.state = followerState
	s.done = false
	s.mu.Unlock()

	s.restore()

	rpcServer := rpc.NewServer()
	rpcServer.Register(s)
	l, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
	}
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
