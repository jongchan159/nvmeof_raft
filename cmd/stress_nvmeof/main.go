//go:build raft
// +build raft

package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"nvmeof_raft"
)

// ── Config ─────────────────────────────────────────────────────────────────
// Running all 3 servers locally on eternity4 with the same NVMe-oF device.
const (
	DEVICE_PATH   = "/dev/nvme1n1"
	PARTITION_OFF = uint64(1048576)
	METADATA_DIR  = "/mnt/nvmeof_raft/pbastress"
	N_ENTRIES     = 5
)

// ── Simple state machine ────────────────────────────────────────────────────
type arraySM struct {
	mu      sync.Mutex
	entries [][]byte
}

func (a *arraySM) Apply(cmd []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]byte, len(cmd))
	copy(cp, cmd)
	a.entries = append(a.entries, cp)
	return nil, nil
}

// ── Helpers ─────────────────────────────────────────────────────────────────
var letters = []byte("abcdefghijklmnopqrstuvwxyz")

func randomPayload() []byte {
	b := make([]byte, 16)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

func waitForLeader(servers []*nvmeof_raft.Server) *nvmeof_raft.Server {
	for {
		for _, s := range servers {
			if s.IsLeader() {
				return s
			}
		}
		fmt.Println("Waiting for leader...")
		time.Sleep(500 * time.Millisecond)
	}
}

func waitAllCommitted(servers []*nvmeof_raft.Server) {
	time.Sleep(time.Second) // let heartbeat propagate commitIndex
	for _, s := range servers {
		for {
			done, pct := s.AllCommitted()
			if done {
				fmt.Printf("Server %d: all committed.\n", s.Id())
				break
			}
			fmt.Printf("Server %d: waiting (%.0f%%)...\n", s.Id(), pct)
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// ── Main ────────────────────────────────────────────────────────────────────
func main() {
	rand.Seed(42)

	// Prepare metadata directory
	os.MkdirAll(METADATA_DIR, 0755)
	files, _ := ioutil.ReadDir(METADATA_DIR)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".dat") {
			os.Remove(METADATA_DIR + "/" + f.Name())
		}
	}

	cluster := []nvmeof_raft.ClusterMember{
		{Id: 1, Address: "localhost:3020", DevicePath: DEVICE_PATH},
		{Id: 2, Address: "localhost:3021", DevicePath: DEVICE_PATH},
		{Id: 3, Address: "localhost:3022", DevicePath: DEVICE_PATH},
	}

	sm1, sm2, sm3 := &arraySM{}, &arraySM{}, &arraySM{}

	s1 := nvmeof_raft.NewServer(cluster, sm1, METADATA_DIR, 0, PARTITION_OFF)
	s2 := nvmeof_raft.NewServer(cluster, sm2, METADATA_DIR, 1, PARTITION_OFF)
	s3 := nvmeof_raft.NewServer(cluster, sm3, METADATA_DIR, 2, PARTITION_OFF)

	s1.Debug = true
	s2.Debug = true
	s3.Debug = true

	s1.Start()
	s2.Start()
	s3.Start()

	servers := []*nvmeof_raft.Server{s1, s2, s3}
	sms := []*arraySM{sm1, sm2, sm3}

	leader := waitForLeader(servers)
	fmt.Printf("\n=== Leader: Server %d ===\n\n", leader.Id())

	// Generate and apply entries
	var allEntries [][]byte
	for i := 0; i < N_ENTRIES; i++ {
		allEntries = append(allEntries, randomPayload())
	}

	fmt.Printf("Applying %d entries via PBA-based replication...\n\n", N_ENTRIES)
	for i, entry := range allEntries {
	retry:
		for {
			for _, s := range servers {
				_, err := s.Apply([][]byte{entry})
				if err == nvmeof_raft.ErrApplyToLeader {
					continue
				} else if err != nil {
					panic(err)
				}
				fmt.Printf("Entry %d applied: %q\n", i+1, entry)
				break retry
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	fmt.Println("\nWaiting for all servers to commit...")
	waitAllCommitted(servers)

	// Validate
	fmt.Println("\n=== Validation ===")
	ok := true
	for i, entry := range allEntries {
		for j, sm := range sms {
			sm.mu.Lock()
			if j >= len(sm.entries) || string(sm.entries[i]) != string(entry) {
				var got string
				if j < len(sm.entries) {
					got = string(sm.entries[i])
				} else {
					got = "<missing>"
				}
				fmt.Printf("MISMATCH server %d entry %d: got %q want %q\n",
					cluster[j].Id, i, got, entry)
				ok = false
			}
			sm.mu.Unlock()
		}
	}

	for j, sm := range sms {
		sm.mu.Lock()
		fmt.Printf("Server %d: %d entries in state machine\n", cluster[j].Id, len(sm.entries))
		sm.mu.Unlock()
	}

	if ok {
		fmt.Println("\nok - PBA-based log replication verified on all servers")
	} else {
		fmt.Println("\nFAIL - some entries did not replicate correctly")
		os.Exit(1)
	}
}
