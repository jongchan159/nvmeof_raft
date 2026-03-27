//go:build raft || raft_tcp
// +build raft raft_tcp

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const (
	N_ENTRIES    = 10000
	BATCH_SIZE   = 256
	PAYLOAD_SIZE = 8192
)

// Must match field names in nvmeof_raft.ClientApplyRequest/Response.
// Gob encoding is field-name based — no import of nvmeof_raft needed.
type ClientApplyRequest struct {
	Commands [][]byte
}
type ClientApplyResponse struct {
	Err string
}

var letters = []byte("abcdefghijklmnopqrstuvwxyz")

func randomPayload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

// findLeader probes each peer until one accepts a warmup Apply (i.e. is leader).
func findLeader(peers []string) (string, *rpc.Client, error) {
	for _, addr := range peers {
		client, err := dialLeader(addr)
		if err != nil {
			fmt.Printf("  %s: connect failed: %v\n", addr, err)
			continue
		}
		req := ClientApplyRequest{Commands: [][]byte{[]byte("probe")}}
		var rsp ClientApplyResponse
		if err := client.Call("Server.HandleClientApply", &req, &rsp); err != nil {
			client.Close()
			fmt.Printf("  %s: rpc error: %v\n", addr, err)
			continue
		}
		if rsp.Err != "" && strings.Contains(rsp.Err, "follower") {
			client.Close()
			fmt.Printf("  %s: follower\n", addr)
			continue
		}
		return addr, client, nil
	}
	return "", nil, fmt.Errorf("no leader found among %v", peers)
}

func printStats(latencies []time.Duration, total time.Duration, n, batch, payload, threads int) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	avg := sum / time.Duration(len(latencies))
	throughput := float64(n) / total.Seconds()

	fmt.Printf("\n")
	fmt.Printf("======= Experimental Parameters =======\n")
	fmt.Printf("%d entries, Batch: %d, Payload: %d, Threads: %d\n", n, batch, payload, threads)
	fmt.Printf("=== client bench (remote Apply) ===\n")
	fmt.Printf("  Total time     : %s\n", total)
	fmt.Printf("  Latency/entry (5s warmup + last drain removed, %d samples)\n", len(latencies))
	fmt.Printf("  Throughput     : %.2f entries/s\n", throughput)
	fmt.Printf("  Latency avg    : %s\n", avg)
	fmt.Printf("  Latency min    : %s\n", latencies[0])
	fmt.Printf("  Latency p50    : %s\n", latencies[len(latencies)*50/100])
	fmt.Printf("  Latency p99    : %s\n", latencies[len(latencies)*99/100])
	fmt.Printf("  Latency max    : %s\n", latencies[len(latencies)-1])
}

func main() {
	peersStr    := flag.String("peers", "", "comma-separated Raft node addresses (all nodes or just the leader)")
	nEntries    := flag.Int("entries", N_ENTRIES, "total number of entries to submit")
	batchSize   := flag.Int("batch", BATCH_SIZE, "commands per Apply() call per thread")
	payloadSize := flag.Int("payload", PAYLOAD_SIZE, "payload size in bytes")
	nThreads    := flag.Int("threads", 1, "number of concurrent client goroutines (each gets its own connection)")
	flag.Parse()

	if *peersStr == "" {
		fmt.Fprintln(os.Stderr, "usage: bench_client --peers=h1:4020,h2:4021,h3:4022 [--entries=N] [--batch=B] [--payload=P] [--threads=T]")
		os.Exit(1)
	}

	rand.Seed(time.Now().UnixNano())

	peers := strings.Split(*peersStr, ",")
	for i := range peers {
		peers[i] = strings.TrimSpace(peers[i])
	}

	fmt.Printf("Searching for leader among: %v\n", peers)
	leaderAddr, leaderClient, err := findLeader(peers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Leader: %s\n", leaderAddr)
	fmt.Printf("Benchmark: %d entries, batch=%d, payload=%d, threads=%d\n",
		*nEntries, *batchSize, *payloadSize, *nThreads)

	// Each thread gets its own RPC connection to avoid send-side serialization.
	clients := make([]*rpc.Client, *nThreads)
	clients[0] = leaderClient
	for t := 1; t < *nThreads; t++ {
		c, err := dialLeader(leaderAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "thread %d: dial failed: %v\n", t, err)
			os.Exit(1)
		}
		clients[t] = c
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	entriesPerThread := *nEntries / *nThreads
	type threadResult struct {
		latencies []time.Duration
		err       error
	}
	results := make([]chan threadResult, *nThreads)

	var done int64 // atomic counter: total entries submitted across all threads
	start := time.Now()

	// Progress reporter: prints every ~10% of total entries.
	reportStop := make(chan struct{})
	go func() {
		reportInterval := time.Second
		for {
			select {
			case <-reportStop:
				return
			case <-time.After(reportInterval):
				n := atomic.LoadInt64(&done)
				pct := float64(n) * 100 / float64(*nEntries)
				elapsed := time.Since(start)
				tput := float64(n) / elapsed.Seconds()
				fmt.Printf("  progress: %d / %d entries (%.0f%%)  elapsed: %s  throughput: %.0f entries/s\n",
					n, *nEntries, pct, elapsed.Truncate(time.Millisecond), tput)
			}
		}
	}()

	warmupEnd := start.Add(5 * time.Second)

	for t := 0; t < *nThreads; t++ {
		results[t] = make(chan threadResult, 1)
		go func(tid int) {
			client := clients[tid]
			var lats []time.Duration
			for i := 0; i < entriesPerThread; i += *batchSize {
				end := i + *batchSize
				if end > entriesPerThread {
					end = entriesPerThread
				}
				var cmds [][]byte
				for k := i; k < end; k++ {
					cmds = append(cmds, randomPayload(*payloadSize))
				}
				req := ClientApplyRequest{Commands: cmds}
				var rsp ClientApplyResponse
				t0 := time.Now()
				if err := client.Call("Server.HandleClientApply", &req, &rsp); err != nil {
					results[tid] <- threadResult{err: fmt.Errorf("rpc: %v", err)}
					return
				}
				if rsp.Err != "" {
					results[tid] <- threadResult{err: fmt.Errorf("apply: %s", rsp.Err)}
					return
				}
				atomic.AddInt64(&done, int64(len(cmds)))
				if t0.After(warmupEnd) {
					lats = append(lats, time.Since(t0)/time.Duration(len(cmds)))
				}
			}
			// Drop last batch (pipeline drain)
			if len(lats) > 0 {
				lats = lats[:len(lats)-1]
			}
			results[tid] <- threadResult{latencies: lats}
		}(t)
	}

	var latencies []time.Duration
	for t := 0; t < *nThreads; t++ {
		r := <-results[t]
		if r.err != nil {
			close(reportStop)
			fmt.Fprintf(os.Stderr, "thread %d error: %v\n", t, r.err)
			os.Exit(1)
		}
		latencies = append(latencies, r.latencies...)
	}
	close(reportStop)

	printStats(latencies, time.Since(start), *nEntries, *batchSize, *payloadSize, *nThreads)
}
