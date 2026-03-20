//go:build raft
// +build raft

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"nvmeof_raft"
)

const (
	N_ENTRIES    = 500
	BATCH_SIZE   = 16 // commands per Apply() call
	PAYLOAD_SIZE = 4096
)

type nullSM struct{}

func (n *nullSM) Apply(cmd []byte) ([]byte, error) { return nil, nil }

var letters = []byte("abcdefghijklmnopqrstuvwxyz")

func randomPayload() []byte {
	b := make([]byte, PAYLOAD_SIZE)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

func parsePeers(peersStr string) []nvmeof_raft.ClusterMember {
	var members []nvmeof_raft.ClusterMember
	for i, addr := range strings.Split(peersStr, ",") {
		members = append(members, nvmeof_raft.ClusterMember{
			Id:      uint64(i + 1),
			Address: strings.TrimSpace(addr),
		})
	}
	return members
}

func printStats(latencies []time.Duration, total time.Duration, n int) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	avg := sum / time.Duration(len(latencies))
	throughput := float64(n) / total.Seconds()

	fmt.Printf("\n=== nvmeof_raft (PBA-based replication) ===\n")
	fmt.Printf("  Entries      : %d\n", n)
	fmt.Printf("  Total time   : %s\n", total)
	fmt.Printf("  Throughput   : %.2f entries/s\n", throughput)
	fmt.Printf("  Latency avg  : %s\n", avg)
	fmt.Printf("  Latency min  : %s\n", latencies[0])
	fmt.Printf("  Latency p50  : %s\n", latencies[len(latencies)*50/100])
	fmt.Printf("  Latency p99  : %s\n", latencies[len(latencies)*99/100])
	fmt.Printf("  Latency max  : %s\n", latencies[len(latencies)-1])
}

func main() {
	id             := flag.Int("id", 0, "node index in peers list (0-based)")
	peersStr       := flag.String("peers", "", "comma-separated addresses: host1:port,host2:port,host3:port")
	metadataDir    := flag.String("metadata-dir", "./bench_nvmeof_data", "directory for ring buffer .dat files (must be on NVMe partition)")
	devicePath     := flag.String("device", "/dev/nvme0n1", "NVMe-oF block device path")
	partitionOffset := flag.Uint64("partition-offset", 0, "partition start offset in bytes (sector_start * 512)")
	bench          := flag.Bool("bench", false, "run benchmark on this node if it becomes leader")
	debug          := flag.Bool("debug", false, "enable raft debug logging")
	flag.Parse()

	if *peersStr == "" {
		fmt.Fprintln(os.Stderr, "usage: bench_nvmeof --id=0 --peers=h1:4020,h2:4021,h3:4022 --device=/dev/nvme0n1 --partition-offset=N [--bench]")
		os.Exit(1)
	}

	rand.Seed(time.Now().UnixNano())
	os.MkdirAll(*metadataDir, 0755)
	files, _ := ioutil.ReadDir(*metadataDir)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".dat") {
			os.Remove(*metadataDir + "/" + f.Name())
		}
	}

	cluster := parsePeers(*peersStr)
	server := nvmeof_raft.NewServer(cluster, &nullSM{}, *metadataDir, *id, *devicePath, *partitionOffset)
	server.Debug = *debug
	server.Start()
	fmt.Printf("[node %d] started at %s\n", cluster[*id].Id, cluster[*id].Address)

	if !*bench {
		select {} // run as plain Raft node
	}

	fmt.Printf("[node %d] waiting to become stable leader...\n", cluster[*id].Id)
	for {
		for !server.IsLeader() {
			time.Sleep(1000 * time.Millisecond)
		}
		_, err := server.Apply([][]byte{[]byte("warmup")})
		if err == nil {
			break
		}
		fmt.Printf("[node %d] lost leadership, waiting again...\n", cluster[*id].Id)
	}
	fmt.Printf("[node %d] is stable leader — starting benchmark (%d entries, batch=%d, payload=%d)\n",
		server.Id(), N_ENTRIES, BATCH_SIZE, PAYLOAD_SIZE)

	var latencies []time.Duration
	start := time.Now()

	for i := 0; i < N_ENTRIES; i += BATCH_SIZE {
		end := i + BATCH_SIZE
		if end > N_ENTRIES {
			end = N_ENTRIES
		}
		var batch [][]byte
		for k := i; k < end; k++ {
			batch = append(batch, randomPayload())
		}
		t := time.Now()
		if _, err := server.Apply(batch); err != nil {
			fmt.Printf("[node %d] lost leadership during benchmark: %v\n", server.Id(), err)
			os.Exit(1)
		}
		latencies = append(latencies, time.Since(t))
	}

	printStats(latencies, time.Since(start), N_ENTRIES)
}
