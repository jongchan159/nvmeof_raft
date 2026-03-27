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
	N_ENTRIES    = 10000
	BATCH_SIZE   = 256
	PAYLOAD_SIZE = 8192
)

type nullSM struct{}

func (n *nullSM) Apply(cmd []byte) ([]byte, error) { return nil, nil }

var letters = []byte("abcdefghijklmnopqrstuvwxyz")

func randomPayload(size int) []byte {
	b := make([]byte, size)
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

func printStats(latencies []time.Duration, total time.Duration, n, batch, payload int) {
	throughput := float64(n) / total.Seconds()

	fmt.Printf("\n")
	fmt.Printf("======= Experimental Parameters ======\n")
	fmt.Printf("%d entries, Batch: %d, Payload: %d\n", n, batch, payload)
	fmt.Printf("=== nvmeof_raft (PBA-based replication) ===\n")
	fmt.Printf("  Total time     : %s\n", total)
	fmt.Printf("  Throughput     : %.2f entries/s\n", throughput)
	if len(latencies) == 0 {
		fmt.Printf("  Latency        : no samples (all entries completed within warmup window)\n")
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	avg := sum / time.Duration(len(latencies))
	fmt.Printf("  Latency/entry (5s warmup + last drain removed, %d samples)\n", len(latencies))
	fmt.Printf("  Latency avg    : %s\n", avg)
	fmt.Printf("  Latency min    : %s\n", latencies[0])
	fmt.Printf("  Latency p50    : %s\n", latencies[len(latencies)*50/100])
	fmt.Printf("  Latency p99    : %s\n", latencies[len(latencies)*99/100])
	fmt.Printf("  Latency max    : %s\n", latencies[len(latencies)-1])
}

func main() {
	id              := flag.Int("id", 0, "node index in peers list (0-based)")
	peersStr        := flag.String("peers", "", "comma-separated addresses: host1:port,host2:port,...")
	metadataDir     := flag.String("metadata-dir", "./bench_nvmeof_data", "directory for ring buffer .dat files (must be on NVMe partition)")
	devicePath      := flag.String("device", "/dev/nvme0n1", "NVMe-oF block device path")
	partitionOffset := flag.Uint64("partition-offset", 0, "partition start offset in bytes (sector_start * 512)")
	debug           := flag.Bool("debug", false, "enable raft debug logging")
	heartbeatMs     := flag.Int("heartbeat-ms", 300, "heartbeat interval in ms (lower = shorter election timeout = wins elections)")
	bench           := flag.Bool("bench", false, "run benchmark on this node if it becomes leader")
	nEntries        := flag.Int("entries", N_ENTRIES, "number of entries to submit")
	batchSize       := flag.Int("batch", BATCH_SIZE, "commands per Apply() call")
	payloadSize     := flag.Int("payload", PAYLOAD_SIZE, "payload size in bytes")
	flag.Parse()

	if *peersStr == "" {
		fmt.Fprintln(os.Stderr, "usage: bench_multi_nvmeof --id=0 --peers=h1:4020,h2:4021,h3:4022 --device=/dev/nvme0n1 --partition-offset=N")
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
	server.HeartbeatMs = *heartbeatMs
	server.Start()
	fmt.Printf("[node %d] started at %s\n", cluster[*id].Id, cluster[*id].Address)

	if !*bench {
		go func() {
			wasLeader := false
			for {
				isLeader := server.IsLeader()
				if isLeader && !wasLeader {
					fmt.Printf("[node %d] became leader\n", cluster[*id].Id)
				} else if !isLeader && wasLeader {
					fmt.Printf("[node %d] lost leadership\n", cluster[*id].Id)
				}
				wasLeader = isLeader
				time.Sleep(100 * time.Millisecond)
			}
		}()
		select {}
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
	// Warmup: send dummy entries for 5s to prime the Raft pipeline.
	fmt.Printf("[node %d] warming up for 5s...\n", server.Id())
	warmupEnd := time.Now().Add(5 * time.Second)
	for time.Now().Before(warmupEnd) {
		batch := make([][]byte, *batchSize)
		for k := range batch {
			batch[k] = randomPayload(*payloadSize)
		}
		if _, err := server.Apply(batch); err != nil {
			fmt.Printf("[node %d] lost leadership during warmup: %v\n", server.Id(), err)
			os.Exit(1)
		}
	}

	fmt.Printf("[node %d] warmup done — starting benchmark (%d entries, batch=%d, payload=%d)\n",
		server.Id(), *nEntries, *batchSize, *payloadSize)

	var latencies []time.Duration
	start := time.Now()

	reportInterval := *nEntries / 10
	if reportInterval == 0 {
		reportInterval = 1
	}
	for i := 0; i < *nEntries; i += *batchSize {
		end := i + *batchSize
		if end > *nEntries {
			end = *nEntries
		}
		batch := make([][]byte, end-i)
		for k := range batch {
			batch[k] = randomPayload(*payloadSize)
		}
		t := time.Now()
		if _, err := server.Apply(batch); err != nil {
			fmt.Printf("[node %d] lost leadership during benchmark: %v\n", server.Id(), err)
			os.Exit(1)
		}
		latencies = append(latencies, time.Since(t)/time.Duration(len(batch)))
		if (i/(*batchSize))%(reportInterval/(*batchSize)+1) == 0 {
			fmt.Printf("[node %d] progress: %d / %d entries (%.0f%%) elapsed: %s\n",
				server.Id(), i+len(batch), *nEntries,
				float64(i+len(batch))*100/float64(*nEntries),
				time.Since(start))
		}
	}
	// Drop last batch (pipeline drain artifact)
	if len(latencies) > 0 {
		latencies = latencies[:len(latencies)-1]
	}

	printStats(latencies, time.Since(start), *nEntries, *batchSize, *payloadSize)
}
