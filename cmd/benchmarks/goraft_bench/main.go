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

	"github.com/eatonphil/goraft"
)

// default benchmark parameters
const (
	N_ENTRIES    = 10000
	BATCH_SIZE   = 256  // commands per Apply() call
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

func parsePeers(peersStr string) []goraft.ClusterMember {
	var members []goraft.ClusterMember
	for i, addr := range strings.Split(peersStr, ",") {
		members = append(members, goraft.ClusterMember{
			Id:      uint64(i + 1),
			Address: strings.TrimSpace(addr),
		})
	}
	return members
}

func printStats(latencies []time.Duration, total time.Duration, n, batch, payload int) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	avg := sum / time.Duration(len(latencies))
	throughput := float64(n) / total.Seconds()

	fmt.Printf("\n")
	fmt.Printf("======= Experimental Parameter ========\n")
	fmt.Printf("%d entires, Batch: %d, Payload: %d\n", n, batch, payload)
	fmt.Printf("=== goraft (file-based replication) ===\n")
	fmt.Printf("  Total time   : %s\n", total)
	fmt.Printf("  Throughput   : %.2f entries/s\n", throughput)
	fmt.Printf("  Latency avg  : %s\n", avg)
	fmt.Printf("  Latency min  : %s\n", latencies[0])
	fmt.Printf("  Latency p50  : %s\n", latencies[len(latencies)*50/100])
	fmt.Printf("  Latency p99  : %s\n", latencies[len(latencies)*99/100])
	fmt.Printf("  Latency max  : %s\n", latencies[len(latencies)-1])
}

func main() {
	id          := flag.Int("id", 0, "node index in peers list (0-based)")
	peersStr    := flag.String("peers", "", "comma-separated addresses: host1:port,host2:port,host3:port")
	metadataDir := flag.String("metadata-dir", "./bench_goraft_data", "directory to store .dat files")
	bench       := flag.Bool("bench", false, "run benchmark on this node if it becomes leader")
	debug       := flag.Bool("debug", false, "enable raft debug logging")
	nEntries    := flag.Int("entries", N_ENTRIES, "number of entries to submit")
	batchSize   := flag.Int("batch", BATCH_SIZE, "commands per Apply() call")
	payloadSize := flag.Int("payload", PAYLOAD_SIZE, "payload size in bytes")
	flag.Parse()

	if *peersStr == "" {
		fmt.Fprintln(os.Stderr, "usage: bench_goraft --id=0 --peers=h1:4020,h2:4021,h3:4022 [--bench] [--metadata-dir=./data]")
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
	server := goraft.NewServer(cluster, &nullSM{}, *metadataDir, *id)
	server.Debug = *debug
	server.Start()
	fmt.Printf("[node %d] started at %s\n", cluster[*id].Id, cluster[*id].Address)

	if !*bench {
		select {} // run as plain Raft node
	}

	// Wait until THIS node becomes a stable leader (quorum confirmed).
	// A node can win a single-node election before peers join — confirm
	// stability by verifying Apply succeeds after becoming leader.
	fmt.Printf("[node %d] waiting to become stable leader...\n", cluster[*id].Id)
	for {
		for !server.IsLeader() {
			time.Sleep(1000 * time.Millisecond)
		}
		// Confirm quorum: a successful Apply proves followers ack'd
		_, err := server.Apply([][]byte{[]byte("warmup")})
		if err == nil {
			break // stable leader with quorum
		}
		fmt.Printf("[node %d] lost leadership, waiting again...\n", cluster[*id].Id)
	}
	fmt.Printf("[node %d] is stable leader — starting benchmark (%d entries, batch=%d, payload=%d)\n",
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
		var batch [][]byte
		for k := i; k < end; k++ {
			batch = append(batch, randomPayload(*payloadSize))
		}
		t := time.Now()
		if _, err := server.Apply(batch); err != nil {
			fmt.Printf("[node %d] lost leadership during benchmark: %v\n", server.Id(), err)
			os.Exit(1)
		}
		latencies = append(latencies, time.Since(t))
		if (i/(*batchSize))%(reportInterval/(*batchSize)+1) == 0 {
			fmt.Printf("[node %d] progress: %d / %d entries (%.0f%%) elapsed: %s\n",
				server.Id(), i+len(batch), *nEntries,
				float64(i+len(batch))*100/float64(*nEntries),
				time.Since(start))
		}
	}

	printStats(latencies, time.Since(start), *nEntries, *batchSize, *payloadSize)
}
