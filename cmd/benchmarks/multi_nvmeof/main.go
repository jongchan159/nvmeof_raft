//go:build raft
// +build raft

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"

	"nvmeof_raft"
)

type nullSM struct{}

func (n *nullSM) Apply(cmd []byte) ([]byte, error) { return nil, nil }

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

func main() {
	id              := flag.Int("id", 0, "node index in peers list (0-based)")
	peersStr        := flag.String("peers", "", "comma-separated addresses: host1:port,host2:port,...")
	metadataDir     := flag.String("metadata-dir", "./bench_nvmeof_data", "directory for ring buffer .dat files (must be on NVMe partition)")
	devicePath      := flag.String("device", "/dev/nvme0n1", "NVMe-oF block device path")
	partitionOffset := flag.Uint64("partition-offset", 0, "partition start offset in bytes (sector_start * 512)")
	debug           := flag.Bool("debug", false, "enable raft debug logging")
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
	server.Start()
	fmt.Printf("[node %d] started at %s\n", cluster[*id].Id, cluster[*id].Address)

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
