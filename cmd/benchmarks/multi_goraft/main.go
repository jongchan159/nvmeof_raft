package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/eatonphil/goraft"
)

type nullSM struct{}

func (n *nullSM) Apply(cmd []byte) ([]byte, error) { return nil, nil }

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

func main() {
	id          := flag.Int("id", 0, "node index in peers list (0-based)")
	peersStr    := flag.String("peers", "", "comma-separated addresses: host1:port,host2:port,...")
	metadataDir := flag.String("metadata-dir", "./bench_goraft_data", "directory to store .dat files")
	debug       := flag.Bool("debug", false, "enable raft debug logging")
	flag.Parse()

	if *peersStr == "" {
		fmt.Fprintln(os.Stderr, "usage: bench_multi_goraft --id=0 --peers=h1:4020,h2:4021,h3:4022 [--metadata-dir=./data]")
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
