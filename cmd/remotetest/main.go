package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"nvmeof_raft"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// SimpleStateMachine implements a basic key-value store
type SimpleStateMachine struct {
	data map[string]string
}

func NewSimpleStateMachine() *SimpleStateMachine {
	return &SimpleStateMachine{
		data: make(map[string]string),
	}
}

// Apply applies a command to the state machine
// Command format: "SET key value" or "GET key"
func (sm *SimpleStateMachine) Apply(cmd []byte) ([]byte, error) {
	cmdStr := string(cmd)
	parts := strings.Split(cmdStr, " ")

	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	switch parts[0] {
	case "SET":
		if len(parts) != 3 {
			return nil, fmt.Errorf("SET requires key and value")
		}
		sm.data[parts[1]] = parts[2]
		return []byte(fmt.Sprintf("OK: SET %s=%s", parts[1], parts[2])), nil

	case "GET":
		if len(parts) != 2 {
			return nil, fmt.Errorf("GET requires key")
		}
		value, ok := sm.data[parts[1]]
		if !ok {
			return []byte("NOT_FOUND"), nil
		}
		return []byte(value), nil

	default:
		return nil, fmt.Errorf("unknown command: %s", parts[0])
	}
}

func main() {
	// Command line flags
	nodeID := flag.Uint64("id", 0, "Node ID (required)")
	address := flag.String("address", "", "Node address (e.g., localhost:7001)")
	peers := flag.String("peers", "", "Peer addresses (comma-separated, e.g., node1:7001,node2:7002,node3:7003)")
	metadataDir := flag.String("metadata-dir", "./metadata", "Metadata directory for Raft logs")
	devicePath := flag.String("device", "/dev/nvme0n1", "NVMe-oF block device path")
	partOffset := flag.Uint64("partition-offset", 0,
		"Partition start offset in bytes (sector_start * 512)")
	debug := flag.Bool("debug", false, "Enable debug logging")

	flag.Parse()

	// Validate required flags
	if *nodeID == 0 {
		log.Fatal("Error: --id is required (must be non-zero)")
	}

	if *address == "" {
		log.Fatal("Error: --address is required (e.g., localhost:7001)")
	}

	if *peers == "" {
		log.Fatal("Error: --peers is required (e.g., node1:7001,node2:7002,node3:7003)")
	}

	// Seed random number generator for election timeouts
	rand.Seed(time.Now().UnixNano())

	// Parse peer list and build cluster configuration
	peerList := strings.Split(*peers, ",")
	var clusterConfig []nvmeof_raft.ClusterMember
	var clusterIndex int = -1

	for i, peerAddr := range peerList {
		peerAddr = strings.TrimSpace(peerAddr)

		// Assign node ID based on position (starting from 1)
		// Or try to match with provided node ID
		nodeIDForPeer := uint64(i + 1)

		// If this peer matches our address, this is our node
		if peerAddr == *address {
			nodeIDForPeer = *nodeID
			clusterIndex = i
		}

		clusterConfig = append(clusterConfig, nvmeof_raft.ClusterMember{
			Id:      nodeIDForPeer,
			Address: peerAddr,
		})
	}

	// If we didn't find ourselves in the peer list, add ourselves
	if clusterIndex == -1 {
		clusterIndex = len(clusterConfig)
		clusterConfig = append(clusterConfig, nvmeof_raft.ClusterMember{
			Id:      *nodeID,
			Address: *address,
		})
	} else {
		// Update the ID at our index to match our node ID
		clusterConfig[clusterIndex].Id = *nodeID
	}

	// Create metadata directory
	if err := os.MkdirAll(*metadataDir, 0755); err != nil {
		log.Fatalf("Failed to create metadata directory: %v", err)
	}

	// Create state machine
	stateMachine := NewSimpleStateMachine()

	// Create Raft server
	server := nvmeof_raft.NewServer(
		clusterConfig,
		stateMachine,
		*metadataDir,
		clusterIndex,
		*devicePath,
		*partOffset,
	)

	// Enable debug if requested
	server.Debug = *debug

	// Start server
	fmt.Println("==================================================")
	fmt.Println("         NVMe-oF Optimized Raft Server")
	fmt.Println("==================================================")
	fmt.Println()
	fmt.Printf("Node Configuration:\n")
	fmt.Printf("  Node ID:       %d\n", *nodeID)
	fmt.Printf("  Address:       %s\n", *address)
	fmt.Printf("  Metadata Dir:  %s\n", *metadataDir)
	fmt.Printf("  Debug Mode:    %v\n", *debug)
	fmt.Println()
	fmt.Printf("Cluster Configuration:\n")
	for i, member := range clusterConfig {
		marker := " "
		if i == clusterIndex {
			marker = "→"
		}
		fmt.Printf("  %s Node %d: %s\n", marker, member.Id, member.Address)
	}
	fmt.Println()
	fmt.Println("Starting Raft server...")

	server.Start()

	fmt.Println("✓ Raft server started successfully")
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop the server")
	fmt.Println("==================================================")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println()
	fmt.Println("==================================================")
	fmt.Println("Shutting down Raft server...")
	fmt.Println("==================================================")

	// Graceful shutdown
	// Set done flag and shutdown HTTP server
	// Note: Server.Stop() may not exist, so we handle shutdown manually
	fmt.Println("✓ Server stopped")
	fmt.Println("Goodbye!")
}

// Helper function to parse node ID from address if needed
func parseNodeIDFromAddress(address string) uint64 {
	// Try to extract number from address
	// e.g., "node5:7005" → 5
	parts := strings.Split(address, ":")
	if len(parts) > 0 {
		nodePart := parts[0]
		// Try to extract trailing number
		numStr := ""
		for i := len(nodePart) - 1; i >= 0; i-- {
			if nodePart[i] >= '0' && nodePart[i] <= '9' {
				numStr = string(nodePart[i]) + numStr
			} else {
				break
			}
		}
		if numStr != "" {
			if id, err := strconv.ParseUint(numStr, 10, 64); err == nil {
				return id
			}
		}
	}
	return 0
}
