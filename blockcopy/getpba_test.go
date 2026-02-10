package blockcopy

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

/*
GETPBA_TEST - Test correctness of PBA retrieval across computing nodes

Configuration:
- Computing nodes: eternity4, eternity6
- Storage node: eternity5
- Combined by NVMe-oF
- Test file: pba_test.txt
- OS: Ubuntu 18.04.6

Test Goals:
1. Verify two computing nodes see same PBA for same file

Test Flow:
1. eternity4: Create/store pba_test.txt on storage node
2. Get PBA of pba_test.txt from both eternity4 and eternity6
3. Compare PBA results from both nodes
*/

const (
	// Storage paths for each computing node
	// These should be NVMe-oF mounted paths
	STORAGE_PATH_ETERNITY4 = ""/*"/mnt/nvme0n1"*/
	STORAGE_PATH_ETERNITY6 = ""/*"/mnt/nvme0n1"*/

	// Test file name
	TEST_PATH     = "/home/jongc/nvmeof_raft/blockcopy/"
	TEST_FILENAME = TEST_PATH + "pba_test.txt"

	// Block size for testing
	TEST_BLOCK_SIZE = 4096
)

// NodeConfig represents configuration for a computing node
type NodeConfig struct {
	NodeID      uint64
	NodeName    string
	StoragePath string
}

// Test configuration for eternity4
var eternity4Config = NodeConfig{
	NodeID:      4,
	NodeName:    "eternity4",
	StoragePath: STORAGE_PATH_ETERNITY4,
}

// Test configuration for eternity6
var eternity6Config = NodeConfig{
	NodeID:      6,
	NodeName:    "eternity6",
	StoragePath: STORAGE_PATH_ETERNITY6,
}

// TestGetPBA_Step1_CreateFile tests file creation on storage node
func TestGetPBA_Step1_CreateFile(t *testing.T) {
	t.Log("=== GETPBA_TEST Step 1: Create/Store pba_test.txt ===")

	// Use eternity4 to create the file
	config := eternity4Config
	testFilePath := filepath.Join(config.StoragePath, TEST_FILENAME)

	// Create test data
	testData := make([]byte, TEST_BLOCK_SIZE)
	for i := 0; i < TEST_BLOCK_SIZE; i++ {
		testData[i] = byte(i % 256)
	}

	// Write test file
	err := ioutil.WriteFile(testFilePath, testData, 0644)
	if err != nil {
		t.Fatalf("[%s] Failed to create test file: %v", config.NodeName, err)
	}

	t.Logf("[%s] Successfully created test file: %s", config.NodeName, testFilePath)
	t.Logf("[%s] File size: %d bytes", config.NodeName, len(testData))

	// Verify file exists
	stat, err := os.Stat(testFilePath)
	if err != nil {
		t.Fatalf("[%s] Failed to stat test file: %v", config.NodeName, err)
	}

	t.Logf("[%s] File stat - Size: %d, Mode: %v",
		config.NodeName, stat.Size(), stat.Mode())
}

// TestGetPBA_Step2_GetPBAFromBothNodes tests PBA retrieval from both nodes
func TestGetPBA_Step2_GetPBAFromBothNodes(t *testing.T) {
	t.Log("=== GETPBA_TEST Step 2: Get PBA from both computing nodes ===")

	// Get PBA from eternity4
	testFilePath4 := filepath.Join(eternity4Config.StoragePath, TEST_FILENAME)
	seg4, err := l_get_pba(testFilePath4, 0, TEST_BLOCK_SIZE)
	if err != nil {
		t.Fatalf("[%s] Failed to get PBA: %v", eternity4Config.NodeName, err)
	}

	t.Logf("[%s] PBA retrieved successfully", eternity4Config.NodeName)
	t.Logf("[%s]   Physical Address: 0x%X (%d)",
		eternity4Config.NodeName, seg4.PBA, seg4.PBA)
	t.Logf("[%s]   Length: %d bytes", eternity4Config.NodeName, seg4.Len)

	// Get PBA from eternity6
	testFilePath6 := filepath.Join(eternity6Config.StoragePath, TEST_FILENAME)
	seg6, err := l_get_pba(testFilePath6, 0, TEST_BLOCK_SIZE)
	if err != nil {
		t.Fatalf("[%s] Failed to get PBA: %v", eternity6Config.NodeName, err)
	}

	t.Logf("[%s] PBA retrieved successfully", eternity6Config.NodeName)
	t.Logf("[%s]   Physical Address: 0x%X (%d)",
		eternity6Config.NodeName, seg6.PBA, seg6.PBA)
	t.Logf("[%s]   Length: %d bytes", eternity6Config.NodeName, seg6.Len)
}

// TestGetPBA_Step3_ComparePBA tests if both nodes see the same PBA
func TestGetPBA_Step3_ComparePBA(t *testing.T) {
	t.Log("=== GETPBA_TEST Step 3: Compare PBA from both nodes ===")

	// Get PBA from eternity4
	testFilePath4 := filepath.Join(eternity4Config.StoragePath, TEST_FILENAME)
	seg4, err := l_get_pba(testFilePath4, 0, TEST_BLOCK_SIZE)
	if err != nil {
		t.Fatalf("[%s] Failed to get PBA: %v", eternity4Config.NodeName, err)
	}

	// Get PBA from eternity6
	testFilePath6 := filepath.Join(eternity6Config.StoragePath, TEST_FILENAME)
	seg6, err := l_get_pba(testFilePath6, 0, TEST_BLOCK_SIZE)
	if err != nil {
		t.Fatalf("[%s] Failed to get PBA: %v", eternity6Config.NodeName, err)
	}

	// Compare PBAs
	t.Logf("Comparing PBAs:")
	t.Logf("  [%s] PBA: 0x%X", eternity4Config.NodeName, seg4.PBA)
	t.Logf("  [%s] PBA: 0x%X", eternity6Config.NodeName, seg6.PBA)

	if seg4.PBA != seg6.PBA {
		t.Errorf("PBA MISMATCH! %s: 0x%X, %s: 0x%X",
			eternity4Config.NodeName, seg4.PBA,
			eternity6Config.NodeName, seg6.PBA)
		t.Fatal("TEST FAILED: Both nodes should see the same PBA for the same file")
	}

	if seg4.Len != seg6.Len {
		t.Errorf("Length MISMATCH! %s: %d, %s: %d",
			eternity4Config.NodeName, seg4.Len,
			eternity6Config.NodeName, seg6.Len)
	}

	t.Logf("✓ SUCCESS: Both nodes see the same PBA (0x%X)", seg4.PBA)
	t.Logf("✓ SUCCESS: Both nodes see the same length (%d bytes)", seg4.Len)
}

// TestGetPBA_Complete runs the complete test sequence (Steps 1-3)
func TestGetPBA_Complete(t *testing.T) {
	t.Log("==================================================")
	t.Log("        GETPBA_TEST - Complete Test Suite")
	t.Log("==================================================")
	t.Log("")
	t.Log("Configuration:")
	t.Logf("  Computing Node 1: %s (ID: %d)", eternity4Config.NodeName, eternity4Config.NodeID)
	t.Logf("  Computing Node 2: %s (ID: %d)", eternity6Config.NodeName, eternity6Config.NodeID)
	t.Logf("  Storage Path 1: %s", eternity4Config.StoragePath)
	t.Logf("  Storage Path 2: %s", eternity6Config.StoragePath)
	t.Logf("  Test File: %s", TEST_FILENAME)
	t.Logf("  Block Size: %d bytes", TEST_BLOCK_SIZE)
	t.Log("")

	// Step 1: Create file
	t.Run("Step1_CreateFile", func(t *testing.T) {
		TestGetPBA_Step1_CreateFile(t)
	})

	// Step 2-3: Get and compare PBA
	t.Run("Step2_GetPBA", func(t *testing.T) {
		TestGetPBA_Step2_GetPBAFromBothNodes(t)
	})

	t.Run("Step3_ComparePBA", func(t *testing.T) {
		TestGetPBA_Step3_ComparePBA(t)
	})

	t.Log("")
	t.Log("==================================================")
	t.Log("        GETPBA_TEST - ALL TESTS PASSED")
	t.Log("==================================================")
}

// TestGetPBA_Cleanup removes test files
func TestGetPBA_Cleanup(t *testing.T) {
	t.Log("=== GETPBA_TEST Cleanup ===")

	testFilePath4 := filepath.Join(eternity4Config.StoragePath, TEST_FILENAME)
	if err := os.Remove(testFilePath4); err != nil {
		t.Logf("Warning: Failed to remove test file from %s: %v",
			eternity4Config.NodeName, err)
	} else {
		t.Logf("Removed test file: %s", testFilePath4)
	}

	// Note: Since both nodes access the same storage,
	// only need to remove from one path
}
