package blockcopy

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
	"os/exec"
)

/*
BLKCP_TEST - Test correctness of block copy operation at storage level

Configuration:
- Computing nodes: eternity5, eternity6
- Storage node: eternity5
- Combined by NVMe-oF
- Test files: blk_test5.log, blk_test6.log
- Device: /dev/nvme0n1
- OS: Ubuntu 18.04.6

Test Goals:
1. Verify storage-level block copy works correctly
2. Verify both nodes see identical content after copy

Test Flow:
1. eternity5, 6: Create blk_test*.log with different contents
2. eternity5, 6: Get PBA of blk_test*.log
3. eternity6: Copy blk_test5.log to blk_test6.log at storage level
4. eternity5, 6: Compare files - should be identical after copy
*/

const (
	// Storage paths for each computing node
	BLKCP_STORAGE_PATH_ETERNITY5 = "/mnt/nvme0n1/jongc/nvmeof_raft"
	BLKCP_STORAGE_PATH_ETERNITY6 = "/mnt/nvme0n1/jongc/nvmeof_raft"

	// Block device path
	BLKCP_DEVICE_PATH = "/dev/nvme-remote"

	// Test file names
	BLKCP_TEST_FILE_5 = "blk_test5.log"
	BLKCP_TEST_FILE_6 = "blk_test6.log"

	// Block size for testing
	BLKCP_BLOCK_SIZE = 4096
)

// BlkCpNodeConfig represents configuration for a computing node
type BlkCpNodeConfig struct {
	NodeID      uint64
	NodeName    string
	StoragePath string
	TestFile    string
}

// Test configuration for eternity5
var blkcp5Config = BlkCpNodeConfig{
	NodeID:      5,
	NodeName:    "eternity5",
	StoragePath: BLKCP_STORAGE_PATH_ETERNITY5,
	TestFile:    BLKCP_TEST_FILE_5,
}

// Test configuration for eternity6
var blkcp6Config = BlkCpNodeConfig{
	NodeID:      6,
	NodeName:    "eternity6",
	StoragePath: BLKCP_STORAGE_PATH_ETERNITY6,
	TestFile:    BLKCP_TEST_FILE_6,
}

// calculateSHA256 computes SHA256 hash of a file
func calculateSHA256(filePath string) (string, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash), nil
}

// TestBlkCp_Step1_CreateFiles creates test files with different contents
func TestBlkCp_Step1_CreateFiles(t *testing.T) {
	t.Log("\n=== BLKCP_TEST Step 1: Create test files with different contents ===")

	// Create blk_test5.log with pattern "5555..."
	file5Path := filepath.Join(blkcp5Config.StoragePath, blkcp5Config.TestFile)
	data5 := make([]byte, BLKCP_BLOCK_SIZE)
	for i := 0; i < BLKCP_BLOCK_SIZE; i++ {
		data5[i] = byte('5')
	}
	// Open with O_SYNC
	f5, err := os.OpenFile(file5Path, os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	_, err = f5.Write(data5)
	if err != nil {
		f5.Close()
		t.Fatalf("Failed to write: %v", err)
	}
	f5.Sync()  // 명시적 sync
	f5.Close()

	t.Logf("[%s] Created %s with pattern '5555...'", blkcp5Config.NodeName, file5Path)

	// Create blk_test6.log with pattern "6666..."
	file6Path := filepath.Join(blkcp6Config.StoragePath, blkcp6Config.TestFile)
	data6 := make([]byte, BLKCP_BLOCK_SIZE)
	for i := 0; i < BLKCP_BLOCK_SIZE; i++ {
		data6[i] = byte('6')
	}

	f6, err := os.OpenFile(file6Path, os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	_, err = f6.Write(data6)
	if err != nil {
		f6.Close()
		t.Fatalf("Failed to write: %v", err)
	}
	f6.Sync()  // 명시적 sync
	f6.Close()

	t.Logf("[%s] Created %s with pattern '6666...'", blkcp6Config.NodeName, file6Path)

	// Verify files are different
	hash5, err := calculateSHA256(file5Path)
	if err != nil {
		t.Fatalf("Failed to calculate hash for %s: %v", file5Path, err)
	}

	hash6, err := calculateSHA256(file6Path)
	if err != nil {
		t.Fatalf("Failed to calculate hash for %s: %v", file6Path, err)
	}

	t.Logf("Initial hashes:")
	t.Logf("  %s: %s", blkcp5Config.TestFile, hash5)
	t.Logf("  %s: %s", blkcp6Config.TestFile, hash6)

	if hash5 == hash6 {
		t.Fatal("Files should have different content initially!")
	}

	t.Log("✓ Files created with different contents")

	// Flush
	t.Log("Syncing files to disk...")
	err = exec.Command("sync").Run()
	if err != nil {
		t.Logf("Warning: sync failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // wait for sync
}

// TestBlkCp_Step2_GetPBAs gets physical block addresses of both files
func TestBlkCp_Step2_GetPBAs(t *testing.T) {
	t.Log("\n=== BLKCP_TEST Step 2: Get PBA of test files ===")

	// Get PBA of blk_test5.log
	file5Path := filepath.Join(blkcp5Config.StoragePath, blkcp5Config.TestFile)
	seg5, err := l_get_pba(file5Path, 0, BLKCP_BLOCK_SIZE)
	if err != nil {
		t.Fatalf("[%s] Failed to get PBA for %s: %v", blkcp5Config.NodeName, blkcp5Config.TestFile, err)
	}
	t.Logf("[%s] %s PBA: 0x%X (%d)", blkcp5Config.NodeName, blkcp5Config.TestFile, seg5.PBA, seg5.PBA)
	t.Logf("[%s] Length: %d bytes", blkcp5Config.NodeName, seg5.Len)

	// Get PBA of blk_test6.log
	file6Path := filepath.Join(blkcp6Config.StoragePath, blkcp6Config.TestFile)
	seg6, err := l_get_pba(file6Path, 0, BLKCP_BLOCK_SIZE)
	if err != nil {
		t.Fatalf("[%s] Failed to get PBA for %s: %v", blkcp6Config.NodeName, blkcp6Config.TestFile, err)
	}
	t.Logf("[%s] %s PBA: 0x%X (%d)", blkcp6Config.NodeName, blkcp6Config.TestFile, seg6.PBA, seg6.PBA)
	t.Logf("[%s] Length: %d bytes", blkcp6Config.NodeName, seg6.Len)

	// Verify we got valid PBAs
	if seg5.PBA == 0 || seg6.PBA == 0 {
		t.Fatal("Invalid PBA: PBA should not be 0")
	}

	t.Log("✓ PBAs retrieved successfully")
}

// TestBlkCp_Step3_CopyBlock copies blk_test5.log to blk_test6.log at storage level
func TestBlkCp_Step3_CopyBlock(t *testing.T) {
	t.Log("\n=== BLKCP_TEST Step 3: Copy block from blk_test5.log to blk_test6.log ===")

	// Get source PBA (blk_test5.log)
	file5Path := filepath.Join(blkcp5Config.StoragePath, blkcp5Config.TestFile)
	seg5, err := l_get_pba(file5Path, 0, BLKCP_BLOCK_SIZE)
	if err != nil {
		t.Fatalf("Failed to get source PBA: %v", err)
	}
	t.Logf("Source PBA (blk_test5.log): 0x%X", seg5.PBA)

	// Get destination PBA (blk_test6.log)
	file6Path := filepath.Join(blkcp6Config.StoragePath, blkcp6Config.TestFile)
	seg6, err := l_get_pba(file6Path, 0, BLKCP_BLOCK_SIZE)
	if err != nil {
		t.Fatalf("Failed to get destination PBA: %v", err)
	}
	t.Logf("Destination PBA (blk_test6.log): 0x%X", seg6.PBA)

	// Perform storage-level block copy
	t.Logf("Performing block copy: 0x%X -> 0x%X (%d bytes)", seg5.PBA, seg6.PBA, BLKCP_BLOCK_SIZE)
	err = r_write_pba(BLKCP_DEVICE_PATH, seg5.PBA, seg6.PBA, uint64(BLKCP_BLOCK_SIZE))
	if err != nil {
		t.Fatalf("Block copy failed: %v", err)
	}

	t.Log("✓ Block copy completed successfully")
}

// TestBlkCp_Step5_VerifyContent verifies files are identical after copy
func TestBlkCp_Step5_VerifyContent(t *testing.T) {
	t.Log("=== BLKCP_TEST Step 4: Verify file contents are identical ===")

		// ★ 추가: 페이지 캐시 비우기
	t.Log("Dropping filesystem page cache...")
	dropCmd := []byte("3\n")
	err := ioutil.WriteFile("/proc/sys/vm/drop_caches", dropCmd, 0655)
	if err != nil {
		t.Logf("Warning: Failed to drop caches: %v", err)
	} else {
		t.Log("✓ Page cache dropped successfully")
	}

	file5Path := filepath.Join(blkcp5Config.StoragePath, blkcp5Config.TestFile)
	file6Path := filepath.Join(blkcp6Config.StoragePath, blkcp6Config.TestFile)

	// Calculate hashes
	hash5, err := calculateSHA256(file5Path)
	if err != nil {
		t.Fatalf("Failed to calculate hash for %s: %v", file5Path, err)
	}

	hash6, err := calculateSHA256(file6Path)
	if err != nil {
		t.Fatalf("Failed to calculate hash for %s: %v", file6Path, err)
	}

	t.Logf("After copy hashes:")
	t.Logf("  %s: %s", blkcp5Config.TestFile, hash5)
	t.Logf("  %s: %s", blkcp6Config.TestFile, hash6)

	// Compare hashes
	if hash5 != hash6 {
		t.Errorf("HASH MISMATCH!")
		t.Errorf("  %s: %s", blkcp5Config.TestFile, hash5)
		t.Errorf("  %s: %s", blkcp6Config.TestFile, hash6)
		t.Fatal("TEST FAILED: Files should be identical after block copy")
	}

	// Read and compare first few bytes for detailed verification
	data5, err := ioutil.ReadFile(file5Path)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", file5Path, err)
	}

	data6, err := ioutil.ReadFile(file6Path)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", file6Path, err)
	}

	if len(data5) != len(data6) {
		t.Fatalf("File sizes differ: %d vs %d", len(data5), len(data6))
	}

	// Show first 16 bytes for verification
	t.Logf("First 16 bytes of %s: %q", blkcp5Config.TestFile, data5[:16])
	t.Logf("First 16 bytes of %s: %q", blkcp6Config.TestFile, data6[:16])

	// Verify blk_test6.log now contains '5555...' pattern
	for i := 0; i < 16; i++ {
		if data6[i] != '5' {
			t.Errorf("Expected '5' at position %d, got '%c'", i, data6[i])
		}
	}

	t.Log("✓ SUCCESS: Files are identical after block copy")
	t.Logf("✓ blk_test6.log now contains the same content as blk_test5.log")
}

// TestBlkCp_Complete runs the complete block copy test sequence
func TestBlkCp_Complete(t *testing.T) {
	t.Log("==================================================")
	t.Log("        BLKCP_TEST - Complete Test Suite")
	t.Log("==================================================")
	t.Log("")
	t.Log("Configuration:")
	t.Logf("  Computing Node 1: %s (ID: %d)", blkcp5Config.NodeName, blkcp5Config.NodeID)
	t.Logf("  Computing Node 2: %s (ID: %d)", blkcp6Config.NodeName, blkcp6Config.NodeID)
	t.Logf("  Storage Path: %s", blkcp5Config.StoragePath)
	t.Logf("  Device Path: %s", BLKCP_DEVICE_PATH)
	t.Logf("  Test File 1: %s", blkcp5Config.TestFile)
	t.Logf("  Test File 2: %s", blkcp6Config.TestFile)
	t.Logf("  Block Size: %d bytes", BLKCP_BLOCK_SIZE)
	t.Log("")

	// Step 1: Create files with different contents
	t.Run("Step1_CreateFiles", func(t *testing.T) {
		TestBlkCp_Step1_CreateFiles(t)
	})

	// Step 2: Get PBAs
	t.Run("Step2_GetPBAs", func(t *testing.T) {
		TestBlkCp_Step2_GetPBAs(t)
	})

	// Step 3: Copy block at storage level
	t.Run("Step3_CopyBlock", func(t *testing.T) {
		TestBlkCp_Step3_CopyBlock(t)
	})

	// Step 4: Verify content
	t.Run("Step5_VerifyContent", func(t *testing.T) {
		TestBlkCp_Step5_VerifyContent(t)
	})

	t.Log("")
	t.Log("==================================================")
	t.Log("        BLKCP_TEST - ALL TESTS PASSED")
	t.Log("==================================================")
}

// TestBlkCp_Cleanup removes test files
func TestBlkCp_Cleanup(t *testing.T) {
	t.Log("=== BLKCP_TEST Cleanup ===")

	file5Path := filepath.Join(blkcp5Config.StoragePath, blkcp5Config.TestFile)
	if err := os.Remove(file5Path); err != nil {
		t.Logf("Warning: Failed to remove %s: %v", file5Path, err)
	} else {
		t.Logf("Removed: %s", file5Path)
	}

	file6Path := filepath.Join(blkcp6Config.StoragePath, blkcp6Config.TestFile)
	if err := os.Remove(file6Path); err != nil {
		t.Logf("Warning: Failed to remove %s: %v", file6Path, err)
	} else {
		t.Logf("Removed: %s", file6Path)
	}
}

// TestBlkCp_Permissions checks if we have required permissions for block copy
func TestBlkCp_Permissions(t *testing.T) {
	t.Log("=== BLKCP_TEST Permission Check ===")

	// Check device access
	_, err := os.Stat(BLKCP_DEVICE_PATH)
	if err != nil {
		t.Logf("⚠️  Cannot access device %s: %v", BLKCP_DEVICE_PATH, err)
		t.Logf("⚠️  Block copy operations require root or sudo access")
		t.Logf("⚠️  Run tests with: sudo -E go test -v -run TestBlkCp_Complete")
		t.Skip("Device access required for block copy tests")
	}

	// Check if we can open device for reading
	f, err := os.OpenFile(BLKCP_DEVICE_PATH, os.O_RDONLY, 0)
	if err != nil {
		t.Logf("⚠️  Cannot open device for reading: %v", err)
		t.Logf("⚠️  Block copy requires read/write access to %s", BLKCP_DEVICE_PATH)
		t.Skip("Insufficient permissions for block copy tests")
	}
	f.Close()

	t.Log("✓ Device access permissions OK")
	t.Logf("✓ Device: %s", BLKCP_DEVICE_PATH)
}
