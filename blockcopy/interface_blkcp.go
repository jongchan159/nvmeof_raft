package blockcopy

/*
#cgo CFLAGS: -D_GNU_SOURCE
#cgo LDFLAGS: -lm

#include <errno.h>
#include <fcntl.h>
#include <linux/fiemap.h>
#include <linux/fs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <stdint.h>

#define ALIGN 4096				// for O_DIRECT (4KB)
#define EXTENTS_MAX 1			// iteration with single extent. if need, modify this constant value
#define DEVICE_BLOCK_SIZE 512	// size of device sector (not use yet)

// Structure to hold physical block address segment
typedef struct {
    uint64_t pba;
    size_t len;
} pba_seg;

// Helper function to get physical block address from logical offset
static int c_get_pba(int fd, off_t logical, size_t length, 
						uint64_t* out_pba, size_t* out_len) {
    size_t size = sizeof(struct fiemap) + EXTENTS_MAX * sizeof(struct fiemap_extent);
    struct fiemap *fiemap = (struct fiemap*)calloc(1, size);
    if(!fiemap) return -1;

    fiemap->fm_start = logical;
    fiemap->fm_length = length;
    fiemap->fm_extent_count = EXTENTS_MAX;

    int result = 0;

    if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
        perror("ioctl fiemap");
        result = -1;
        goto exit;
    }
    
    if (fiemap->fm_mapped_extents > EXTENTS_MAX) {
        fprintf(stderr, "More mapped extents needed: mapped %ld, but need %u\n", 
                (long)EXTENTS_MAX, fiemap->fm_mapped_extents);
        result = -1;
        goto exit;
    }
    
    if (fiemap->fm_mapped_extents == 0) {
        fprintf(stderr, "no extents mapped at logical %ld\n", (long)logical);
        result = -1;
        goto exit;
    }

    // Get first extent
    struct fiemap_extent *e = &fiemap->fm_extents[0];
    *out_pba = e->fe_physical + (logical - e->fe_logical);
    *out_len = length;

exit:
    free(fiemap);
    return result;
}

// Helper function to write from source PBA to destination PBA
static int c_write_pba(int fd, uint64_t pba_src, uint64_t pba_dst, size_t nbytes) {
    void *buf;
    if (posix_memalign(&buf, ALIGN, nbytes) != 0) {
        perror("posix_memalign");
        return -1;
    }

    // Read from source PBA
    ssize_t r = pread(fd, buf, nbytes, pba_src);
    if (r != (ssize_t)nbytes) {
        perror("pread");
        free(buf);
        return -1;
    }

    // Write to destination PBA
    ssize_t w = pwrite(fd, buf, nbytes, pba_dst);
    if (w != (ssize_t)nbytes) {
        perror("pwrite");
        free(buf);
        return -1;
    }

    free(buf);
    return 0;
}
*/
import "C"
import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// PBASegment represents a physical block address segment
type PBASegment struct {
	PBA uint64
	Len uint64
}

// l_get_pba gets the physical block address for a logical offset in a file
// This is the local (leader) version of get_pba
//
// Parameters:
//   - filePath: path to the file to query
//   - logical: logical offset in the file
//   - length: length of the region to query
//
// Returns:
//   - PBASegment: physical block address and length
//   - error: error if operation fails
func l_get_pba(filePath string, logical int64, length uint64) (PBASegment, error) {
	// Open file with O_RDONLY flag
	fd, err := syscall.Open(filePath, syscall.O_RDONLY, 0)
	if err != nil {
		return PBASegment{}, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer syscall.Close(fd)

	var outPBA C.uint64_t
	var outLen C.size_t

	// Call C function to get PBA
	ret := C.c_get_pba(
		C.int(fd),
		C.off_t(logical),
		C.size_t(length),
		(*C.uint64_t)(unsafe.Pointer(&outPBA)),
		(*C.size_t)(unsafe.Pointer(&outLen)),
	)

	if ret != 0 {
		return PBASegment{}, fmt.Errorf("c_get_pba failed with code %d", ret)
	}

	return PBASegment{
		PBA: uint64(outPBA),
		Len: uint64(outLen),
	}, nil
}

// r_write_pba performs a block copy operation from source PBA to destination PBA
// This is the remote (follower) version of write_pba
//
// Parameters:
//   - devicePath: path to the block device (e.g., /dev/nvme0n1)
//   - pbaSrc: source physical block address
//   - pbaDst: destination physical block address
//   - nbytes: number of bytes to copy
//
// Returns:
//   - error: error if operation fails
func r_write_pba(devicePath string, pbaSrc uint64, pbaDst uint64, nbytes uint64) error {
	// Open device with O_RDWR and O_DIRECT flags
	fd, err := syscall.Open(devicePath, syscall.O_RDWR|syscall.O_DIRECT, 0)
	if err != nil {
		return fmt.Errorf("failed to open device %s: %v", devicePath, err)
	}
	defer syscall.Close(fd)

	// Call C function to perform PBA write
	ret := C.c_write_pba(
		C.int(fd),
		C.uint64_t(pbaSrc),
		C.uint64_t(pbaDst),
		C.size_t(nbytes),
	)

	if ret != 0 {
		return fmt.Errorf("c_write_pba failed with code %d", ret)
	}

	return nil
}

// l_get_pba_batch gets physical block addresses for multiple logical offsets
// This is a batch version for efficiency
//
// Parameters:
//   - filePath: path to the file to query
//   - logicals: slice of logical offsets
//   - length: length of each region (assumes uniform block size)
//
// Returns:
//   - []PBASegment: slice of physical block address segments
//   - error: error if any operation fails
func l_get_pba_batch(filePath string, logicals []int64, length uint64) ([]PBASegment, error) {
	// Open file once for all operations
	fd, err := syscall.Open(filePath, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer syscall.Close(fd)

	results := make([]PBASegment, len(logicals))

	for i, logical := range logicals {
		var outPBA C.uint64_t
		var outLen C.size_t

		ret := C.c_get_pba(
			C.int(fd),
			C.off_t(logical),
			C.size_t(length),
			(*C.uint64_t)(unsafe.Pointer(&outPBA)),
			(*C.size_t)(unsafe.Pointer(&outLen)),
		)

		if ret != 0 {
			return nil, fmt.Errorf("c_get_pba failed for logical offset %d (index %d): code %d", logical, i, ret)
		}

		results[i] = PBASegment{
			PBA: uint64(outPBA),
			Len: uint64(outLen),
		}
	}

	return results, nil
}

// r_write_pba_batch performs multiple block copy operations in batch
// This is a batch version for efficiency
//
// Parameters:
//   - devicePath: path to the block device
//   - pbaSrcs: slice of source physical block addresses
//   - pbaDsts: slice of destination physical block addresses
//   - nbytes: number of bytes to copy for each operation
//
// Returns:
//   - error: error if any operation fails
func r_write_pba_batch(devicePath string, pbaSrcs []uint64, pbaDsts []uint64, nbytes uint64) error {
	if len(pbaSrcs) != len(pbaDsts) {
		return fmt.Errorf("pbaSrcs and pbaDsts must have the same length")
	}

	// Open device once for all operations
	fd, err := syscall.Open(devicePath, syscall.O_RDWR|syscall.O_DIRECT, 0)
	if err != nil {
		return fmt.Errorf("failed to open device %s: %v", devicePath, err)
	}
	defer syscall.Close(fd)

	for i := range pbaSrcs {
		ret := C.c_write_pba(
			C.int(fd),
			C.uint64_t(pbaSrcs[i]),
			C.uint64_t(pbaDsts[i]),
			C.size_t(nbytes),
		)

		if ret != 0 {
			return fmt.Errorf("c_write_pba failed for operation %d (src: %d, dst: %d): code %d", 
				i, pbaSrcs[i], pbaDsts[i], ret)
		}
	}

	return nil
}

// Helper function to check if a file exists and is accessible
func checkFileAccess(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("file access check failed for %s: %v", path, err)
	}
	return nil
}

// GetBlockSize returns the default block size used for operations
func GetBlockSize() uint64 {
	return uint64(C.ALIGN)
}
