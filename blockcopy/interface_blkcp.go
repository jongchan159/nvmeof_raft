package blockcopy

/*
#cgo CFLAGS: -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64
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

#define ALIGN 4096				// alignment for O_DIRECT (4KB page boundary)
#define EXTENTS_MAX 1			// iterate with single extent; increase if needed
#define DEVICE_BLOCK_SIZE 512	// NVMe sector size

// c_get_pba: retrieves physical block address for a given logical file offset.
// Uses FIEMAP ioctl to map logical offset -> physical block address.
//
// Parameters:
//   fd      - open file descriptor (O_RDONLY)
//   logical - logical byte offset within the file
//   length  - byte length of the region to query
//   out_pba - output: physical block address (bytes from device start)
//   out_len - output: length of the mapped region
//
// Returns 0 on success, -1 on failure.
static int c_get_pba(int fd, off_t logical, size_t length,
						uint64_t* out_pba, size_t* out_len) {
	size_t size = sizeof(struct fiemap) + EXTENTS_MAX * sizeof(struct fiemap_extent);
	struct fiemap *fiemap = (struct fiemap*)calloc(1, size);
	if (!fiemap) return -1;

	fiemap->fm_start        = logical;
	fiemap->fm_length       = length;
	fiemap->fm_extent_count = EXTENTS_MAX;

	int result = 0;

	if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
		perror("ioctl fiemap");
		result = -1;
		goto exit;
	}

	if (fiemap->fm_mapped_extents > EXTENTS_MAX) {
		fprintf(stderr, "More mapped extents needed: mapped %u, max %d\n",
				fiemap->fm_mapped_extents, EXTENTS_MAX);
		result = -1;
		goto exit;
	}

	if (fiemap->fm_mapped_extents == 0) {
		fprintf(stderr, "no extents mapped at logical %ld\n", (long)logical);
		result = -1;
		goto exit;
	}

	{
		// compute exact PBA: extent physical base + intra-extent offset
		struct fiemap_extent *e = &fiemap->fm_extents[0];
		*out_pba = e->fe_physical + (logical - e->fe_logical);
		*out_len = length;
	}

exit:
	free(fiemap);
	return result;
}

// c_direct_write: writes arbitrary data to a physical block address on a block
// device using O_DIRECT. Handles unaligned pba or nbytes via read-modify-write
// within 4KB-aligned pages so O_DIRECT constraints are always satisfied.
//
// Parameters:
//   fd     - open device file descriptor (O_RDWR | O_DIRECT)
//   pba    - target physical byte address (need not be ALIGN-aligned)
//   data   - bytes to write
//   nbytes - number of bytes to write (need not be ALIGN-aligned)
//
// Returns 0 on success, -1 on failure.
static int c_direct_write(int fd, uint64_t pba, const void* data, size_t nbytes) {
	if (nbytes == 0) return 0;

	// Compute the aligned range that covers [pba, pba+nbytes)
	uint64_t start_page = pba & ~(uint64_t)(ALIGN - 1);
	uint64_t end        = pba + nbytes;
	uint64_t end_page   = (end + ALIGN - 1) & ~(uint64_t)(ALIGN - 1);
	size_t   total      = (size_t)(end_page - start_page);

	void *buf;
	if (posix_memalign(&buf, ALIGN, total) != 0) {
		perror("c_direct_write: posix_memalign");
		return -1;
	}

	// Read existing data for the aligned range (read-modify-write)
	ssize_t r = pread(fd, buf, total, (off_t)start_page);
	if (r != (ssize_t)total) {
		fprintf(stderr, "c_direct_write: pread failed at 0x%lx total=%zu returned=%zd errno=%d\n",
				(unsigned long)start_page, total, r, errno);
		free(buf);
		return -1;
	}

	// Overlay caller's data at the correct offset within the aligned buffer
	size_t offset = (size_t)(pba - start_page);
	memcpy((char*)buf + offset, data, nbytes);

	// Write back the full aligned range
	ssize_t w = pwrite(fd, buf, total, (off_t)start_page);
	free(buf);
	if (w != (ssize_t)total) {
		fprintf(stderr, "c_direct_write: pwrite failed at 0x%lx total=%zu returned=%zd errno=%d\n",
				(unsigned long)start_page, total, w, errno);
		return -1;
	}

	ioctl(fd, BLKFLSBUF, 0);
	return 0;
}

// c_write_pba: copies nbytes from pba_src to pba_dst on the same block device.
// Uses O_DIRECT aligned I/O to bypass the page cache.
//
// Parameters:
//   fd      - open device file descriptor (O_RDWR | O_DIRECT)
//   pba_src - source physical byte address
//   pba_dst - destination physical byte address
//   nbytes  - number of bytes to copy (must be ALIGN-aligned)
//
// Returns 0 on success, -1 on failure.
static int c_write_pba(int fd, uint64_t pba_src, uint64_t pba_dst, size_t nbytes) {
	void *buf;
	if (posix_memalign(&buf, ALIGN, nbytes) != 0) {
		perror("posix_memalign");
		return -1;
	}

	// read from source PBA
	ssize_t r = pread(fd, buf, nbytes, (off_t)pba_src);
	if (r != (ssize_t)nbytes) {
		perror("pread");
		free(buf);
		return -1;
	}

	// write to destination PBA
    ssize_t w = pwrite(fd, buf, nbytes, (off_t)pba_dst);
    if (w != (ssize_t)nbytes) {
        fprintf(stderr, "pwrite failed: dst=0x%lx nbytes=%zu returned=%zd errno=%d\n",
                (unsigned long)pba_dst, nbytes, w, errno);
        free(buf);
        return -errno;  // return errno
    }

	ioctl(fd, BLKFLSBUF, 0);
	free(buf);
	return 0;
}
*/
import "C"
import (
	"fmt"
	"syscall"
	"unsafe"
)

// PBASegment represents a contiguous physical block address region.
type PBASegment struct {
	PBA uint64 // physical byte address from device start
	Len uint64 // length in bytes
}

// L_get_pba retrieves the physical block address for a logical offset in a file.
// Wraps c_get_pba via FIEMAP ioctl.
//
// Parameters:
//
//	filePath - path to the file (must be on a FIEMAP-capable filesystem, e.g. ext4)
//	logical  - logical byte offset within the file
//	length   - byte length of the region to query
//
// Returns PBASegment and nil error on success.
func L_get_pba(filePath string, logical int64, length uint64) (PBASegment, error) {
	fd, err := syscall.Open(filePath, syscall.O_RDONLY, 0)
	if err != nil {
		return PBASegment{}, fmt.Errorf("L_get_pba: open %s: %v", filePath, err)
	}
	defer syscall.Close(fd)

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
		return PBASegment{}, fmt.Errorf("L_get_pba: c_get_pba failed (logical=%d, len=%d)", logical, length)
	}

	return PBASegment{
		PBA: uint64(outPBA),
		Len: uint64(outLen),
	}, nil
}

// R_write_pba copies nbytes from pbaSrc to pbaDst on the block device at devicePath.
// Wraps c_write_pba using O_DIRECT aligned I/O.
//
// Parameters:
//
//	devicePath - path to the NVMe-oF block device
//	pbaSrc     - source physical byte address
//	pbaDst     - destination physical byte address
//	nbytes     - number of bytes to copy (rounded up to ALIGN=4096 internally)
//
// Returns nil on success.
func R_write_pba(devicePath string, pbaSrc uint64, pbaDst uint64, nbytes uint64) error {
	fd, err := syscall.Open(devicePath, syscall.O_RDWR|syscall.O_DIRECT|syscall.O_SYNC, 0)
	if err != nil {
		return fmt.Errorf("R_write_pba: open %s: %v", devicePath, err)
	}
	defer syscall.Close(fd)

	ret := C.c_write_pba(
		C.int(fd),
		C.uint64_t(pbaSrc),
		C.uint64_t(pbaDst),
		C.size_t(nbytes),
	)
	if ret != 0 {
		return fmt.Errorf("R_write_pba: c_write_pba failed (src=%d, dst=%d, nbytes=%d)", pbaSrc, pbaDst, nbytes)
	}
	return nil
}

// AlignUp rounds nbytes up to the nearest ALIGN (4096) boundary.
// Required because O_DIRECT mandates 4KB-aligned transfer sizes.
func AlignUp(nbytes uint64) uint64 {
	const align = 4096
	return (nbytes + align - 1) &^ (align - 1)
}

// GetBlockSize returns the O_DIRECT alignment size (4096 bytes).
func GetBlockSize() uint64 {
	return uint64(C.ALIGN)
}

// DirectWrite writes data to the specified physical byte address on devicePath
// using O_DIRECT, bypassing the page cache. Unaligned pba or sizes are handled
// transparently via read-modify-write within 4KB pages.
func DirectWrite(devicePath string, pba uint64, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	fd, err := syscall.Open(devicePath, syscall.O_RDWR|syscall.O_DIRECT|syscall.O_SYNC, 0)
	if err != nil {
		return fmt.Errorf("DirectWrite: open %s: %v", devicePath, err)
	}
	defer syscall.Close(fd)

	ret := C.c_direct_write(
		C.int(fd),
		C.uint64_t(pba),
		unsafe.Pointer(&data[0]),
		C.size_t(len(data)),
	)
	if ret != 0 {
		return fmt.Errorf("DirectWrite: c_direct_write failed (pba=0x%X, nbytes=%d)", pba, len(data))
	}
	return nil
}

// DirectRead reads nbytes from physical address pba using O_DIRECT.
// Buffer is 4KB-aligned via posix_memalign for O_DIRECT compatibility.
// Returns an error if the read fails (e.g. pba is beyond the device size).
func DirectRead(devicePath string, pba uint64, nbytes uint64) ([]byte, error) {
	aligned := AlignUp(nbytes)

	fd, err := syscall.Open(devicePath, syscall.O_RDONLY|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, fmt.Errorf("DirectRead: open %s: %v", devicePath, err)
	}
	defer syscall.Close(fd)

	// posix_memalign: allocate 4KB-aligned buffer
	var buf unsafe.Pointer
	ret := C.posix_memalign(&buf, C.size_t(C.ALIGN), C.size_t(aligned))
	if ret != 0 {
		return nil, fmt.Errorf("DirectRead: posix_memalign failed: %d", ret)
	}
	defer C.free(buf)

	r, err := C.pread(C.int(fd), buf, C.size_t(aligned), C.off_t(pba))
	if r != C.ssize_t(aligned) {
		return nil, fmt.Errorf("DirectRead: pread at 0x%X returned %d, err=%v", pba, r, err)
	}

	result := C.GoBytes(buf, C.int(aligned))
	return result, nil
}
