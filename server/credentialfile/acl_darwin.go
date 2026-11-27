package credentialfile

import (
	"syscall"
	"unsafe"
)

// A macOS ACL can grant access independently of the POSIX permission bits.
func aclIsRestricted(fd int) bool {
	const (
		attrBitMapCount  = 5
		extendedSecurity = 0x00400000
		reportFullSize   = 0x00000004
	)
	// attrlist and the fixed return fields from <sys/attr.h>.
	attributes := struct {
		bitmapCount uint16
		reserved    uint16
		common      uint32
		volume      uint32
		directory   uint32
		file        uint32
		fork        uint32
	}{bitmapCount: attrBitMapCount, common: extendedSecurity}
	var result struct {
		size   uint32
		offset int32
		length uint32
	}
	size := unsafe.Sizeof(result)
	_, _, errno := syscall.Syscall6(syscall.SYS_FGETATTRLIST, uintptr(fd),
		uintptr(unsafe.Pointer(&attributes)), uintptr(unsafe.Pointer(&result)), size, reportFullSize, 0)
	return errno == 0 && result.size == uint32(size) && result.length == 0
}
