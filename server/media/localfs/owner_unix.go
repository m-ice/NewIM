//go:build darwin || linux

package localfs

import (
	"errors"
	"os"
	"syscall"
)

func checkRootOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("missing ownership metadata")
	}
	if int(stat.Uid) != os.Geteuid() {
		return errors.New("root owner mismatch")
	}
	return nil
}
