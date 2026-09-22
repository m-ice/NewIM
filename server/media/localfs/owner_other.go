//go:build !darwin && !linux

package localfs

import "os"

func checkRootOwner(os.FileInfo) error { return nil }
