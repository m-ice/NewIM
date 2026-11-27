//go:build !darwin && !linux

package localfs

import "os"

const objectReadFlags = os.O_RDONLY

func checkRootOwner(os.FileInfo) error { return nil }
