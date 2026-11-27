//go:build !darwin

package localfs

import "os"

func rootACLRestricted(*os.File) bool { return true }
