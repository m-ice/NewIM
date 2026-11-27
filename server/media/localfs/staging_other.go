//go:build !linux

package localfs

import "os"

func openUnlinked(root *os.Root) (*os.File, error) {
	return createAndUnlink(root)
}
