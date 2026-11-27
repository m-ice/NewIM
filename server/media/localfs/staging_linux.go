package localfs

import (
	"errors"
	"os"
	"syscall"
)

// Linux __O_TMPFILE is omitted from syscall on some architectures.
const anonymousFile = 0x400000 | syscall.O_DIRECTORY

func openUnlinked(root *os.Root) (*os.File, error) {
	file, err := root.OpenFile(".", anonymousFile|os.O_RDWR, 0o600)
	if errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.EISDIR) ||
		errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOSYS) {
		return createAndUnlink(root)
	}
	return file, err
}
