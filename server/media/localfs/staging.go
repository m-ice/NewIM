package localfs

import "os"

// Staging owns a private directory for unlinked temporary files.
type Staging struct {
	root *os.Root
}

func OpenStaging(path string) (*Staging, error) {
	root, _, err := openRoot(path)
	if err != nil {
		return nil, err
	}
	return &Staging{root: root}, nil
}

func (s *Staging) Close() error {
	return s.root.Close()
}

// Create returns a private seekable file with no directory entry.
func (s *Staging) Create() (*os.File, error) {
	return openUnlinked(s.root)
}

func createAndUnlink(root *os.Root) (*os.File, error) {
	name, file, err := createTemp(root, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	if err = root.Remove(name); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}
