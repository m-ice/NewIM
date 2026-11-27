package localfs

import (
	"os"

	"github.com/m-ice/NewIM/server/credentialfile"
)

func rootACLRestricted(dir *os.File) bool {
	return credentialfile.ACLIsRestricted(dir)
}
