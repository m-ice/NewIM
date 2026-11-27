package credentialfile

// Linux POSIX ACL permissions for additional users and groups are bounded by
// the group-class bits, which credential loading requires to be zero.
func aclIsRestricted(_ int) bool {
	return true
}
