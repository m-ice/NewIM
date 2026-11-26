package login

// Linux POSIX ACL permissions for additional users and groups are bounded by
// the group-class bits, which credential loading requires to be zero.
func issuerACLIsRestricted(_ int) bool {
	return true
}
