//go:build !linux

package checkout

// UserNamespaces says why user namespaces cannot be made: elsewhere than Linux, there are none.
func UserNamespaces() string {
	return "user namespaces are Linux's: run ok's workers and deployer on Linux"
}
