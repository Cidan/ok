//go:build !linux

package checkout

import (
	"fmt"
	"runtime"
)

// RequireIsolated refuses: a virtual checkout's okfs daemon mounts Linux's FUSE, in a user and
// mount namespace only Linux makes. Elsewhere, check commits out real (worker.checkout,
// deploy.checkout).
func RequireIsolated() error {
	return fmt.Errorf("a virtual checkout needs Linux's FUSE and namespaces, and this is %s: check out real", runtime.GOOS)
}
