package checkout

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// initialUserNamespace is the host's user namespace as /proc/self/ns/user names it: the kernel
// fixes its inode (PROC_USER_INIT_INO).
const initialUserNamespace = "user:[4026531837]"

// Launch is how a role with virtual checkouts is started: in a user namespace of its own, where it
// is root, and a mount namespace that namespace owns, every mount private (docs/okfs.md, "ok's
// roles"). As root, map the ids the role's files belong to as well (--map-users, --map-groups).
const Launch = "unshare --map-root-user --mount --propagation private -- ok …"

// RequireIsolated checks that this process can present virtual checkouts, and keeps what they
// mount here: it must run in a user and mount namespace of its own, where the okfs daemon it starts
// may mount, and it makes every mount in that namespace private, so a presentation is seen only
// inside. ok never makes the namespaces itself: only a single-threaded process can enter a new user
// namespace, which a Go program never is, so whatever starts ok makes them (Launch), as the chart
// does. Call it in main, before starting anything.
func RequireIsolated() error {
	ns, err := os.Readlink("/proc/self/ns/user")
	if err != nil {
		return fmt.Errorf("reading this process's user namespace: %w", err)
	}
	return isolated(ns, func() error {
		return unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, "")
	}, userNamespaceRestriction())
}

// isolated is RequireIsolated given this process's user namespace, how to make its mounts private,
// and what on this host restricts user namespaces ("" when nothing does).
func isolated(userNS string, makePrivate func() error, restriction string) error {
	if userNS == initialUserNamespace {
		err := fmt.Errorf("a virtual checkout mounts okfs in a user and mount namespace of the role's own, and this process runs in the host's: "+
			"start it in one (%s), or check out real (worker.checkout, deploy.checkout)", Launch)
		if restriction != "" {
			err = fmt.Errorf("%w; and %s", err, restriction)
		}
		return err
	}
	if err := makePrivate(); err != nil {
		return fmt.Errorf("making this process's mounts private: %w: it runs in a user namespace of its own, but holds no capability over its "+
			"mount namespace — start it in a mount namespace that user namespace owns, as root there (%s)", err, Launch)
	}
	return nil
}
