package checkout

import (
	"os"
	"strings"
)

// UserNamespaces says why this host refuses the user namespaces a worker's sandbox and a virtual
// checkout's launcher make, as its kernel settings tell — what to change, and where
// (docs/install.md, "Workers' nodes").
func UserNamespaces() string {
	if r := userNamespaceRestriction(); r != "" {
		return r
	}
	return "the container may not make user namespaces: its seccomp profile must allow them (the chart runs workers and the deployer " +
		"with seccomp Unconfined), and so must its node (docs/install.md, \"Workers' nodes\")"
}

// userNamespaceRestriction is what in this host's kernel settings restricts unprivileged user
// namespaces, and what to change; "" when nothing does.
func userNamespaceRestriction() string {
	sysctl := func(name string) string {
		b, err := os.ReadFile("/proc/sys/" + name)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(b))
	}
	switch {
	case sysctl("kernel/apparmor_restrict_unprivileged_userns") == "1":
		return "this node restricts unprivileged user namespaces through AppArmor " +
			"(kernel.apparmor_restrict_unprivileged_userns = 1, Ubuntu 23.10's default and later's): " +
			"run ok's workers and deployer under the ok-sandbox AppArmor profile (deploy/apparmor), or set the sysctl to 0 on the node " +
			"(docs/install.md, \"Workers' nodes\")"
	case sysctl("user/max_user_namespaces") == "0":
		return "this node allows no user namespaces (user.max_user_namespaces = 0): raise it on the node " +
			"(docs/install.md, \"Workers' nodes\")"
	case sysctl("kernel/unprivileged_userns_clone") == "0":
		return "this node allows no unprivileged user namespaces (kernel.unprivileged_userns_clone = 0): set it to 1 on the node " +
			"(docs/install.md, \"Workers' nodes\")"
	}
	return ""
}
