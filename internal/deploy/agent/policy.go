package agent

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
)

// The cluster API calls an agent makes for a deployer (docs/hosted.md, "Deploy agents, as built"):
// what rolling and observing need (docs/deploy.md, "Grants"), and discovery — nothing else, so a
// deployer, whatever it asks, reads no Secret, runs no command in a pod, opens no proxy or port,
// mints no token, impersonates no one, and deletes nothing. The agent's ServiceAccount's RBAC
// bounds it further, to the namespaces its organization grants.

var (
	// label is a namespace's: a DNS label.
	label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	// subdomain is an object's name: a DNS subdomain.
	subdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)
)

// discovery are the paths client-go may read to learn what the server serves.
var discovery = map[string]bool{"/version": true, "/api": true, "/api/v1": true, "/apis": true, "/apis/apps": true, "/apis/apps/v1": true}

// Allowed refuses a request an agent does not make.
func Allowed(method, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "" || u.Host != "" || u.User != nil || u.RawPath != "" || !strings.HasPrefix(u.Path, "/") {
		return fmt.Errorf("%q is no path of the cluster's API", rawURL)
	}
	p := u.Path
	if path.Clean(p) != p {
		return fmt.Errorf("%q is no clean path", p)
	}
	read := method == http.MethodGet
	if discovery[p] {
		if read {
			return nil
		}
		return refused(method, p)
	}
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	ns := func(i int) bool { return len(parts) > i && label.MatchString(parts[i]) }
	name := func(i int) bool { return len(parts) > i && subdomain.MatchString(parts[i]) }
	switch {
	// Pods: read, never their exec, attach, port-forward, proxy, log, or ephemeral containers.
	case len(parts) == 5 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "namespaces" && ns(3) && parts[4] == "pods",
		len(parts) == 6 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "namespaces" && ns(3) && parts[4] == "pods" && name(5):
		if read {
			return nil
		}
	case len(parts) >= 6 && parts[0] == "apis" && parts[1] == "apps" && parts[2] == "v1" && parts[3] == "namespaces" && ns(4):
		switch res := parts[5]; {
		case res == "replicasets" && (len(parts) == 6 || len(parts) == 7 && name(6)):
			if read {
				return nil
			}
		case res == "deployments" || res == "statefulsets":
			switch {
			case len(parts) == 6:
				if read || method == http.MethodPost {
					return nil
				}
			case len(parts) == 7 && name(6), len(parts) == 8 && name(6) && (parts[7] == "scale" || parts[7] == "status"):
				if read || method == http.MethodPut || method == http.MethodPatch {
					return nil
				}
			}
		}
	}
	return refused(method, p)
}

func refused(method, p string) error {
	return fmt.Errorf("a deploy agent makes no %s %s: it reads and rolls Deployments and StatefulSets, and reads pods", method, p)
}

// forwarded are the request headers an agent passes on: none that authenticates, impersonates, or
// says where it came from.
var forwarded = []string{"Accept", "Content-Type", "User-Agent"}

// dropped are the answer's headers an agent keeps back: its connection's, and its encoding's, which
// it undid.
var dropped = map[string]bool{"Connection": true, "Keep-Alive": true, "Transfer-Encoding": true, "Trailer": true, "Upgrade": true,
	"Content-Length": true, "Content-Encoding": true}
