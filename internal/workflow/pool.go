package workflow

import (
	"fmt"
	"regexp"
)

// DefaultPool is the pool of workers a job runs in when neither it nor its repository names one.
const DefaultPool = "default"

var poolName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// ValidPool reports whether a pool's name is one: lowercase letters, digits, and -, at most 63,
// starting and ending with a letter or digit — a Kubernetes label's value, as a pool's workers
// are labeled.
func ValidPool(name string) error {
	if !poolName.MatchString(name) {
		return fmt.Errorf("pool %q is lowercase letters, digits, and -, at most 63, starting and ending with a letter or digit", name)
	}
	return nil
}

// PoolOf is the pool a job runs in: its own, else its repository's, else the default.
func PoolOf(job, repository string) string {
	switch {
	case job != "":
		return job
	case repository != "":
		return repository
	}
	return DefaultPool
}
