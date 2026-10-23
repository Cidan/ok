package secrets

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// memoryDir is where a directory of secrets is made: /dev/shm, when it is a memory file system
// (tmpfs), as a container's and a Linux machine's is.
func memoryDir() (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs("/dev/shm", &st); err != nil {
		return "", fmt.Errorf("no memory file system for secrets at /dev/shm: %w", err)
	}
	if st.Type != unix.TMPFS_MAGIC {
		return "", fmt.Errorf("/dev/shm is no memory file system (tmpfs): secrets are written to none other")
	}
	return "/dev/shm", nil
}
