//go:build !linux

package secrets

import "errors"

// memoryDir is where a directory of secrets is made: on Linux alone, which ok's deployer and
// workers run on.
func memoryDir() (string, error) {
	return "", errors.New("secrets are written to a memory file system, which ok finds on Linux alone")
}
