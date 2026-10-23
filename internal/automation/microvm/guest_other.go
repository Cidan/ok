//go:build !linux

package microvm

import "errors"

// Guest is a microVM's init, which runs on Linux alone.
func Guest() error { return errors.New("ok worker guest is a Linux microVM's init") }
