//go:build !linux

// Package proc sets what the processes ok starts carry where the platform has it.
package proc

import "syscall"

// DieWithParent returns attr as it is: no other platform has Linux's parent-death signal, so a
// child may outlive this process there.
func DieWithParent(attr *syscall.SysProcAttr, _ syscall.Signal) *syscall.SysProcAttr {
	return attr
}
