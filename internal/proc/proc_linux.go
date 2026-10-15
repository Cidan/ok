// Package proc sets what the processes ok starts carry where the platform has it.
package proc

import "syscall"

// DieWithParent has the kernel send sig to the child when this process dies, so nothing it
// started outlives it: Linux's parent-death signal. It returns attr, set.
func DieWithParent(attr *syscall.SysProcAttr, sig syscall.Signal) *syscall.SysProcAttr {
	attr.Pdeathsig = sig
	return attr
}
