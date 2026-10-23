package main

import (
	"flag"
	"os"

	"github.com/Cidan/ok/internal/automation/microvm"
)

// workerVMExec runs a command in a run's microVM (docs/hosted.md, "Hosted automation, as built"):
// the wrapper every command of a run in a microVM runs under, its exit status the command's.
func workerVMExec(args []string) error {
	flags := flag.NewFlagSet("worker vm-exec", flag.ContinueOnError)
	sock := flags.String("socket", "", "the microVM's vsock socket")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if *sock == "" || flags.NArg() == 0 {
		return usageError("ok worker vm-exec -socket SOCKET -- COMMAND [ARG...]")
	}
	status, err := microvm.RunInGuest(*sock, flags.Args(), os.Stdout, os.Stderr)
	if err != nil {
		return err
	}
	if status < 0 {
		status = 128 - status // a signal's death, as a shell reports it
	}
	if status != 0 {
		return exitStatus(status)
	}
	return nil
}
