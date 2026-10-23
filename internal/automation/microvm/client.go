package microvm

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// RunInGuest runs a command in a microVM's guest, through its vsock socket — in this process's
// directory and with its environment, as it would run here — copying its output to stdout and
// stderr, and returns its exit status: ok worker vm-exec. A first SIGTERM or SIGINT here terminates
// the command's process group in the guest, a second kills it.
func RunInGuest(sock string, argv []string, stdout, stderr io.Writer) (int, error) {
	dir, err := os.Getwd()
	if err != nil {
		return 0, err
	}
	c, err := DialGuest(sock, ExecPort)
	if err != nil {
		return 0, fmt.Errorf("reaching the microVM: %w", err)
	}
	defer c.Close()
	if err := writeLine(c, Request{Argv: argv, Dir: dir, Env: os.Environ()}); err != nil {
		return 0, err
	}
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)
	go func() {
		sent := Terminate
		for range sigs {
			if _, err := c.Write([]byte{sent}); err != nil {
				return
			}
			sent = Kill
		}
	}()
	for {
		kind, payload, err := ReadFrame(c)
		if err != nil {
			return 0, fmt.Errorf("the microVM's answer: %w", err)
		}
		switch kind {
		case FrameStdout:
			if _, err := stdout.Write(payload); err != nil {
				return 0, err
			}
		case FrameStderr:
			if _, err := stderr.Write(payload); err != nil {
				return 0, err
			}
		case FrameExit:
			return ExitStatus(payload)
		case FrameError:
			return 0, errors.New(string(payload))
		default:
			return 0, fmt.Errorf("the microVM's answer: a frame %q", kind)
		}
	}
}
