//go:build linux

package microvm

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// Start boots a run's microVM, and waits for it to take commands.
func Start(ctx context.Context, cfg Config, o Options) (*VM, error) {
	dir, err := os.MkdirTemp("", "okvm")
	if err != nil {
		return nil, err
	}
	vm := &VM{cfg: cfg, dir: dir, sock: filepath.Join(dir, "v.sock"), done: make(chan struct{}), log: o.Log}
	if vm.log == nil {
		vm.log = slog.Default()
	}
	ok := false
	defer func() {
		if !ok {
			vm.Stop()
		}
	}()
	uid, gid := os.Getuid(), os.Getgid()
	var mounts []Mount
	shares := []Share{{Path: cfg.Root, ReadOnly: true}}
	for _, s := range cfg.systemShares() {
		shares = append(shares, Share{Path: s, ReadOnly: true})
	}
	shares = append(shares, o.Shares...)
	tags := []string{}
	for i, s := range shares {
		tag := "okroot"
		if i > 0 {
			tag = "fs" + strconv.Itoa(i)
			if err := MountPoint(cfg.Root, s.Path); err != nil {
				return nil, err
			}
			mounts = append(mounts, Mount{Tag: tag, Path: s.Path, ReadOnly: s.ReadOnly})
		}
		sock := filepath.Join(dir, tag+".sock")
		args := []string{"--socket-path=" + sock, "--shared-dir=" + s.Path, "--sandbox=none", "--cache=auto"}
		if s.ReadOnly {
			args = append(args, "--readonly")
		}
		cmd := exec.Command(cfg.Virtiofsd, args...)
		cmd.Stdout, cmd.Stderr = nil, nil
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("virtiofsd for %s: %w", s.Path, err)
		}
		vm.fsd = append(vm.fsd, cmd)
		if err := waitFor(ctx, sock); err != nil {
			return nil, fmt.Errorf("virtiofsd for %s: %w", s.Path, err)
		}
		tags = append(tags, "tag="+tag+",socket="+sock)
	}
	// What the guest is told as it boots, and where its connections out go.
	gc := GuestConfig{Mounts: mounts, UID: uid, GID: gid, Egress: o.Egress != nil, Cache: o.Cache != nil}
	if err := vm.listen(ConfigPort, func(c net.Conn) { defer c.Close(); _ = writeLine(c, gc) }); err != nil {
		return nil, err
	}
	if o.Egress != nil {
		e := *o.Egress
		if err := vm.listen(EgressPort, func(c net.Conn) { proxy(c, e) }); err != nil {
			return nil, err
		}
	}
	if o.Cache != nil {
		dial := o.Cache
		if err := vm.listen(CachePort, func(c net.Conn) {
			up, err := dial(context.Background())
			if err != nil {
				vm.log.Warn("microvm: dialing the remote cache", "err", err)
				c.Close()
				return
			}
			pipe(c, up)
		}); err != nil {
			return nil, err
		}
	}
	args := []string{"--kernel", cfg.Kernel,
		"--cmdline", "console=ttyS0 quiet panic=-1 root=okroot rootfstype=virtiofs ro init=/ok -- worker guest",
		"--cpus", "boot=" + strconv.Itoa(max(cfg.CPUs, 1)),
		"--memory", "size=" + strconv.Itoa(max(cfg.MemoryMiB, 256)) + "M,shared=on",
		"--vsock", "cid=3,socket=" + vm.sock,
		"--serial", "file=" + filepath.Join(dir, "console"),
		"--console", "off",
		"--fs"}
	args = append(args, tags...)
	vm.ch = exec.Command(cfg.Hypervisor, args...)
	var stderr bytes.Buffer
	vm.ch.Stdout, vm.ch.Stderr = &stderr, &stderr
	vm.ch.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if err := vm.ch.Start(); err != nil {
		return nil, fmt.Errorf("cloud-hypervisor: %w", err)
	}
	go func() { _ = vm.ch.Wait(); close(vm.done) }()
	if err := vm.ready(ctx); err != nil {
		return nil, fmt.Errorf("the microVM did not start: %w; its console: %s%s", err, vm.console(), strings.TrimSpace(stderr.String()))
	}
	ok = true
	return vm, nil
}
