//go:build linux

package microvm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// hostCID is the host's vsock context ID, as the guest reaches it.
const hostCID = 2

// outputGrace is how long a command's output is read after it exits.
const outputGrace = 2 * time.Second

// Guest is the microVM's init (ok worker guest): it mounts what the guest sees, reads its
// configuration from the host, and runs the host's commands until the host has it power off.
func Guest() error {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := guestMounts(); err != nil {
		return powerOff(log, fmt.Errorf("the guest's own mounts: %w", err))
	}
	if err := loopbackUp(); err != nil {
		return powerOff(log, fmt.Errorf("the loopback interface: %w", err))
	}
	cfg, err := readConfig()
	if err != nil {
		return powerOff(log, fmt.Errorf("the guest's configuration: %w", err))
	}
	for _, m := range cfg.Mounts {
		// Under the guest's own /tmp or /run, the host's mount point is hidden: made here.
		_ = os.MkdirAll(m.Path, 0o755)
		var flags uintptr
		if m.ReadOnly {
			flags = unix.MS_RDONLY
		}
		if err := unix.Mount(m.Tag, m.Path, "virtiofs", flags|unix.MS_NOSUID|unix.MS_NODEV, ""); err != nil {
			return powerOff(log, fmt.Errorf("mounting %s at %s: %w", m.Tag, m.Path, err))
		}
	}
	r := newReaper()
	if cfg.Egress {
		go forwardTCP(log, fmt.Sprintf("127.0.0.1:%d", ProxyPort), EgressPort)
	}
	if cfg.Cache {
		go forwardUnix(log, CacheSocket, cfg.UID, cfg.GID, CachePort)
	}
	l, err := listenVsock(ExecPort)
	if err != nil {
		return powerOff(log, fmt.Errorf("listening for commands: %w", err))
	}
	for {
		c, err := l.Accept()
		if err != nil {
			return powerOff(log, fmt.Errorf("accepting a command: %w", err))
		}
		go func() {
			defer c.Close()
			var req Request
			if err := readLine(c, &req); err != nil {
				return
			}
			if req.PowerOff {
				_ = powerOff(log, nil)
				return
			}
			serveExec(c, req, cfg, r)
		}()
	}
}

// guestMounts mounts the guest's own filesystems: its root is the host's, read-only.
func guestMounts() error {
	for _, m := range []struct {
		fstype, path, data string
		flags              uintptr
	}{
		{"proc", "/proc", "", unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC},
		{"sysfs", "/sys", "", unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC},
		{"devtmpfs", "/dev", "mode=0755", unix.MS_NOSUID},
		{"tmpfs", "/tmp", "mode=1777", unix.MS_NOSUID | unix.MS_NODEV},
		{"tmpfs", "/run", "mode=0755", unix.MS_NOSUID | unix.MS_NODEV},
	} {
		if err := unix.Mount(m.fstype, m.path, m.fstype, m.flags, m.data); err != nil && !errors.Is(err, unix.EBUSY) {
			return fmt.Errorf("%s at %s: %w", m.fstype, m.path, err)
		}
	}
	for _, d := range []string{"/dev/shm", "/dev/pts"} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	if err := unix.Mount("tmpfs", "/dev/shm", "tmpfs", unix.MS_NOSUID|unix.MS_NODEV, "mode=1777"); err != nil {
		return err
	}
	if err := unix.Mount("devpts", "/dev/pts", "devpts", unix.MS_NOSUID|unix.MS_NOEXEC, "newinstance,ptmxmode=0666"); err != nil {
		return err
	}
	return os.MkdirAll("/run/ok", 0o755)
}

// loopbackUp brings lo up: the guest's only interface, where its egress proxy listens.
func loopbackUp() error {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	ifr, err := unix.NewIfreq("lo")
	if err != nil {
		return err
	}
	if err := unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, ifr); err != nil {
		return err
	}
	ifr.SetUint16(ifr.Uint16() | unix.IFF_UP)
	return unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, ifr)
}

func readConfig() (GuestConfig, error) {
	var cfg GuestConfig
	var c net.Conn
	var err error
	for range 50 {
		if c, err = dialVsock(hostCID, ConfigPort); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return cfg, err
	}
	defer c.Close()
	err = readLine(c, &cfg)
	return cfg, err
}

// powerOff stops the guest: its filesystems' writes flushed to the host first.
func powerOff(log *slog.Logger, why error) error {
	if why != nil {
		log.Error("ok worker guest: stopping", "err", why)
	}
	unix.Sync()
	_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
	return why
}

// ─── vsock ───

func listenVsock(port uint32) (net.Listener, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.Bind(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_ANY, Port: port}); err != nil {
		unix.Close(fd)
		return nil, err
	}
	if err := unix.Listen(fd, 64); err != nil {
		unix.Close(fd)
		return nil, err
	}
	return &vsockListener{fd: fd}, nil
}

type vsockListener struct{ fd int }

func (l *vsockListener) Accept() (net.Conn, error) {
	nfd, _, err := unix.Accept4(l.fd, unix.SOCK_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return fileConn(nfd, "vsock-accepted")
}

func (l *vsockListener) Close() error   { return unix.Close(l.fd) }
func (l *vsockListener) Addr() net.Addr { return vsockAddr{} }

type vsockAddr struct{}

func (vsockAddr) Network() string { return "vsock" }
func (vsockAddr) String() string  { return "vsock" }

func dialVsock(cid, port uint32) (net.Conn, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.Connect(fd, &unix.SockaddrVM{CID: cid, Port: port}); err != nil {
		unix.Close(fd)
		return nil, err
	}
	return fileConn(fd, "vsock")
}

// fileConn is a connected socket's descriptor as a net.Conn, read and written blocking: the
// runtime's poller does not take AF_VSOCK's.
func fileConn(fd int, name string) (net.Conn, error) {
	return &fdConn{f: os.NewFile(uintptr(fd), name)}, nil
}

type fdConn struct{ f *os.File }

func (c *fdConn) Read(b []byte) (int, error)         { return c.f.Read(b) }
func (c *fdConn) Write(b []byte) (int, error)        { return c.f.Write(b) }
func (c *fdConn) Close() error                       { return c.f.Close() }
func (c *fdConn) LocalAddr() net.Addr                { return vsockAddr{} }
func (c *fdConn) RemoteAddr() net.Addr               { return vsockAddr{} }
func (c *fdConn) SetDeadline(t time.Time) error      { return nil }
func (c *fdConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *fdConn) SetWriteDeadline(t time.Time) error { return nil }

// CloseWrite half-closes the socket.
func (c *fdConn) CloseWrite() error { return unix.Shutdown(int(c.f.Fd()), unix.SHUT_WR) }

// forwardTCP forwards a loopback TCP address's connections to the host's vsock port.
func forwardTCP(log *slog.Logger, addr string, port uint32) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("ok worker guest: the egress proxy", "err", err)
		return
	}
	serveForward(log, l, port)
}

// forwardUnix forwards a Unix socket's connections, which uid and gid may make, to the host's
// vsock port.
func forwardUnix(log *slog.Logger, path string, uid, gid int, port uint32) {
	l, err := net.Listen("unix", path)
	if err != nil {
		log.Error("ok worker guest: the remote cache's socket", "err", err)
		return
	}
	if err := os.Chown(path, uid, gid); err != nil {
		log.Error("ok worker guest: the remote cache's socket", "err", err)
	}
	serveForward(log, l, port)
}

func serveForward(log *slog.Logger, l net.Listener, port uint32) {
	for {
		c, err := l.Accept()
		if err != nil {
			log.Error("ok worker guest: forwarding", "err", err)
			return
		}
		go func() {
			h, err := dialVsock(hostCID, port)
			if err != nil {
				c.Close()
				return
			}
			pipe(c, h)
		}()
	}
}

// ─── Commands ───

// reaper reaps every child of init — the commands it starts, and what they leave behind — handing
// each command's status to whoever waits for it.
type reaper struct {
	mu      sync.Mutex
	waiting map[int]chan unix.WaitStatus
	exited  map[int]unix.WaitStatus
}

func newReaper() *reaper {
	r := &reaper{waiting: map[int]chan unix.WaitStatus{}, exited: map[int]unix.WaitStatus{}}
	sigs := make(chan os.Signal, 64)
	signal.Notify(sigs, syscall.SIGCHLD)
	go func() {
		for range sigs {
			r.reap()
		}
	}()
	return r
}

func (r *reaper) reap() {
	for {
		var ws unix.WaitStatus
		pid, err := unix.Wait4(-1, &ws, unix.WNOHANG, nil)
		if pid <= 0 || err != nil {
			return
		}
		r.mu.Lock()
		if ch, ok := r.waiting[pid]; ok {
			delete(r.waiting, pid)
			ch <- ws
		} else {
			r.exited[pid] = ws
		}
		r.mu.Unlock()
	}
}

// wait waits for a command's pid, started while r reaped.
func (r *reaper) wait(pid int) unix.WaitStatus {
	r.mu.Lock()
	if ws, ok := r.exited[pid]; ok {
		delete(r.exited, pid)
		r.mu.Unlock()
		return ws
	}
	ch := make(chan unix.WaitStatus, 1)
	r.waiting[pid] = ch
	r.mu.Unlock()
	r.reap()
	return <-ch
}

// lookPath finds argv0 in env's PATH, as a shell would.
func lookPath(argv0 string, env []string) (string, error) {
	if strings.Contains(argv0, "/") {
		return argv0, nil
	}
	path := "/usr/local/bin:/usr/bin:/bin"
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			path = v
		}
	}
	for _, dir := range filepath.SplitList(path) {
		p := filepath.Join(dir, argv0)
		if st, err := os.Stat(p); err == nil && !st.IsDir() && st.Mode()&0o111 != 0 {
			return p, nil
		}
	}
	return "", fmt.Errorf("%s: not found in PATH", argv0)
}

// serveExec runs a command as the worker, in its own process group, streaming its output; the
// connection closing kills the group.
func serveExec(c net.Conn, req Request, cfg GuestConfig, r *reaper) {
	var wmu sync.Mutex
	write := func(kind byte, p []byte) error {
		wmu.Lock()
		defer wmu.Unlock()
		return WriteFrame(c, kind, p)
	}
	if len(req.Argv) == 0 {
		_ = write(FrameError, []byte("no command"))
		return
	}
	path, err := lookPath(req.Argv[0], req.Env)
	if err != nil {
		_ = write(FrameError, []byte(err.Error()))
		return
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		_ = write(FrameError, []byte(err.Error()))
		return
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		_ = write(FrameError, []byte(err.Error()))
		return
	}
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		_ = write(FrameError, []byte(err.Error()))
		return
	}
	pid, err := syscall.ForkExec(path, req.Argv, &syscall.ProcAttr{
		Dir:   req.Dir,
		Env:   req.Env,
		Files: []uintptr{devnull.Fd(), outW.Fd(), errW.Fd()},
		Sys: &syscall.SysProcAttr{Setpgid: true, Credential: &syscall.Credential{Uid: uint32(cfg.UID), Gid: uint32(cfg.GID)},
			Pdeathsig: syscall.SIGKILL},
	})
	devnull.Close()
	outW.Close()
	errW.Close()
	if err != nil {
		outR.Close()
		errR.Close()
		_ = write(FrameError, []byte(fmt.Sprintf("starting %s: %v", req.Argv[0], err)))
		return
	}
	// The client signals the group; its going away kills it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		var b [1]byte
		for {
			if _, err := c.Read(b[:]); err != nil {
				_ = unix.Kill(-pid, unix.SIGKILL)
				return
			}
			switch b[0] {
			case Terminate:
				_ = unix.Kill(-pid, unix.SIGTERM)
			case Kill:
				_ = unix.Kill(-pid, unix.SIGKILL)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	var wg sync.WaitGroup
	for _, s := range []struct {
		f    *os.File
		kind byte
	}{{outR, FrameStdout}, {errR, FrameStderr}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 32<<10)
			for {
				n, err := s.f.Read(buf)
				if n > 0 {
					if write(s.kind, buf[:n]) != nil {
						return
					}
				}
				if err != nil {
					return
				}
			}
		}()
	}
	ws := r.wait(pid)
	// What the command left running may hold its output open: it has a moment to finish
	// writing, then the output ends with the command.
	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(outputGrace):
	}
	outR.Close()
	errR.Close()
	wg.Wait()
	status := ws.ExitStatus()
	if ws.Signaled() {
		status = -int(ws.Signal())
	}
	_ = write(FrameExit, exitPayload(status))
}
