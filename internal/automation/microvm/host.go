package microvm

import (
	"bufio"
	"context"
	"debug/elf"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Config is how a worker runs its microVMs.
type Config struct {
	// Hypervisor is Cloud Hypervisor's path, Virtiofsd virtiofsd's, and Kernel the guest's kernel
	// (a bzImage or vmlinux with virtio-fs and vsock built in).
	Hypervisor, Virtiofsd, Kernel string
	// CPUs and MemoryMiB are each microVM's.
	CPUs      int
	MemoryMiB int
	// Root is the guest's root, which Prepare makes: shared read-only.
	Root string
	// System are the host's directories every guest sees read-only, at the same paths: /usr, the
	// certificates TLS checks against, the installation's worker.microvm.share.
	System []string
	// Init is ok's binary, the guest's init, which must be static.
	Init string
}

// DefaultSystem are the host directories a guest sees unless the installation says more: the
// programs, and what TLS trusts.
var DefaultSystem = []string{"/usr", "/etc/ssl", "/etc/ca-certificates", "/etc/alternatives", "/etc/pki"}

// Static reports whether a binary is statically linked: one a guest can run as its init before
// anything is mounted.
func Static(path string) error {
	f, err := elf.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			return fmt.Errorf("%s is dynamically linked: a microVM's init is ok built with CGO_ENABLED=0", path)
		}
	}
	return nil
}

// Prepare makes the guests' root: ok as /ok, the system directories' mount points, /etc's few files
// for the worker's user, and the merged-/usr links the host has.
func Prepare(cfg Config, uid, gid int) error {
	if err := Static(cfg.Init); err != nil {
		return err
	}
	root := cfg.Root
	for _, d := range []string{"proc", "sys", "dev", "tmp", "run", "etc", "usr"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return err
		}
	}
	if err := copyFile(cfg.Init, filepath.Join(root, "ok"), 0o755); err != nil {
		return fmt.Errorf("the guests' init: %w", err)
	}
	for _, l := range []string{"bin", "sbin", "lib", "lib32", "lib64", "libx32"} {
		host := filepath.Join("/", l)
		target, err := os.Readlink(host)
		dst := filepath.Join(root, l)
		_ = os.Remove(dst)
		switch {
		case err == nil:
			if err := os.Symlink(target, dst); err != nil {
				return err
			}
		case errors.Is(err, os.ErrNotExist):
		default:
			// A directory of its own (a distribution without a merged /usr): a system share.
			if st, serr := os.Stat(host); serr == nil && st.IsDir() {
				if err := os.MkdirAll(dst, 0o755); err != nil {
					return err
				}
			}
		}
	}
	for _, s := range cfg.System {
		if err := MountPoint(root, s); err != nil {
			return err
		}
	}
	files := map[string]string{
		"etc/passwd":        fmt.Sprintf("root:x:0:0:root:/root:/bin/sh\nworker:x:%d:%d:worker:/tmp:/bin/sh\n", uid, gid),
		"etc/group":         fmt.Sprintf("root:x:0:\nworker:x:%d:\n", gid),
		"etc/hosts":         "127.0.0.1 localhost\n::1 localhost\n",
		"etc/nsswitch.conf": "passwd: files\ngroup: files\nhosts: files\n",
		"etc/resolv.conf":   "",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// MountPoint makes a path's mount point in the guests' root.
func MountPoint(root, path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("a guest's mount is at an absolute path, not %q", path)
	}
	return os.MkdirAll(filepath.Join(root, filepath.Clean(path)), 0o755)
}

// systemShares are the system directories that exist on the host, and the directories of their
// own a host without a merged /usr has.
func (cfg Config) systemShares() []string {
	var out []string
	for _, s := range cfg.System {
		if st, err := os.Stat(s); err == nil && st.IsDir() {
			out = append(out, s)
		}
	}
	for _, l := range []string{"bin", "sbin", "lib", "lib32", "lib64", "libx32"} {
		host := filepath.Join("/", l)
		if st, err := os.Lstat(host); err == nil && st.IsDir() && st.Mode()&os.ModeSymlink == 0 {
			out = append(out, host)
		}
	}
	return out
}

func copyFile(src, dst string, mode os.FileMode) error {
	if st, err := os.Stat(dst); err == nil {
		if ss, err := os.Stat(src); err == nil && ss.Size() == st.Size() && !ss.ModTime().After(st.ModTime()) {
			return nil
		}
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

// Share is a host directory a run's guest sees at the same path.
type Share struct {
	Path     string
	ReadOnly bool
}

// Egress is what a guest may reach through the host: an HTTP CONNECT proxy that dials only the
// hosts Allowed names, on port 443 unless a name carries one ("host:port"). A name that starts
// with "*." allows every host under it. Refused is told each host refused.
type Egress struct {
	Allowed []string
	Refused func(hostport string)
}

// allows reports whether the egress allows a host and port.
func (e Egress) allows(host, port string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, a := range e.Allowed {
		a = strings.ToLower(a)
		ah, ap, err := net.SplitHostPort(a)
		if err != nil {
			ah, ap = a, "443"
		}
		if ap != port {
			continue
		}
		if suffix, ok := strings.CutPrefix(ah, "*."); ok {
			if strings.HasSuffix(host, "."+suffix) {
				return true
			}
		} else if host == ah {
			return true
		}
	}
	return false
}

// VM is a run's microVM.
type VM struct {
	cfg    Config
	dir    string
	sock   string
	ch     *exec.Cmd
	fsd    []*exec.Cmd
	done   chan struct{}
	closed sync.Once
	ls     []net.Listener
	log    *slog.Logger
}

// Options are a run's microVM's.
type Options struct {
	// Shares are the run's directories: its trust domain's checkouts, read-only, and its output
	// bases, home, and scratch.
	Shares []Share
	// Egress is what it may reach; nil for nothing.
	Egress *Egress
	// Cache, when set, dials the remote cache, which the guest's CacheSocket reaches.
	Cache func(ctx context.Context) (net.Conn, error)
	Log   *slog.Logger
}

// waitFor waits for a socket to appear.
func waitFor(ctx context.Context, path string) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s never appeared", path)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// listen serves the guest's connections to the host's port.
func (vm *VM) listen(port int, serve func(net.Conn)) error {
	l, err := net.Listen("unix", vm.sock+"_"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	vm.ls = append(vm.ls, l)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go serve(c)
		}
	}()
	return nil
}

// ready waits until the guest takes commands.
func (vm *VM) ready(ctx context.Context) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := DialGuest(vm.sock, ExecPort)
		if err == nil {
			// A connection the guest closes at once: it is listening.
			c.Close()
			return nil
		}
		select {
		case <-vm.done:
			return errors.New("cloud-hypervisor exited")
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no answer in 30s: %w", err)
		}
	}
}

func (vm *VM) console() string {
	b, _ := os.ReadFile(filepath.Join(vm.dir, "console"))
	if len(b) > 4096 {
		b = b[len(b)-4096:]
	}
	return string(b)
}

// Socket is the vsock socket commands reach the guest through (ok worker vm-exec -socket).
func (vm *VM) Socket() string { return vm.sock }

// Exec is the command prefix that runs a command in the guest: ok worker vm-exec.
func (vm *VM) Exec(ok string) []string {
	return []string{ok, "worker", "vm-exec", "-socket", vm.sock, "--"}
}

// Stop powers the guest off, and kills what it does not stop in time.
func (vm *VM) Stop() {
	vm.closed.Do(func() {
		if vm.ch != nil && vm.ch.Process != nil {
			if c, err := DialGuest(vm.sock, ExecPort); err == nil {
				_ = writeLine(c, Request{PowerOff: true})
				c.Close()
			}
			select {
			case <-vm.done:
			case <-time.After(5 * time.Second):
				_ = syscall.Kill(-vm.ch.Process.Pid, syscall.SIGKILL)
				<-vm.done
			}
		}
		for _, l := range vm.ls {
			l.Close()
		}
		for _, f := range vm.fsd {
			if f.Process != nil {
				_ = syscall.Kill(-f.Process.Pid, syscall.SIGKILL)
				_ = f.Wait()
			}
		}
		os.RemoveAll(vm.dir)
	})
}

// DialGuest connects to the guest's vsock port through Cloud Hypervisor's socket.
func DialGuest(sock string, port int) (net.Conn, error) {
	c, err := net.Dial("unix", sock)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(c, "CONNECT %d\n", port); err != nil {
		c.Close()
		return nil, err
	}
	// "OK <host port>\n", read a byte at a time so nothing after it is consumed.
	var line []byte
	var b [1]byte
	for {
		if _, err := c.Read(b[:]); err != nil {
			c.Close()
			return nil, fmt.Errorf("the guest's port %d: %w", port, err)
		}
		if b[0] == '\n' {
			break
		}
		line = append(line, b[0])
		if len(line) > 64 {
			c.Close()
			return nil, errors.New("microvm: an answer too long")
		}
	}
	if !strings.HasPrefix(string(line), "OK ") {
		c.Close()
		return nil, fmt.Errorf("the guest's port %d: %q", port, line)
	}
	return c, nil
}

// proxy serves one HTTP CONNECT request from the guest, dialing what egress allows.
func proxy(c net.Conn, e Egress) {
	defer c.Close()
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	if req.Method != http.MethodConnect {
		fmt.Fprintf(c, "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
		return
	}
	host, port, err := net.SplitHostPort(req.Host)
	if err != nil {
		host, port = req.Host, "443"
	}
	if !e.allows(host, port) {
		if e.Refused != nil {
			e.Refused(net.JoinHostPort(host, port))
		}
		fmt.Fprintf(c, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
		return
	}
	up, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 30*time.Second)
	if err != nil {
		fmt.Fprintf(c, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
		return
	}
	if _, err := fmt.Fprintf(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		up.Close()
		return
	}
	// What the guest sent after its request, already read.
	if n := br.Buffered(); n > 0 {
		b, _ := br.Peek(n)
		if _, err := up.Write(b); err != nil {
			up.Close()
			return
		}
	}
	pipe(c, up)
}
