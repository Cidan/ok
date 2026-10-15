package checkout

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/proc"
	okfspb "github.com/Cidan/ok/proto/ok/okfs/v1"
)

// VirtualConfig is how a Virtual presents commits.
type VirtualConfig struct {
	// Dir is where commits are presented: an absolute path, an empty directory or a previous
	// presentation's.
	Dir string
	// State holds the okfs daemon's state — its caches of every repository presented — and its
	// control socket. It must not be inside Dir: what runs in the checkout must reach neither.
	State string
	// JJ is ok's jj, whose `jj fs daemon` presents.
	JJ string
	// SCM is the SCM's address, host:port, reached over TLS when TLS is set — its certificate
	// checked against CAFile, or the system's roots without one — and in plaintext otherwise, on
	// a network the installation trusts.
	SCM    string
	TLS    bool
	CAFile string
	// ConfigDir holds the credential the daemon presents (its credential/ directory, as `jj auth
	// login` writes one): the service's own, kept current by the service.
	ConfigDir string
	// DevPrincipal is the principal named to a loopback development SCM instead (tests).
	DevPrincipal string
	// ReadAhead are globs of repository-relative paths (`*` within a name, `**` across
	// directories) each Sync has local before it returns.
	ReadAhead []string
	// Log is where the daemon logs.
	Log io.Writer
}

// Virtual presents each commit at its directory through okfs (docs/okfs.md, "ok's roles"): it
// runs the okfs daemon as its child, starting it again if it exits, and asks
// it to present each commit it is synced to. It must run where it can mount: in the namespace
// Isolate makes.
type Virtual struct {
	cfg VirtualConfig
	// syncing serializes Syncs at cfg.Dir; each presentation At makes has its own.
	syncing sync.Mutex

	mu     sync.Mutex // guards the daemon
	daemon *exec.Cmd
	exited chan struct{} // closed when the daemon has exited
	// spawn starts processes from one thread held for the life of the program, so that the
	// daemon's parent-death signal, which the kernel ties to the thread that started it, is sent
	// only when the program ends.
	spawn chan func()
}

// NewVirtual returns a Virtual; the daemon starts at the first Sync.
func NewVirtual(cfg VirtualConfig) (*Virtual, error) {
	if !filepath.IsAbs(cfg.Dir) || !filepath.IsAbs(cfg.State) {
		return nil, fmt.Errorf("a virtual checkout's directory (%q) and state (%q) are absolute paths", cfg.Dir, cfg.State)
	}
	if rel, err := filepath.Rel(cfg.Dir, cfg.State); err == nil && !strings.HasPrefix(rel, "..") {
		return nil, fmt.Errorf("the okfs daemon's state %s is inside the checkout %s", cfg.State, cfg.Dir)
	}
	if cfg.JJ == "" || cfg.SCM == "" {
		return nil, errors.New("a virtual checkout needs ok's jj and the SCM's address")
	}
	if cfg.Log == nil {
		cfg.Log = os.Stderr
	}
	for _, d := range []string{cfg.Dir, filepath.Join(cfg.State, "home"), filepath.Join(cfg.State, "state"), filepath.Join(cfg.State, "run")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}
	v := &Virtual{cfg: cfg, spawn: make(chan func())}
	go func() {
		runtime.LockOSThread()
		for f := range v.spawn {
			f()
		}
	}()
	return v, nil
}

// Dir is where commits are presented.
func (v *Virtual) Dir() string { return v.cfg.Dir }

// Sync presents the commit, with files at its root.
func (v *Virtual) Sync(ctx context.Context, repo int64, commit []byte, files map[string][]byte) (Stats, error) {
	v.syncing.Lock()
	defer v.syncing.Unlock()
	return v.present(ctx, v.cfg.Dir, repo, commit, files)
}

// At returns a Checkout that presents commits at dir, through the same daemon and its caches:
// a worker that runs several jobs at once presents each one's commit at a directory of its own.
// dir is an absolute path, an empty directory or a previous presentation's, and holds neither
// the daemon's state nor another presentation.
func (v *Virtual) At(dir string) (Checkout, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("a presentation's directory %q is an absolute path", dir)
	}
	if rel, err := filepath.Rel(dir, v.cfg.State); err == nil && !strings.HasPrefix(rel, "..") {
		return nil, fmt.Errorf("the okfs daemon's state %s is inside the presentation %s", v.cfg.State, dir)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &presentation{v: v, dir: dir}, nil
}

// presentation is a Checkout At made: commits presented at its directory, by its Virtual's
// daemon.
type presentation struct {
	v   *Virtual
	dir string
	mu  sync.Mutex // serializes its Syncs
}

func (p *presentation) Dir() string { return p.dir }

func (p *presentation) Sync(ctx context.Context, repo int64, commit []byte, files map[string][]byte) (Stats, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.v.present(ctx, p.dir, repo, commit, files)
}

// present asks the daemon — starting it, or starting it again, as needed — to present the
// commit at dir, with files at its root. The daemon presents several directories at once.
func (v *Virtual) present(ctx context.Context, dir string, repo int64, commit []byte, files map[string][]byte) (Stats, error) {
	present := &okfspb.Present{
		MountPoint:   dir,
		Endpoint:     v.endpoint(),
		RepositoryId: repo,
		CommitId:     commit,
		ReadAhead:    v.cfg.ReadAhead,
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		present.Files = append(present.Files, &okfspb.PresentedFile{Name: name, Contents: files[name]})
	}
	req := &okfspb.Request{Request: &okfspb.Request_Present{Present: present}}
	for attempt := 0; ; attempt++ {
		v.mu.Lock()
		err := v.start()
		v.mu.Unlock()
		if err != nil {
			return Stats{}, err
		}
		resp, err := v.call(ctx, req)
		if err != nil {
			if ctx.Err() != nil {
				return Stats{}, ctx.Err()
			}
			// A daemon that died mid-request is started again, and asked again, once.
			if attempt == 0 && v.died(5*time.Second) {
				continue
			}
			return Stats{}, fmt.Errorf("the okfs daemon: %w", err)
		}
		if resp.GetRefused() {
			return Stats{}, &CommitError{Message: resp.GetError()}
		}
		if resp.GetError() != "" {
			return Stats{}, fmt.Errorf("the okfs daemon: %s", resp.GetError())
		}
		r := resp.GetPresent()
		if r == nil {
			return Stats{}, errors.New("the okfs daemon answered Present with something else")
		}
		return Stats{Mounted: r.GetMounted(), ReadAhead: int(r.GetReadAheadFiles()), ReadAheadBytes: int64(r.GetReadAheadBytes())}, nil
	}
}

// endpoint is the SCM as the daemon dials it.
func (v *Virtual) endpoint() string {
	if v.cfg.TLS {
		return "https://" + v.cfg.SCM
	}
	return "http://" + v.cfg.SCM
}

// Close stops the daemon, which unmounts the presentation.
func (v *Virtual) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.daemon == nil {
		return nil
	}
	_ = v.daemon.Process.Signal(syscall.SIGTERM)
	select {
	case <-v.exited:
	case <-time.After(20 * time.Second):
		_ = v.daemon.Process.Kill()
		<-v.exited
	}
	v.daemon = nil
	return nil
}

func (v *Virtual) socket() string { return filepath.Join(v.cfg.State, "run", "control.sock") }

// start starts the daemon unless it is running, and waits for its control socket. It is
// called with v.mu held.
func (v *Virtual) start() error {
	if v.daemon != nil {
		select {
		case <-v.exited:
			v.daemon = nil
		default:
			return nil
		}
	}
	cmd := exec.Command(v.cfg.JJ, "fs", "daemon")
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + filepath.Join(v.cfg.State, "home"),
		"OKFS_STATE_DIR=" + filepath.Join(v.cfg.State, "state"),
		"OKFS_RUNTIME_DIR=" + filepath.Join(v.cfg.State, "run"),
		"LANG=C.UTF-8",
		"TZ=UTC",
	}
	switch {
	case v.cfg.DevPrincipal != "":
		cmd.Env = append(cmd.Env, "OK_DEV_PRINCIPAL="+v.cfg.DevPrincipal)
	case !v.cfg.TLS:
		cmd.Env = append(cmd.Env, "OK_PLAINTEXT=1")
	}
	if v.cfg.ConfigDir != "" {
		cmd.Env = append(cmd.Env, "OK_CONFIG_DIR="+v.cfg.ConfigDir)
	}
	if v.cfg.CAFile != "" {
		cmd.Env = append(cmd.Env, "OK_CA_FILE="+v.cfg.CAFile)
	}
	cmd.Stdout, cmd.Stderr = v.cfg.Log, v.cfg.Log
	// The daemon stops — unmounting — when the program ends, however it ends.
	cmd.SysProcAttr = proc.DieWithParent(&syscall.SysProcAttr{}, syscall.SIGTERM)
	_ = os.Remove(v.socket())
	started := make(chan error, 1)
	v.spawn <- func() { started <- cmd.Start() }
	if err := <-started; err != nil {
		return fmt.Errorf("starting the okfs daemon (%s fs daemon): %w", v.cfg.JJ, err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	v.daemon, v.exited = cmd, exited
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := net.Dial("unix", v.socket())
		if err == nil {
			conn.Close()
			return nil
		}
		select {
		case <-exited:
			v.daemon = nil
			return fmt.Errorf("the okfs daemon exited as it started (%s): its log is the service's", cmd.ProcessState)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return errors.New("the okfs daemon did not start in 30 seconds")
		}
	}
}

// died reports whether the daemon has exited, or exits within wait.
func (v *Virtual) died(wait time.Duration) bool {
	v.mu.Lock()
	daemon, exited := v.daemon, v.exited
	v.mu.Unlock()
	if daemon == nil {
		return true
	}
	select {
	case <-exited:
		return true
	case <-time.After(wait):
		return false
	}
}

// call sends one request on the control socket and reads its response: a 4-byte big-endian
// length, then the message, each way.
func (v *Virtual) call(ctx context.Context, req *okfspb.Request) (*okfspb.Response, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", v.socket())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	msg, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	frame := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(msg)), uint32(len(msg)))
	if _, err := conn.Write(append(frame, msg...)); err != nil {
		return nil, err
	}
	var n [4]byte
	if _, err := io.ReadFull(conn, n[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(n[:])
	if size > maxControlMessage {
		return nil, fmt.Errorf("a %d-byte response", size)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}
	resp := &okfspb.Response{}
	if err := proto.Unmarshal(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// maxControlMessage is the largest message the daemon sends (its MAX_MESSAGE).
const maxControlMessage = 256 << 20
