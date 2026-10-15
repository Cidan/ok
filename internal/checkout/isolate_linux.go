package checkout

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// isolatedEnv marks the program's copy that runs in the namespace Isolate makes.
const isolatedEnv = "OK_CHECKOUT_ISOLATED"

// Isolate runs this program again in a user and mount namespace of its own, where the okfs
// daemon a Virtual starts can mount (docs/okfs.md, "ok's roles"), and returns only in
// that copy. Every id this process has is mapped to itself there when it is root — so it keeps
// them all, and is root of the namespace with every capability over its mounts — and its own id
// to root otherwise. This process stays to forward the signals it gets to the copy, reap the
// orphans the copy leaves (it is a subreaper), stop what the copy leaves running, and exit with
// the copy's status. Call it first in main.
//
// The copy's ids are mapped before it first runs (the clone's maps, which Go writes between the
// clone and the exec), so it starts as root of the namespace with every capability — the only
// way it can where privilege escalation is not allowed (no_new_privs: a container's
// allowPrivilegeEscalation: false), under which no later exec gains a capability. Where that
// cannot be done — a sandbox's /proc may number an outer PID namespace's processes, so the number
// the clone returns names another process there — the copy starts with no id mapped, tells this
// process its number as /proc numbers it, this process writes the maps there, and the copy runs
// itself again, now root of the namespace (which needs privilege escalation allowed).
func Isolate() error {
	switch os.Getenv(isolatedEnv) {
	case "1":
		// Mounts made here stay here.
		if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
			if status, _ := os.ReadFile("/proc/self/status"); strings.Contains(string(status), "NoNewPrivs:\t1") {
				return fmt.Errorf("making the mount namespace private: %w: its ids were mapped only after it first ran, and privilege escalation is not allowed (no_new_privs), so it holds no capability", err)
			}
			return fmt.Errorf("making the mount namespace private: %w; %s", err, UserNamespaces())
		}
		return nil
	case "pending":
		return awaitMaps()
	}
	uidMap, err1 := os.ReadFile("/proc/self/uid_map")
	gidMap, err2 := os.ReadFile("/proc/self/gid_map")
	setgroups, err3 := os.ReadFile("/proc/self/setgroups")
	if err := errors.Join(err1, err2, err3); err != nil {
		return err
	}
	uids, gids, allowSetgroups, err := namespaceMaps(os.Geteuid(), os.Getegid(), string(uidMap), string(gidMap), string(setgroups))
	if err != nil {
		return err
	}
	// The copy is killed if this process dies (Pdeathsig), which the kernel ties to the thread
	// that started it: that thread is held until this process exits.
	runtime.LockOSThread()
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("becoming a subreaper: %w", err)
	}
	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	cmd, err := startMapped(uids, gids, allowSetgroups)
	if err != nil {
		var err2 error
		if cmd, err2 = startUnmapped(uids, gids, allowSetgroups); err2 != nil {
			return fmt.Errorf("running in a user and mount namespace of its own: %w (with its ids unmapped at first: %w); %s", err, err2, UserNamespaces())
		}
	}
	child := cmd.Process.Pid
	go func() {
		for s := range signals {
			_ = cmd.Process.Signal(s)
		}
	}()
	code := reap(child)
	// What the copy left running — its process group: the daemon, Bazel — is stopped.
	_ = syscall.Kill(-child, syscall.SIGTERM)
	deadline := time.Now().Add(10 * time.Second)
	for reapAny() && time.Now().Before(deadline) {
	}
	_ = syscall.Kill(-child, syscall.SIGKILL)
	for reapAny() {
	}
	os.Exit(code)
	return nil
}

// copyOf is the command that runs this program again, as it was run, through the kernel's own link
// to it — whatever path it was started by, which a sandbox may not show.
func copyOf(state string) *exec.Cmd {
	cmd := exec.Command("/proc/self/exe", os.Args[1:]...)
	cmd.Args[0] = os.Args[0]
	cmd.Env = append(os.Environ(), isolatedEnv+"="+state)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	return cmd
}

// startMapped starts the copy with its ids mapped as it is cloned: it runs as root of its
// namespace from the start.
func startMapped(uids, gids []syscall.SysProcIDMap, allowSetgroups bool) (*exec.Cmd, error) {
	cmd := copyOf("1")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:                 syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS,
		UidMappings:                uids,
		GidMappings:                gids,
		GidMappingsEnableSetgroups: allowSetgroups,
		Setpgid:                    true,
		Pdeathsig:                  syscall.SIGKILL,
	}
	return cmd, cmd.Start()
}

// startUnmapped starts the copy with no id mapped, and maps them where the copy's /proc numbers it
// (awaitMaps).
func startUnmapped(uids, gids []syscall.SysProcIDMap, allowSetgroups bool) (*exec.Cmd, error) {
	cmd := copyOf("pending")
	mapped, mappedW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer mappedW.Close()
	numberR, number, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd.ExtraFiles = []*os.File{mapped, number}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS,
		Setpgid:    true,
		Pdeathsig:  syscall.SIGKILL,
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	mapped.Close()
	number.Close()
	if err := writeMaps(numberR, uids, gids, allowSetgroups); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("mapping the ids of its user namespace: %w", err)
	}
	if _, err := mappedW.Write([]byte{1}); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}
	return cmd, nil
}

// writeMaps reads the copy's number in /proc from it and writes its namespace's maps there.
func writeMaps(numberR *os.File, uids, gids []syscall.SysProcIDMap, allowSetgroups bool) error {
	defer numberR.Close()
	b, err := io.ReadAll(io.LimitReader(numberR, 32))
	if err != nil {
		return err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return fmt.Errorf("the copy's /proc number %q", b)
	}
	dir := "/proc/" + strconv.Itoa(n)
	if !allowSetgroups {
		if err := os.WriteFile(dir+"/setgroups", []byte("deny"), 0); err != nil {
			return err
		}
	}
	if err := os.WriteFile(dir+"/uid_map", mapText(uids), 0); err != nil {
		return err
	}
	return os.WriteFile(dir+"/gid_map", mapText(gids), 0)
}

func mapText(m []syscall.SysProcIDMap) []byte {
	var b strings.Builder
	for _, r := range m {
		fmt.Fprintf(&b, "%d %d %d\n", r.ContainerID, r.HostID, r.Size)
	}
	return []byte(b.String())
}

// awaitMaps is the copy before its namespace maps any id: it says its number in /proc, waits for
// the maps, and runs itself again, as root of the namespace.
func awaitMaps() error {
	mapped, number := os.NewFile(3, "mapped"), os.NewFile(4, "number")
	self, err := os.Readlink("/proc/self")
	if err != nil {
		return err
	}
	if _, err := number.Write([]byte(self)); err != nil {
		return err
	}
	number.Close()
	var b [1]byte
	if n, err := mapped.Read(b[:]); n != 1 {
		return fmt.Errorf("its user namespace's ids were never mapped: %v", err)
	}
	mapped.Close()
	env := os.Environ()
	for i, kv := range env {
		if strings.HasPrefix(kv, isolatedEnv+"=") {
			env[i] = isolatedEnv + "=1"
		}
	}
	return syscall.Exec("/proc/self/exe", os.Args, env)
}

// reap waits for child, reaping every other process that ends meanwhile, and returns the exit
// status it should be reported with.
func reap(child int) int {
	for {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, 0, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return 1
		}
		if pid != child {
			continue
		}
		switch {
		case ws.Exited():
			return ws.ExitStatus()
		case ws.Signaled():
			return 128 + int(ws.Signal())
		}
	}
}

// reapAny reaps one ended process, waiting at most a moment; false when none is left to wait
// for.
func reapAny() bool {
	var ws syscall.WaitStatus
	pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
	if errors.Is(err, syscall.ECHILD) {
		return false
	}
	if pid == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	return true
}

// namespaceMaps are the new namespace's id maps, from this process's ids and its namespace's
// uid_map, gid_map, and setgroups files: for root, every range of ids it has, each to itself; for
// anyone else, their own id to root. setgroups(2) stays allowed only where it is now: a namespace
// whose parent denies it cannot allow it, and the kernel refuses its gid map if it tries.
func namespaceMaps(euid, egid int, uidMap, gidMap, setgroups string) (uids, gids []syscall.SysProcIDMap, allowSetgroups bool, err error) {
	if euid != 0 {
		return []syscall.SysProcIDMap{{ContainerID: 0, HostID: euid, Size: 1}},
			[]syscall.SysProcIDMap{{ContainerID: 0, HostID: egid, Size: 1}}, false, nil
	}
	if uids, err = parseIdentityMap(strings.NewReader(uidMap)); err != nil {
		return nil, nil, false, err
	}
	if gids, err = parseIdentityMap(strings.NewReader(gidMap)); err != nil {
		return nil, nil, false, err
	}
	return uids, gids, strings.TrimSpace(setgroups) == "allow", nil
}

// parseIdentityMap maps each range of ids a uid_map or gid_map says this process has to
// itself.
func parseIdentityMap(r interface{ Read([]byte) (int, error) }) ([]syscall.SysProcIDMap, error) {
	var out []syscall.SysProcIDMap
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 {
			return nil, fmt.Errorf("an id map line %q", sc.Text())
		}
		inside, err1 := strconv.Atoi(fields[0])
		size, err2 := strconv.Atoi(fields[2])
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("an id map line %q", sc.Text())
		}
		out = append(out, syscall.SysProcIDMap{ContainerID: inside, HostID: inside, Size: size})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("an empty id map")
	}
	return out, nil
}
