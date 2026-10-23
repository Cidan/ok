package worker

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/Cidan/ok/internal/checkout"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// Trust domains (docs/hosted.md, "Hosted automation, as built"): what a worker keeps from run to
// run — checkouts, Bazel's output bases and home, kept graphs — kept for each repository's pre (pre's
// runs, manual runs, and their plans) and its post (post's runs, pushes, and their plans), never
// across repositories, never from pre into post. A run takes an idle copy of its domain's, or a
// new one.

// domainOf is a run's trust domain.
func domainOf(run *automationpb.Run) string {
	side := "pre"
	switch run.GetOrigin().GetPhase() {
	case automationpb.Origin_PHASE_POST, automationpb.Origin_PHASE_DEPLOY:
		side = "post"
	}
	return strconv.FormatInt(run.GetRepositoryId(), 10) + "-" + side
}

// domainCopy is one copy of a domain's: <root>/<domain>/<n>, its checkouts under view/, which a
// run's microVM sees read-only, and what it writes under rw/.
type domainCopy struct {
	key, dir       string
	checkout, base checkout.Checkout
	used           time.Time
}

func (c *domainCopy) view() string       { return filepath.Join(c.dir, "view") }
func (c *domainCopy) rw() string         { return filepath.Join(c.dir, "rw") }
func (c *domainCopy) outputBase() string { return filepath.Join(c.rw(), "out") }
func (c *domainCopy) baseOutput() string { return filepath.Join(c.rw(), "out-base") }
func (c *domainCopy) home() string       { return filepath.Join(c.rw(), "home") }
func (c *domainCopy) scratch() string    { return filepath.Join(c.rw(), "scratch") }

// graphs is where a domain keeps commits' build graphs: shared by its copies, by no other domain.
func (d *domains) graphs(key string) string { return filepath.Join(d.root, key, "graphs") }

// domains are a worker's trust domains' copies.
type domains struct {
	root        string
	newCheckout func(dir string) (checkout.Checkout, error)
	// reserve is the space kept free on root's filesystem, and most the copies kept: past either,
	// the least recently used idle copies go.
	reserve uint64
	most    int

	mu     sync.Mutex
	copies []*domainCopy
	busy   map[*domainCopy]bool
}

// newDomains keeps domains under root, taking up the copies a worker before it left there.
func newDomains(root string, newCheckout func(dir string) (checkout.Checkout, error), reserve uint64, most int) (*domains, error) {
	d := &domains{root: root, newCheckout: newCheckout, reserve: reserve, most: max(most, 1), busy: map[*domainCopy]bool{}}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	keys, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		if !k.IsDir() {
			continue
		}
		ns, err := os.ReadDir(filepath.Join(root, k.Name()))
		if err != nil {
			return nil, err
		}
		for _, n := range ns {
			if _, err := strconv.Atoi(n.Name()); err != nil || !n.IsDir() {
				continue
			}
			c, err := d.open(k.Name(), filepath.Join(root, k.Name(), n.Name()))
			if err != nil {
				return nil, err
			}
			if info, err := n.Info(); err == nil {
				c.used = info.ModTime()
			}
			d.copies = append(d.copies, c)
		}
	}
	return d, nil
}

// open makes a copy's directories and its checkouts' presentations.
func (d *domains) open(key, dir string) (*domainCopy, error) {
	c := &domainCopy{key: key, dir: dir}
	for _, p := range []string{c.view(), c.outputBase(), c.baseOutput(), c.home(), c.scratch(), d.graphs(key)} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			return nil, err
		}
	}
	var err error
	if c.checkout, err = d.newCheckout(filepath.Join(c.view(), "checkout")); err != nil {
		return nil, err
	}
	if c.base, err = d.newCheckout(filepath.Join(c.view(), "base")); err != nil {
		return nil, err
	}
	return c, nil
}

// take is an idle copy of a domain's — the most recently used, whose output base is the warmest —
// or a new one.
func (d *domains) take(key string) (*domainCopy, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var best *domainCopy
	for _, c := range d.copies {
		if c.key == key && !d.busy[c] && (best == nil || c.used.After(best.used)) {
			best = c
		}
	}
	if best == nil {
		n := 1
		for {
			dir := filepath.Join(d.root, key, strconv.Itoa(n))
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				c, err := d.open(key, dir)
				if err != nil {
					return nil, fmt.Errorf("a copy of trust domain %s: %w", key, err)
				}
				best = c
				d.copies = append(d.copies, c)
				break
			}
			n++
		}
	}
	d.busy[best] = true
	return best, nil
}

// put gives a copy back, idle, then evicts what the worker keeps past its bounds.
func (d *domains) put(c *domainCopy) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.busy, c)
	c.used = time.Now()
	_ = os.Chtimes(c.dir, c.used, c.used)
	return d.evict()
}

// evict removes the least recently used idle copies while the worker keeps more than its most, or
// its filesystem has less free than its reserve; it says what it could not remove.
func (d *domains) evict() error {
	for {
		over := len(d.copies) > d.most
		if !over && d.reserve > 0 {
			free, err := freeSpace(d.root)
			if err != nil {
				return err
			}
			over = free < d.reserve
		}
		if !over {
			return nil
		}
		idle := slices.DeleteFunc(slices.Clone(d.copies), func(c *domainCopy) bool { return d.busy[c] })
		if len(idle) == 0 {
			return nil
		}
		oldest := slices.MinFunc(idle, func(a, b *domainCopy) int { return a.used.Compare(b.used) })
		for _, co := range []checkout.Checkout{oldest.checkout, oldest.base} {
			if cl, ok := co.(io.Closer); ok {
				if err := cl.Close(); err != nil {
					return fmt.Errorf("evicting %s: %w", oldest.dir, err)
				}
			}
		}
		if err := removeTree(oldest.dir); err != nil {
			return fmt.Errorf("evicting %s: %w", oldest.dir, err)
		}
		d.copies = slices.DeleteFunc(d.copies, func(c *domainCopy) bool { return c == oldest })
		// A domain with no copy left keeps no graphs.
		if !slices.ContainsFunc(d.copies, func(c *domainCopy) bool { return c.key == oldest.key }) {
			_ = removeTree(filepath.Join(d.root, oldest.key))
		}
	}
}

// freeSpace is the space free to the worker on a directory's filesystem.
func freeSpace(dir string) (uint64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}

// removeTree removes a directory and everything in it, read-only directories among them — what
// Bazel and module caches leave.
func removeTree(dir string) error {
	_ = filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err == nil && e.IsDir() {
			_ = os.Chmod(p, 0o755)
		}
		return nil
	})
	return os.RemoveAll(dir)
}
