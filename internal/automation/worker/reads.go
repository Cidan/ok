package worker

import (
	"fmt"
	"strconv"

	"golang.org/x/sys/unix"
)

// okfsFailuresAttr is okfs's extended attribute on a presentation's root: how many reads of ok its
// daemon could not answer and failed (docs/okfs.md, "Failed reads"). A program that takes a read's
// error for the end of its file — protoc does — reads too little and still succeeds, so nothing a run
// made while the count rose is kept.
const okfsFailuresAttr = "user.ok.fetch_failures"

// readsGuard watches a checkout's okfs presentation for failed reads of ok, from when it was made.
type readsGuard struct {
	dir  string
	base uint64
}

// watchReads returns a guard over the okfs presentation at dir, from now: nil when dir is no okfs
// presentation, or one whose daemon keeps no count. okfs refuses to set its own attributes, so a
// run cannot lower the count.
func watchReads(dir string) *readsGuard {
	n, ok := okfsFailures(dir)
	if !ok {
		return nil
	}
	return &readsGuard{dir: dir, base: n}
}

// failed is an error once the presentation has failed a read of ok since the guard began, or its
// daemon restarted (the count is lower than it was, and what the old daemon failed is unknown), or it
// stopped keeping a count; nil until then, and for no guard.
func (g *readsGuard) failed() error {
	if g == nil {
		return nil
	}
	n, ok := okfsFailures(g.dir)
	switch {
	case !ok:
		return fmt.Errorf("okfs at %s no longer reports its failed reads of ok: what the run built from its files is not kept", g.dir)
	case n < g.base:
		return fmt.Errorf("okfs at %s restarted during the run, and may have failed reads of ok: what the run built from its files is not kept", g.dir)
	case n > g.base:
		return fmt.Errorf("okfs at %s failed %d reads of ok during the run: a program may have read too little of a file and not said so, so what the run built from its files is not kept", g.dir, n-g.base)
	}
	return nil
}

// okfsFailures reads a presentation's count of failed reads of ok.
func okfsFailures(dir string) (uint64, bool) {
	buf := make([]byte, 32)
	n, err := unix.Getxattr(dir, okfsFailuresAttr, buf)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseUint(string(buf[:n]), 10, 64)
	return v, err == nil
}
