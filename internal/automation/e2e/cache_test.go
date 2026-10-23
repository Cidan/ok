package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/config"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// TestRemoteCache drives pre on two workers that share nothing but ok automation and its remote
// cache (docs/hosted.md, "Hosted automation, as built"): what the first built, the second takes
// from the cache rather than building again.
func TestRemoteCache(t *testing.T) {
	e := start(t, func(c *config.Config) { c.Automation.Cache = &config.AutomationCache{TTL: time.Hour, MemoryMiB: 64} })
	a, repo := e.newRepository("cached")
	w1 := e.startWorker("w1", 1)
	build := func(msg string) string {
		t.Helper()
		a.jj("cached", "new", "main@ok", "-m", msg)
		write(t, a.dir("cached"), map[string]*string{"lib/a.txt": ptr("hello, cache\n")})
		cl := uploaded(a.jj("cached", "cl", "--ready"))[0]
		p := e.settled(repo, cl, 1)
		if p.GetVerdict() != automationpb.Pre_VERDICT_PASSED {
			t.Fatalf("%s: %s", msg, e.describe(p))
		}
		return e.logOf(jobs(p)["build"].GetId())
	}
	build("built on w1")
	w1.stop()
	// A worker of its own output base, which has built nothing.
	e.startWorker("w2", 1)
	if log := build("the same, on w2"); !strings.Contains(log, "remote cache hit") {
		t.Errorf("the second worker built again what the first had:\n%s", log)
	}
}
