package worker

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/Cidan/ok/internal/automation/microvm"
	"github.com/Cidan/ok/internal/checkout"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// MicroVM is how a worker runs each run in a microVM of its own (docs/hosted.md, "Hosted
// automation, as built"): its trust domain's checkouts and output bases shared with it, the rest
// of the host out of its sight, and the network only through the worker's egress proxy.
type MicroVM struct {
	VM microvm.Config
	// OK is ok's binary, which every command runs under (ok worker vm-exec).
	OK string
	// Slots is how many runs the worker runs at once, each in its own microVM.
	Slots int
	// Domains is where the trust domains' copies are kept, NewCheckout makes a copy's checkout at a
	// directory, and Reserve (bytes free on Domains' filesystem) and Most (copies) bound what is
	// kept.
	Domains     string
	NewCheckout func(dir string) (checkout.Checkout, error)
	Reserve     uint64
	Most        int
	// Egress is what a run's guest may reach through the worker (microvm.Egress's names); a push
	// run's PushEgress too — its registries.
	Egress, PushEgress []string

	domains *domains
}

// open takes up the copies a worker before this one left.
func (m *MicroVM) open() error {
	d, err := newDomains(m.Domains, m.NewCheckout, m.Reserve, m.Most)
	if err != nil {
		return fmt.Errorf("the trust domains: %w", err)
	}
	m.domains = d
	return nil
}

// inDomain is a slot's configuration for a run in a copy of its trust domain.
func (cfg Config) inDomain(c *domainCopy) Config {
	cfg.domain = c
	cfg.checkout, cfg.base = c.checkout, c.base
	cfg.outputBase, cfg.baseOutputBase = c.outputBase(), c.baseOutput()
	cfg.scratch = c.scratch()
	cfg.Graphs = cfg.MicroVM.domains.graphs(c.key)
	cfg.others = nil
	cfg.Proc = ProcFresh
	return cfg
}

// startVM boots a run's microVM: its domain's checkouts read-only, what it writes read-write, and
// egress to what the worker allows for it, each host refused said in its log.
func (cfg Config) startVM(ctx context.Context, run *automationpb.Run, log *logStream) (*microvm.VM, error) {
	m := cfg.MicroVM
	allowed := append([]string(nil), m.Egress...)
	if run.GetJob().GetKind() == automationpb.Job_KIND_PUSH {
		allowed = append(allowed, m.PushEgress...)
	}
	var mu sync.Mutex
	said := map[string]bool{}
	var dialCache func(ctx context.Context) (net.Conn, error)
	if sock := cfg.cacheSocket; sock != "" {
		dialCache = func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "unix", sock) }
	}
	shares := []microvm.Share{{Path: cfg.domain.view(), ReadOnly: true}, {Path: cfg.domain.rw()}}
	if cfg.secretsDir != "" {
		// At the same path in the guest, under its own /dev/shm, for its sandbox to bind.
		shares = append(shares, microvm.Share{Path: cfg.secretsDir, ReadOnly: true})
	}
	return microvm.Start(ctx, m.VM, microvm.Options{
		Cache:  dialCache,
		Shares: shares,
		Egress: &microvm.Egress{Allowed: allowed, Refused: func(hostport string) {
			mu.Lock()
			defer mu.Unlock()
			if !said[hostport] {
				said[hostport] = true
				log.Printf("egress refused: %s, which no rule allows (worker.microvm.egress)", hostport)
			}
		}},
		Log: cfg.Log,
	})
}

// vmEnv is a run's environment in its microVM: its domain's home, and the egress proxy.
func (cfg Config) vmEnv(env []string) []string {
	proxy := fmt.Sprintf("http://127.0.0.1:%d", microvm.ProxyPort)
	out := make([]string, 0, len(env)+8)
	for _, kv := range env {
		k, _, _ := strings.Cut(kv, "=")
		switch strings.ToUpper(k) {
		case "HOME", "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY":
			continue
		}
		out = append(out, kv)
	}
	return append(out, "HOME="+cfg.domain.home(), "HTTPS_PROXY="+proxy, "https_proxy="+proxy, "HTTP_PROXY="+proxy, "http_proxy="+proxy,
		"NO_PROXY=localhost,127.0.0.1", "no_proxy=localhost,127.0.0.1")
}
