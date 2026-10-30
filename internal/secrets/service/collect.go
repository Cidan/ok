package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/work"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// References are what a repository still holds under //secret: the SCM's.
type References interface {
	// References are the versions a repository's main, open CLs, and workspaces hold; purged is
	// whether the SCM holds the repository no more.
	References(ctx context.Context, repo int64) (refs map[store.VersionKey]bool, purged bool, err error)
}

// SCMReferences reads references through the SCM's SecretReferences service.
type SCMReferences struct{ Client scmpb.SecretReferencesClient }

func (s SCMReferences) References(ctx context.Context, repo int64) (map[store.VersionKey]bool, bool, error) {
	r, err := s.Client.ListSecretReferences(ctx, &scmpb.ListSecretReferencesRequest{RepositoryId: repo})
	if err != nil {
		return nil, false, err
	}
	switch r.GetState() {
	case scmpb.ListSecretReferencesResponse_STATE_PURGED:
		return nil, true, nil
	case scmpb.ListSecretReferencesResponse_STATE_ACTIVE:
	default:
		return nil, false, fmt.Errorf("the SCM answered repository %d's references in state %s", repo, r.GetState())
	}
	out := map[store.VersionKey]bool{}
	for _, p := range r.GetPointers() {
		var d secrets.Digest
		if len(p.GetDigest()) != len(d) {
			return nil, false, fmt.Errorf("the SCM answered a digest of %d bytes", len(p.GetDigest()))
		}
		copy(d[:], p.GetDigest())
		out[store.VersionKey{Path: p.GetPath(), Digest: d}] = true
	}
	return out, false, nil
}

// DefaultCollectHour is the hour (UTC) each repository's daily collection is due from.
const DefaultCollectHour = 10

// collector is who a collection's destructions are recorded as.
const collector = "ok secrets collection"

// Collector is ok secrets' collection (docs/secrets.md, "The SCM"): each repository's pass, once
// a day, keyed work done by one replica at a time — versions nothing references marked, then
// destroyed past the grace; a repository the SCM purged, purged here; and keys wrapped by a root
// the repository no longer names brought under the one it does (Store.FollowRoot).
type Collector struct {
	Store *store.Store
	Refs  References
	// Hour is when each day's passes are due, UTC; Grace how long a version nothing references
	// is kept (store.DefaultGrace unless set).
	Hour  int
	Grace time.Duration
	Log   *slog.Logger
}

// Run does the passes as they come due until ctx ends: each repository's booked by the
// transaction that makes its first key, and by each pass for the next day's.
func (c *Collector) Run(ctx context.Context) error {
	go func() {
		if err := work.Once(ctx, c.Store.Work(), scheduleWork, c.schedule, c.log()); err != nil && ctx.Err() == nil {
			c.log().Warn("ok secrets: booking the repositories' collections", "err", err)
		}
	}()
	return (&work.Runner{
		Kind:  store.CollectWork,
		Store: c.Store.Work(),
		Do: func(ctx context.Context, repo int64) error {
			ctx = move.WithRepository(ctx, repo)
			purged, err := c.pass(ctx, repo)
			if err != nil || purged {
				return err
			}
			return c.Store.Work().MarkAt(ctx, store.CollectWork, repo, nextDue(time.Now().UTC(), c.Hour))
		},
		Workers: 2,
		Log:     c.log(),
	}).Run(ctx)
}

// scheduleWork books, once, what a release that looked for its keyed work left unbooked (schedule).
const scheduleWork = "schedule"

func (c *Collector) log() *slog.Logger {
	if c.Log == nil {
		return slog.Default()
	}
	return c.Log
}

// schedule books every repository with keys for its next daily pass.
func (c *Collector) schedule(ctx context.Context) error {
	next := nextDue(time.Now().UTC(), c.Hour)
	w := c.Store.Work()
	var after int64
	for {
		repos, err := c.Store.Repositories(ctx, after, 1000)
		if err != nil {
			return err
		}
		for _, r := range repos {
			if err := w.MarkAt(ctx, store.CollectWork, r, next); err != nil {
				return err
			}
		}
		if len(repos) < 1000 {
			return nil
		}
		after = repos[len(repos)-1]
	}
}

// nextDue is the next time at hour, UTC, after now.
func nextDue(now time.Time, hour int) time.Time {
	due := time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, time.UTC)
	if !due.After(now) {
		due = due.AddDate(0, 0, 1)
	}
	return due
}

// Pass collects one repository now.
func (c *Collector) Pass(ctx context.Context, repo int64) error {
	_, err := c.pass(ctx, repo)
	return err
}

// pass collects one repository now, and reports whether it purged it, as the SCM had.
func (c *Collector) pass(ctx context.Context, repo int64) (bool, error) {
	refs, purged, err := c.Refs.References(ctx, repo)
	if err != nil {
		return false, fmt.Errorf("repository %d's references: %w", repo, err)
	}
	if purged {
		if err := c.Store.Purge(ctx, repo); err != nil {
			return false, fmt.Errorf("purging repository %d: %w", repo, err)
		}
		c.log().Info("ok secrets: purged a repository the SCM purged", "repository", repo)
		return true, nil
	}
	if moved, err := c.Store.FollowRoot(ctx, repo); err != nil {
		return false, fmt.Errorf("repository %d's root: %w", repo, err)
	} else if moved {
		if err := c.Store.Audit(ctx, store.AuditEntry{Repository: repo, Actor: collector, Op: "rewrap", Outcome: "keys brought under the repository's root"}); err != nil {
			return false, err
		}
	}
	grace := c.Grace
	if grace <= 0 {
		grace = store.DefaultGrace
	}
	res, err := c.Store.Collect(ctx, repo, func(path string, d secrets.Digest) bool { return refs[store.VersionKey{Path: path, Digest: d}] },
		time.Now(), grace, collector)
	if err != nil {
		return false, err
	}
	if res.Destroyed > 0 {
		if err := c.Store.Audit(ctx, store.AuditEntry{Repository: repo, Actor: collector, Op: "destroy",
			Outcome: fmt.Sprintf("%d versions unreferenced for %v", res.Destroyed, grace)}); err != nil {
			return false, err
		}
	}
	c.log().Info("ok secrets: collected", "repository", repo, "marked", res.Marked, "cleared", res.Cleared, "destroyed", res.Destroyed,
		"unseals", res.Unseals)
	return false, nil
}
