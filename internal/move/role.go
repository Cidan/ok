package move

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/Cidan/ok/internal/work"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// RepositoryRole is the side of a move of a role whose rows are about repositories, not a network
// as a whole: ok control, ok automation, the deployer. A network's repositories move together.
type RepositoryRole struct {
	// RoleName is the role's, as cells list it.
	RoleName string
	Rows     Rows
	// Kept are its objects about the repositories, if it keeps any.
	Kept Objects
	// Frozen, when set, runs once the gates closed — no new work about the repositories can start —
	// its writes passing them: a role gives up the work it holds (ok automation requeues its runs),
	// or refuses the freeze while it cannot (the deployer while a deployment is under way), when the
	// gates open again.
	Frozen func(ctx context.Context, repos []int64) error
	// Arrived, when set, runs once the repositories are open here: a role brings up to date what it
	// keeps of them outside the rows moved.
	Arrived func(ctx context.Context, repos []int64) error
	// Thawed, when set, runs once a move called off opened the repositories' gates here again: a
	// role takes up the work it gave up as they froze.
	Thawed func(ctx context.Context, repos []int64) error
	// Work is the role's keyed work's store: each repository that moved away has its purge booked
	// there, Kept on (Purge).
	Work work.Store
}

var _ Role = (*RepositoryRole)(nil)

// dropRows bounds the rows one statement of a drop deletes.
const dropRows = 10_000

func (r *RepositoryRole) Name() string { return r.RoleName }

func (r *RepositoryRole) Tables(ctx context.Context) ([]string, error) { return r.Rows.Order(ctx) }

func (r *RepositoryRole) Export(ctx context.Context, s Scope, table string, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	return r.Rows.Export(ctx, s.Repositories, table, after, limit)
}

func (r *RepositoryRole) Import(ctx context.Context, _ Scope, table string, rows []*okpb.Row) error {
	return r.Rows.Import(ctx, table, rows)
}

func (r *RepositoryRole) Imported(ctx context.Context, s Scope) error {
	return r.Rows.Finish(ctx, s.Repositories)
}

func (r *RepositoryRole) Open(ctx context.Context, s Scope) error {
	if err := r.Rows.SetGates(ctx, s.Repositories, "", ""); err != nil {
		return err
	}
	if r.Arrived != nil {
		return r.Arrived(ctx, s.Repositories)
	}
	return nil
}

func (r *RepositoryRole) Drop(ctx context.Context, s Scope) error {
	for {
		n, err := r.Rows.Drop(ctx, s.Repositories, dropRows)
		if err != nil || n < dropRows {
			return err
		}
	}
}

func (r *RepositoryRole) Freeze(ctx context.Context, s Scope) error {
	if err := r.Rows.SetGates(ctx, s.Repositories, StateFrozen, ""); err != nil {
		return err
	}
	if r.Frozen == nil {
		return nil
	}
	if err := r.Frozen(Untagged(ctx), s.Repositories); err != nil {
		if terr := r.Rows.SetGates(context.WithoutCancel(ctx), s.Repositories, "", ""); terr != nil {
			return errors.Join(err, terr)
		}
		return err
	}
	return nil
}

func (r *RepositoryRole) Thaw(ctx context.Context, s Scope) error {
	if err := r.Rows.SetGates(ctx, s.Repositories, "", ""); err != nil {
		return err
	}
	if r.Thawed == nil {
		return nil
	}
	return r.Thawed(Untagged(ctx), s.Repositories)
}

func (r *RepositoryRole) Finish(ctx context.Context, s Scope, cell string) error {
	if err := r.Rows.SetGates(ctx, s.Repositories, StateMoved, cell); err != nil {
		return err
	}
	at := time.Now().Add(Kept)
	for _, repo := range s.Repositories {
		if err := r.Work.MarkAt(ctx, MovedWork, repo, at); err != nil {
			return err
		}
	}
	return nil
}

func (r *RepositoryRole) Objects() Objects { return r.Kept }

// Kept is how long a role keeps what moved away before it deletes it.
const Kept = 24 * time.Hour

// MovedWork is a repository that moved away, its purge here as keyed work (internal/work): booked
// Kept on as the move finishes.
const MovedWork = "moved-purge"

// movedUpgrade books, once, the purges of the repositories a release before saw move away.
const movedUpgrade = "moved-upgrade"

// Purge deletes, a day after it, what a role holds of each repository that moved away: a
// node.Application's Start. Each repository's purge comes as its job does, to one replica at a time;
// one that moved back meanwhile keeps what it brought. Once, it books the purges of the repositories
// a release before saw move away.
func (r *RepositoryRole) Purge(ctx context.Context) error {
	go func() {
		if err := work.Once(ctx, r.Work, movedUpgrade, r.bookMoved, nil); err != nil && ctx.Err() == nil {
			slog.Warn("move: booking the purges of a release before", slog.String("role", r.RoleName), slog.Any("err", err))
		}
	}()
	return (&work.Runner{Kind: MovedWork, Store: r.Work, Do: r.purge, Workers: 1}).Run(ctx)
}

// purge deletes what the role holds of a repository that moved away Kept ago or more; one that
// moved less long ago is booked for then, and one that did not, or moved back, is left.
func (r *RepositoryRole) purge(ctx context.Context, repo int64) error {
	at, moved, err := r.Rows.MovedAt(ctx, repo)
	switch {
	case err != nil:
		return err
	case !moved:
		return nil
	case time.Since(at) < Kept:
		return r.Work.MarkAt(ctx, MovedWork, repo, at.Add(Kept))
	}
	s := Scope{Repositories: []int64{repo}}
	if r.Kept != nil {
		if err := r.Kept.Drop(ctx, s); err != nil {
			return err
		}
	}
	return r.Drop(ctx, s)
}

// bookMoved books the purge of every repository that moved away.
func (r *RepositoryRole) bookMoved(ctx context.Context) error {
	for after := int64(math.MinInt64); ; {
		repos, err := r.Rows.MovedBefore(ctx, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), after, 1000)
		if err != nil {
			return err
		}
		for _, repo := range repos {
			if err := r.Work.Mark(ctx, MovedWork, repo); err != nil {
				return err
			}
		}
		if len(repos) < 1000 {
			return nil
		}
		after = repos[len(repos)-1]
	}
}
