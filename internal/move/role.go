package move

import (
	"context"
	"errors"
	"log/slog"
	"time"

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
	return r.Rows.SetGates(ctx, s.Repositories, "", "")
}

func (r *RepositoryRole) Finish(ctx context.Context, s Scope, cell string) error {
	return r.Rows.SetGates(ctx, s.Repositories, StateMoved, cell)
}

func (r *RepositoryRole) Objects() Objects { return r.Kept }

// Kept is how long a role keeps what moved away before it deletes it.
const Kept = 24 * time.Hour

// purgeEvery is how often a role looks for repositories that moved away long enough ago.
const purgeEvery = 10 * time.Minute

// Purge deletes, a day after it, what a role holds of the repositories that moved away: a
// node.Application's Start. Replicas beside each other may delete the same rows, harmlessly.
func (r *RepositoryRole) Purge(ctx context.Context) error {
	for {
		repos, err := r.Rows.MovedBefore(ctx, time.Now().Add(-Kept))
		if err == nil && len(repos) > 0 {
			s := Scope{Repositories: repos}
			if r.Kept != nil {
				err = r.Kept.Drop(ctx, s)
			}
			if err == nil {
				err = r.Drop(ctx, s)
			}
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("move: deleting what moved away", slog.String("role", r.RoleName), slog.Any("err", err))
		}
		if err == nil && len(repos) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(purgeEvery):
		}
	}
}
