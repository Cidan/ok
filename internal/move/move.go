// Package move moves a fork network between cells (docs/hosted.md, "Cells, as built"): what every
// role but identity serves of it (Server, over the role's Role), and what its gates read — the
// repository or network a transaction writes for (WithRepository, WithNetwork) and the refusal a
// frozen one gets (Frozen).
package move

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Scope is a network that moves, and its repositories.
type Scope struct {
	Network      int64
	Repositories []int64
}

func scopeOf(s *okpb.MoveScope) Scope {
	return Scope{Network: s.GetNetworkId(), Repositories: s.GetRepositories()}
}

// Proto is the scope as a request carries it.
func (s Scope) Proto() *okpb.MoveScope {
	return &okpb.MoveScope{NetworkId: s.Network, Repositories: s.Repositories}
}

// Has reports whether a repository is the scope's.
func (s Scope) Has(repo int64) bool {
	for _, r := range s.Repositories {
		if r == repo {
			return true
		}
	}
	return false
}

// ErrFrozen is a write refused because its repository's network is moving.
var ErrFrozen = errors.New("the repository's network is moving")

// Frozen is the refusal of a write about a frozen network's repository: UNAVAILABLE, with an
// ok.v1.Frozen detail, which ok's clients retry, as nothing was done.
func Frozen(network int64) error {
	st, err := status.New(codes.Unavailable, fmt.Sprintf("network %d is moving between cells: try again in a moment", network)).
		WithDetails(&okpb.Frozen{NetworkId: network})
	if err != nil {
		return status.Errorf(codes.Unavailable, "network %d is moving between cells", network)
	}
	return &frozenError{st: st}
}

type frozenError struct{ st *status.Status }

func (e *frozenError) Error() string              { return e.st.Message() }
func (e *frozenError) GRPCStatus() *status.Status { return e.st }
func (e *frozenError) Is(target error) bool       { return target == ErrFrozen }

// IsFrozen reports whether err is a frozen network's refusal, here or from another service.
func IsFrozen(err error) bool {
	if errors.Is(err, ErrFrozen) {
		return true
	}
	var se interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &se) || se.GRPCStatus().Code() != codes.Unavailable {
		return false
	}
	for _, d := range se.GRPCStatus().Details() {
		if _, ok := d.(*okpb.Frozen); ok {
			return true
		}
	}
	return false
}

type repositoryKey struct{}
type networkKey struct{}

// WithRepository is ctx for writes about a repository: each write transaction under it reads the
// repository's gate first (docs/hosted.md, "Cells, as built").
func WithRepository(ctx context.Context, repo int64) context.Context {
	if repo == 0 {
		return ctx
	}
	return context.WithValue(ctx, repositoryKey{}, repo)
}

// RepositoryOf is the repository ctx writes about, if any.
func RepositoryOf(ctx context.Context) (int64, bool) {
	r, ok := ctx.Value(repositoryKey{}).(int64)
	return r, ok
}

// WithNetwork is ctx for writes about a network as a whole: its pack compaction, its chunk index.
func WithNetwork(ctx context.Context, network int64) context.Context {
	if network == 0 {
		return ctx
	}
	return context.WithValue(ctx, networkKey{}, network)
}

// NetworkOf is the network ctx writes about as a whole, if any.
func NetworkOf(ctx context.Context) (int64, bool) {
	n, ok := ctx.Value(networkKey{}).(int64)
	return n, ok
}

// Role is a role's side of a move: its rows and objects about a network, and its gates.
type Role interface {
	// Name is the role's, as cells list it: scm, control, automation, deploy.
	Name() string
	// Tables are the tables that hold rows about a network, parents first.
	Tables(ctx context.Context) ([]string, error)
	// Export is up to limit of a table's rows about the scope, after a row by key.
	Export(ctx context.Context, s Scope, table string, after *okpb.Row, limit int) ([]*okpb.Row, error)
	// Import writes a table's rows, over any of the same keys.
	Import(ctx context.Context, s Scope, table string, rows []*okpb.Row) error
	// Imported finishes an import: what numbers rows moves past those it brought; the scope stays
	// frozen here until Open.
	Imported(ctx context.Context, s Scope) error
	// Open lets the scope be written here — the directory names this cell its cell — and brings up
	// to date what the role keeps of it outside the rows moved.
	Open(ctx context.Context, s Scope) error
	// Drop deletes every row about the scope, a bounded transaction at a time.
	Drop(ctx context.Context, s Scope) error
	// Freeze refuses every write about the scope from its commit, waiting for those under way;
	// Thaw undoes it. Finish records the scope moved to a cell: writes are refused, and its rows
	// and objects deleted a day later.
	Freeze(ctx context.Context, s Scope) error
	Thaw(ctx context.Context, s Scope) error
	Finish(ctx context.Context, s Scope, cell string) error
	// Objects are its objects about a network; nil for a role that keeps none.
	Objects() Objects
}

// Objects are a role's objects about a network.
type Objects interface {
	// List is up to limit of the scope's objects, after a key, in key order.
	List(ctx context.Context, s Scope, after string, limit int) ([]*okpb.MovedObject, error)
	// Open reads one of the scope's objects.
	Open(ctx context.Context, s Scope, key string) (io.ReadCloser, error)
	// Has reports whether the role holds an object.
	Has(ctx context.Context, key string) (bool, error)
	// Put writes an object.
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	// Drop deletes the scope's objects.
	Drop(ctx context.Context, s Scope) error
}
