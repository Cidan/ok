package store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// movesClient serves a store's moves on loopback, every caller ok identity, as a move's directory
// calls it.
func movesClient(t *testing.T, srv *move.Server) okpb.MovesClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	asIdentity := func(ctx context.Context) context.Context {
		return authn.NewContext(ctx, authn.Caller{Principal: "identity.ok.robot.example.com", Role: okpb.Caller_CALLER_IDENTITY})
	}
	g := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			return h(asIdentity(ctx), req)
		}),
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			return h(srv, &identityStream{ServerStream: ss, ctx: asIdentity(ss.Context())})
		}))
	srv.Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return okpb.NewMovesClient(conn)
}

type identityStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *identityStream) Context() context.Context { return s.ctx }

// otherDatabase is a database of the other kind than the suite's — Spanner for PostgreSQL's, and
// back — when the environment has one, for a move between them; false when it has none.
func otherDatabase(t *testing.T) (testDB, bool) {
	t.Helper()
	switch os.Getenv("OK_TEST_DATABASE") {
	case "spanner":
		if os.Getenv("PGHOST") == "" {
			return nil, false
		}
		t.Setenv("OK_TEST_DATABASE", "postgres")
		defer t.Setenv("OK_TEST_DATABASE", "spanner")
		return newTestDB(t, 32), true
	default:
		if os.Getenv("OK_TEST_SPANNER_OMNI") == "" && os.Getenv("OK_TEST_SPANNER_INSTANCE") == "" {
			return nil, false
		}
		return newSpannerDB(t), true
	}
}

// A network moves between two cells' SCMs: its objects copied while it is written; frozen, its
// writes refused; then every row copied, the destination reading it as the source did — its main's
// path history, its files, its CLs, its operation log — passing fsck, and written on, its counters
// past what came; the source refusing it, naming the cell, and dropping it whole. Between two
// databases of the suite's kind, and to the other kind when the environment has it.
func TestMoveNetwork(t *testing.T) {
	t.Run("same database", func(t *testing.T) { testMoveNetwork(t, testStore(t)) })
	t.Run("the other database", func(t *testing.T) {
		db, ok := otherDatabase(t)
		if !ok {
			t.Skip("the environment has one kind of database")
		}
		testMoveNetwork(t, mustOpen(t, db, testobjects.Store(t), Config{}))
	})
}

func testMoveNetwork(t *testing.T, dst *Store) {
	ctx := context.Background()
	src := newCLEnv(t)
	steps := historySteps(10)
	mains := landSteps(t, src, steps[:6])
	compactAll(t, src.s, src.repo)
	mains = append(mains, landSteps(t, src, steps[6:])...)
	fork := newFork(t, src.s, src.repo)
	forkEnv := &clEnv{t: t, s: src.s, repo: fork.ID}
	forkEnv.landChange("the fork's own", map[string]string{"fork.txt": "only the fork\n"})
	cl := src.commit(alice, newChange(), src.head(), "a CL under review", map[string]string{"review.txt": "under review\n"})
	number := src.open(cl)
	net := netOf(t, src.s, src.repo)
	scope := &okpb.MoveScope{NetworkId: int64(net), Repositories: []int64{src.repo, fork.ID}, Role: cells.SCM}
	want := pathAnswers(t, src.s, src.repo, mains)
	wantCL, err := src.s.GetChangeList(ctx, src.repo, number)
	if err != nil {
		t.Fatal(err)
	}

	srcMoves := movesClient(t, move.NewServer(src.s.Mover(), okpb.Caller_CALLER_SCM, nil))
	dstMoves := movesClient(t, move.NewServer(dst.Mover(), okpb.Caller_CALLER_SCM, func(context.Context, string) (okpb.MovesClient, error) {
		return srcMoves, nil
	}))
	got, err := dstMoves.Import(ctx, &okpb.ImportRequest{Scope: scope, Source: "a", Stage: okpb.ImportRequest_STAGE_OBJECTS})
	if err != nil || got.GetObjects() == 0 || got.GetRows() != 0 {
		t.Fatalf("copying the objects: %v, %v", got, err)
	}

	// Frozen: a write about either repository is refused, nothing done; a read is served.
	if _, err := srcMoves.Freeze(ctx, scope); err != nil {
		t.Fatal(err)
	}
	for _, repo := range []int64{src.repo, fork.ID} {
		err := src.s.inTx(move.WithRepository(ctx, repo), func(context.Context, *txn) error { return nil })
		if !move.IsFrozen(err) {
			t.Errorf("a write about repository %d while frozen: %v", repo, err)
		}
	}
	b := newBuilder(t)
	b.change(newChange(), src.head(), "while frozen", map[string]string{"frozen.txt": "no\n"})
	if _, err := publish(move.WithRepository(ctx, src.repo), src.s, src.repo, alice, b, nil); !move.IsFrozen(err) {
		t.Errorf("a publish while frozen: %v", err)
	}
	if got := pathAnswers(t, src.s, src.repo, mains); got != want {
		t.Errorf("reading while frozen:\n%s\nwant\n%s", got, want)
	}

	got, err = dstMoves.Import(ctx, &okpb.ImportRequest{Scope: scope, Source: "a", Stage: okpb.ImportRequest_STAGE_ALL})
	if err != nil || got.GetRows() == 0 {
		t.Fatalf("copying everything: %v, %v", got, err)
	}
	// Again, as a directory that lost track of the first does: the same rows, whole.
	again, err := dstMoves.Import(ctx, &okpb.ImportRequest{Scope: scope, Source: "a", Stage: okpb.ImportRequest_STAGE_ALL})
	if err != nil || again.GetRows() != got.GetRows() || again.GetObjects() != 0 {
		t.Fatalf("copying everything again: %v, %v; the first copied %v", again, err, got)
	}

	// The destination reads it as the source did, from a replica with nothing cached too.
	for _, s := range []*Store{dst, reopen(t, dst)} {
		if got := pathAnswers(t, s, src.repo, mains); got != want {
			t.Fatalf("the destination's path history:\n%s\nwant\n%s", got, want)
		}
		// As each database reads it — the same times and lists, a time's location and an empty
		// list's representation aside.
		gotCL, err := s.GetChangeList(ctx, src.repo, number)
		if err != nil || fmt.Sprintf("%+v", gotCL) != fmt.Sprintf("%+v", wantCL) {
			t.Errorf("the CL at the destination: %+v, %v\nwant %+v", gotCL, err, wantCL)
		}
		if r := mustRepo(t, s, fork.ID); r.Parent != src.repo || r.Network != net {
			t.Errorf("the fork at the destination: %+v", r)
		}
	}
	clean(t, dst, src.repo)
	clean(t, dst, fork.ID)

	// Frozen there until it opens, once the directory names the destination; then written on: a
	// landing, a publish, compaction — numbered past what came.
	if err := dst.inTx(move.WithRepository(ctx, src.repo), func(context.Context, *txn) error { return nil }); !move.IsFrozen(err) {
		t.Errorf("a write at the destination before it opened: %v", err)
	}
	if _, err := dstMoves.Open(ctx, scope); err != nil {
		t.Fatal(err)
	}
	moved := &clEnv{t: t, s: dst, repo: src.repo}
	mains = append(mains, landSteps(t, moved, historySteps(12)[10:])...)
	compactAll(t, dst, src.repo)
	clean(t, dst, src.repo)

	// Finished: the source refuses it, naming the destination, and drops it.
	if _, err := srcMoves.Finish(ctx, &okpb.FinishRequest{Scope: scope, Destination: "b"}); err != nil {
		t.Fatal(err)
	}
	err = src.s.inTx(move.WithRepository(ctx, src.repo), func(context.Context, *txn) error { return nil })
	if m, ok := cells.MovedOf(err); !ok || m.GetCell() != "b" {
		t.Errorf("a write after the move: %v", err)
	}
	if err := src.s.PurgeMoved(ctx, int64(net)); err != nil {
		t.Fatal(err)
	}
	for _, repo := range []int64{src.repo, fork.ID} {
		if _, err := src.s.GetRepository(ctx, repo); !errors.Is(err, ErrNotFound) {
			t.Errorf("repository %d at the source after its purge: %v", repo, err)
		}
		for _, table := range []string{"object", "commit", "changelist", "op_log", "event"} {
			if n := dbOf(src.s).count(t, table, `repository_id = @repo`, map[string]any{"repo": repo}); n != 0 {
				t.Errorf("%s: %d rows of repository %d left at the source", table, n, repo)
			}
		}
	}
	if n := dbOf(src.s).count(t, "pack", `network_id = @net`, map[string]any{"net": int64(net)}); n != 0 {
		t.Errorf("%d packs left at the source", n)
	}
	if objs, err := src.s.blobs.List(ctx, netPrefix(int64(net))); err != nil || len(objs) != 0 {
		t.Errorf("%d objects left at the source: %v", len(objs), err)
	}
}

// A move carries a content index's log as it applies — the rows its generation's fold did not
// read, in their order — to either database, which reads that order from the rows alone: a put,
// a sweep's drop, and the put again leave the chunk held; a put and a drop leave it gone; and a
// network's chunk index carries its own.
func TestMoveCarriesTheLogInOrder(t *testing.T) {
	t.Run("same database", func(t *testing.T) { testMoveCarriesTheLog(t, testStore(t)) })
	t.Run("the other database", func(t *testing.T) {
		db, ok := otherDatabase(t)
		if !ok {
			t.Skip("the environment has one kind of database")
		}
		testMoveCarriesTheLog(t, mustOpen(t, db, testobjects.Store(t), Config{}))
	})
}

func testMoveCarriesTheLog(t *testing.T, dst *Store) {
	ctx := context.Background()
	src := testStore(t)
	repo := newRepo(t, src).ID
	net := netOf(t, src, repo)
	sc, nsc := repoScope(repo), backend.NetworkScope(net)
	held, gone, located := chunkID(1), chunkID(2), chunkID(3)
	drop := func(id encoding.ID) contentindex.Update {
		return contentindex.Update{Op: contentindex.Drop, ID: id, Entry: contentindex.Entry{Kind: encoding.KindChunk}}
	}
	// A fold first, so what it read is in the generation the move carries, not the log.
	logAs(t, src, sc, false, putChunk(chunkID(9)))
	if _, _, _, err := src.foldIndex(ctx, sc); err != nil {
		t.Fatal(err)
	}
	logAs(t, src, sc, false, putChunk(held))
	logAs(t, src, sc, true, drop(held))
	logAs(t, src, sc, false, putChunk(held))
	logAs(t, src, sc, false, putChunk(gone))
	logAs(t, src, sc, true, drop(gone))
	logAs(t, src, nsc, false, putChunk(located))

	scope := &okpb.MoveScope{NetworkId: int64(net), Repositories: []int64{repo}, Role: cells.SCM}
	srcMoves := movesClient(t, move.NewServer(src.Mover(), okpb.Caller_CALLER_SCM, nil))
	dstMoves := movesClient(t, move.NewServer(dst.Mover(), okpb.Caller_CALLER_SCM, func(context.Context, string) (okpb.MovesClient, error) {
		return srcMoves, nil
	}))
	if _, err := srcMoves.Freeze(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if got, err := dstMoves.Import(ctx, &okpb.ImportRequest{Scope: scope, Source: "a", Stage: okpb.ImportRequest_STAGE_ALL}); err != nil || got.GetRows() == 0 {
		t.Fatalf("copying everything: %v, %v", got, err)
	}
	if n := dbOf(dst).count(t, "content_log", `scope = 0 AND owner_id = @repo`, map[string]any{"repo": repo}); n != 5 {
		t.Errorf("%d of the repository's log rows carried, want the 5 its fold did not read", n)
	}
	for _, s := range []*Store{dst, reopen(t, dst)} {
		x := indexOf(t, s, repo)
		if _, ok := x.Get(held); !ok {
			t.Error("a chunk put, dropped, and put again is not held at the destination")
		}
		if _, ok := x.Get(gone); ok {
			t.Error("a chunk put and dropped is held at the destination")
		}
		if _, ok := x.Get(chunkID(9)); !ok {
			t.Error("what the fold read is not held at the destination")
		}
		if _, ok := locationsOf(t, s, repo).Get(located); !ok {
			t.Error("the network's chunk index lacks its logged location at the destination")
		}
	}
}
