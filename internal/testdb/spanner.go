package testdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/data/spannerconn"
)

// Spanner is reached one of three ways, as the environment names it: OK_TEST_SPANNER_OMNI, a
// Spanner Omni single server (host:port, plain text), in whose one instance the tests make their
// databases; OK_TEST_SPANNER_INSTANCE, a real instance (projects/P/instances/I) the environment's
// credentials administer; or SPANNER_EMULATOR_HOST, an emulator, in which the tests make an
// instance of their own. A test with none is skipped, unless OK_TEST_SPANNER is "require", which
// fails it instead.
const (
	omniInstance     = "projects/default/instances/default"
	emulatorProject  = "ok-test"
	emulatorInstance = "ok-test"
)

var emulatorReady sync.Once
var emulatorErr error

// SpannerConnection is how the environment reaches Spanner, and the instance tests make their
// databases in.
func SpannerConnection(t testing.TB) (spannerconn.Connection, string) {
	t.Helper()
	if addr := os.Getenv("OK_TEST_SPANNER_OMNI"); addr != "" {
		return spannerconn.Connection{Omni: addr, PlainText: true}, omniInstance
	}
	if i := os.Getenv("OK_TEST_SPANNER_INSTANCE"); i != "" {
		return spannerconn.Connection{}, i
	}
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		if os.Getenv("OK_TEST_SPANNER") == "require" {
			t.Fatal("testdb: no Spanner (OK_TEST_SPANNER_OMNI, OK_TEST_SPANNER_INSTANCE, SPANNER_EMULATOR_HOST), and OK_TEST_SPANNER=require")
		}
		t.Skip("testdb: no Spanner in the environment (OK_TEST_SPANNER_OMNI, OK_TEST_SPANNER_INSTANCE, SPANNER_EMULATOR_HOST)")
	}
	name := "projects/" + emulatorProject + "/instances/" + emulatorInstance
	emulatorReady.Do(func() {
		ctx := context.Background()
		admin, err := instance.NewInstanceAdminClient(ctx)
		if err != nil {
			emulatorErr = err
			return
		}
		defer admin.Close()
		op, err := admin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
			Parent: "projects/" + emulatorProject, InstanceId: emulatorInstance,
			Instance: &instancepb.Instance{Config: "projects/" + emulatorProject + "/instanceConfigs/emulator-config", DisplayName: "ok's tests", NodeCount: 1},
		})
		if status.Code(err) == codes.AlreadyExists {
			return
		}
		if err == nil {
			_, err = op.Wait(ctx)
		}
		emulatorErr = err
	})
	if emulatorErr != nil {
		t.Fatalf("testdb: the emulator's instance: %v", emulatorErr)
	}
	return spannerconn.Connection{}, name
}

// SpannerAdmin is a database admin client of the environment's Spanner, closed when the test ends.
func SpannerAdmin(t testing.TB) *database.DatabaseAdminClient {
	t.Helper()
	conn, _ := SpannerConnection(t)
	admin, err := conn.Admin(context.Background())
	if err != nil {
		t.Fatalf("testdb: Spanner's database admin: %v", err)
	}
	t.Cleanup(func() { admin.Close() })
	return admin
}

// Spanner returns the name of a new, empty Spanner database (projects/P/instances/I/databases/D)
// for this test alone, in GoogleSQL, made with ddl; it is dropped when the test ends.
func Spanner(t testing.TB, ddl ...string) string {
	t.Helper()
	// A database ID is 2 to 30 of [a-z0-9_-], starting with a letter.
	return SpannerDatabase(t, fmt.Sprintf("t%d-%d", os.Getpid()%1_000_000, counter.Add(1)), ddl...)
}

// SpannerPrefix is a database name prefix for this test alone, as a configuration's
// database.spanner.prefix names its roles' databases: at most 19 characters.
func SpannerPrefix() string { return fmt.Sprintf("e%d-%d", os.Getpid()%1_000_000, counter.Add(1)) }

// SpannerDatabase is Spanner, the database named id in the environment's instance.
func SpannerDatabase(t testing.TB, id string, ddl ...string) string {
	t.Helper()
	return SpannerDatabases(t, map[string][]string{id: ddl})[id]
}

// SpannerDatabases are SpannerDatabase's of each id, its ddl, made at once, by id.
func SpannerDatabases(t testing.TB, ddl map[string][]string) map[string]string {
	t.Helper()
	_, inst := SpannerConnection(t)
	admin := SpannerAdmin(t)
	ctx := context.Background()
	var mu sync.Mutex
	names := map[string]string{}
	var errs []error
	var wg sync.WaitGroup
	for id, stmts := range ddl {
		wg.Go(func() {
			op, err := admin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{Parent: inst, CreateStatement: "CREATE DATABASE `" + id + "`",
				ExtraStatements: stmts})
			var db *databasepb.Database
			if err == nil {
				db, err = op.Wait(ctx)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("creating Spanner database %s: %w", id, err))
				return
			}
			names[id] = db.GetName()
		})
	}
	wg.Wait()
	for _, name := range names {
		t.Cleanup(func() {
			if err := admin.DropDatabase(context.Background(), &databasepb.DropDatabaseRequest{Database: name}); err != nil &&
				status.Code(err) != codes.NotFound {
				t.Errorf("testdb: dropping Spanner database %s: %v", name, err)
			}
		})
	}
	if len(errs) > 0 {
		t.Fatalf("testdb: %v", errors.Join(errs...))
	}
	return names
}

// SpannerClient is a client of a Spanner database (Spanner's name), closed when the test ends.
func SpannerClient(t testing.TB, name string) *spanner.Client {
	t.Helper()
	conn, _ := SpannerConnection(t)
	c, err := conn.Client(context.Background(), name)
	if err != nil {
		t.Fatalf("testdb: a Spanner client of %s: %v", name[strings.LastIndex(name, "/")+1:], err)
	}
	t.Cleanup(c.Close)
	return c
}
