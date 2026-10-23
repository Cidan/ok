# Building and testing ok

ok is a Go module (the roles and the `ok` command) and a Cargo workspace (`jj/`: ok's jj and okfs)
in one repository. Both build with their own tools and nothing else.

## What you need

- **Go**, the version `go.mod` names or newer.
- **Rust**, a current stable toolchain (tested with 1.98), and a C compiler: TLS is aws-lc, which
  builds C. The protocols compile in the build with
  [protox](https://github.com/andrewhickman/protox), so no `protoc` is needed.
- **Linux.** Virtual workspaces need `/dev/fuse`, and FUSE 3's `fusermount3` (the `fuse3`
  package) where jj may not mount itself; real checkouts (`--real`) need neither.

## Building

```bash
go build ./cmd/ok                      # the ok binary: every role, and its commands
cargo build --release -p ok-jj         # target/release/jj
```

Generated files are committed; regenerate them after changing their sources:

| Source | Regenerate with |
|---|---|
| `proto/**/*.proto` | `scripts/protoc.sh` (needs `protoc` on the `PATH`, or `$PROTOC`; the Go plugins come from `go.mod`) |
| `*.templ` | `go tool templ generate` |
| `input.css` beside a page's templates | `scripts/css.sh` (needs the [Tailwind CSS](https://tailwindcss.com) v4 command line, as `tailwindcss` or `$TAILWINDCSS`) |

The Rust side reads `proto/` directly, so it needs no regeneration.

## Tests

```bash
go test ./...
cargo test --workspace
```

Unit tests need nothing. Tests of the database, object storage, and the client against a real
`ok scm` need PostgreSQL and an S3-compatible store, named as their own tools name them:

| Variable | |
|---|---|
| `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` | A PostgreSQL server (tested with 18) and a role that may create databases. Each Go test makes a database of its own and drops it; the Rust suites' `ok scm` keeps its tables in a schema of its own, in `PGDATABASE` or `postgres`. |
| `AWS_ENDPOINT_URL_S3`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | An S3-compatible server. Tests make the buckets they use. |
| `OK_TEST_OBJECTS` | Where the tests that keep objects keep them: `s3` (the default), the server above; `gcs`, a GCS emulator each test process runs on its loopback, needing no server (`internal/testdb/testobjects`); or `fs`, a directory. The S3 store's own tests use S3 whatever it says. |
| `OK_GCS_BINARY` | The GCS emulator the Rust suites start beside their SCM with `OK_TEST_OBJECTS=gcs` (`go build -o /tmp/ok-test-gcs ./internal/testdb/testobjects/gcsserver`; default: `ok-test-gcs` on the `PATH`). |
| `OK_BINARY` | The `ok` binary the Rust suites run as their SCM (default: `ok` on the `PATH`). |
| `OK_TEST_INTEGRATION` | Without the servers, those tests are skipped and say so; `require` fails them instead, as CI does. |
| `OK_BAZEL` | Bazel (default: `bazel`, else `bazelisk`, on the `PATH`), for the tests that run it: the Bazel driver's, the deployer's, and ok automation's end-to-end suite, whose workers run real Bazel. They need bubblewrap (`bwrap`) too. Without them, those tests are skipped and say so. |
| `OK_JJ` | ok's jj, built (`cargo build -p ok-jj`), for the end-to-end suites whose people upload with it: ok control's, ok automation's, and the deployer's. Without it, they are skipped and say so. |
| `OK_TEST_BAZEL`, `OK_TEST_JJ` | `require` fails the tests that cannot run without Bazel, or jj, instead, as CI does. |

Any PostgreSQL and S3 will do. With containers — [versitygw](https://github.com/versity/versitygw)
is a small S3 server over a directory:

```bash
podman run -d --name ok-pg -e POSTGRES_PASSWORD=okpass -p 127.0.0.1:55432:5432 docker.io/library/postgres:18
podman run -d --name ok-s3 -e ROOT_ACCESS_KEY=okaccess -e ROOT_SECRET_KEY=oksecretkey \
  -p 127.0.0.1:59000:7070 docker.io/versity/versitygw:latest --port :7070 posix /tmp

export PGHOST=127.0.0.1 PGPORT=55432 PGUSER=postgres PGPASSWORD=okpass
export AWS_ENDPOINT_URL_S3=http://127.0.0.1:59000 AWS_ACCESS_KEY_ID=okaccess \
  AWS_SECRET_ACCESS_KEY=oksecretkey AWS_REGION=us-east-1
export OK_TEST_INTEGRATION=require

go test ./...
go build -o /tmp/ok ./cmd/ok && OK_BINARY=/tmp/ok cargo test --workspace
cargo build -p ok-jj && OK_JJ=$PWD/target/debug/jj go test ./internal/control/e2e ./internal/automation/e2e ./internal/deploy/role
```

(`docker run` takes the same arguments.) CI runs the suites on S3 and again on GCS:

```bash
OK_TEST_OBJECTS=gcs go test ./...
go build -o /tmp/ok-test-gcs ./internal/testdb/testobjects/gcsserver
OK_TEST_OBJECTS=gcs OK_GCS_BINARY=/tmp/ok-test-gcs OK_BINARY=/tmp/ok cargo test --workspace
```

ok automation's suite builds small Bazel workspaces of Starlark rules alone, so Bazel downloads
nothing for them beyond itself. The client's suites that mount virtual workspaces need `/dev/fuse` and unprivileged user
namespaces: each runs in a user and mount namespace of its own (`okfs_fuse::testns`), where it may
mount without privilege, so they need neither root nor `fusermount3`.

## Images, the chart, CI, and releases

```bash
docker build -f deploy/images/ok.Dockerfile -t ok .                 # ok, on distroless static
docker build -f deploy/images/ok-runner.Dockerfile -t ok-runner .   # + ok's jj, bazelisk, bubblewrap, FUSE
scripts/chart-check.sh                                              # helm lint, kubeconform, ok config check
```

`.github/workflows/ci.yaml` runs on every change: the Go module and the Cargo workspace on Linux with
every integration test required (`.github/actions/test-host` sets up S3, Bazel, bubblewrap, FUSE,
and user namespaces beside a PostgreSQL service), the builds and the tests that need no server on
macOS, the chart check, and both images. `.github/workflows/release.yaml` runs on a tag `vX.Y.Z`,
which must be `Cargo.toml`'s workspace version: `ok` for Linux and macOS, `jj` built on each
platform's own runner, both images for amd64 and arm64 on ghcr.io, the chart as an OCI artifact,
and a GitHub release with the binaries and their SHA-256 sums.

The Rust toolchain is `rust-toolchain.toml`'s; the Go one is `go.mod`'s.
