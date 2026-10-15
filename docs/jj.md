# ok's jj

ok's jj is upstream [jj](https://github.com/jj-vcs/jj) — `jj-cli` and `jj-lib` — with ok's stores
(backend, op store, op heads store) registered and ok's commands built in: `jj auth`, `jj clone`,
`jj create`, and the ChangeList commands — `jj cl` (upload), `jj cl get`, `jj sync`, and a CL's
whole lifecycle: `jj cl status [-f]`, `ready`, `draft`, `reviewers`, `review`, `comment`, `resolve`,
`submit`, `abandon`, `restore`, `rollback`, `rerun`, `cancel`, `log`, and `deployment`. It is the
only way code enters ok. Its design — stores, cache, outbox, commands — is [scm.md](scm.md), "The
client".

A workspace is **virtual** unless asked otherwise ([okfs.md](okfs.md)): a FUSE file system this
binary serves, so a clone is ready at once, a file is read from ok the first time something reads
it, and every write is kept in ok as a checkpoint within about a second. `--real` makes a real
checkout, every file on disk. Virtual workspaces are Linux's (okfs serves Linux's FUSE): on macOS,
and anywhere else, every clone is a real checkout, and `--attach` is refused.

## Using it

```bash
cargo install --locked --path jj/cli               # the jj binary
jj auth login https://ok.example.com               # once per machine: approve the code in a browser
jj create hello                                    # a new repository on ok, cloned into ./hello
jj clone app ~/src/app                             # an existing one (virtual)
jj clone app ~/src/app-real --real                 # a real checkout
```

`jj auth login` signs the machine in with ok identity's device login, as `ok auth login` does —
the same credential, in the same place, so either one signs the machine in for both
([identity.md](identity.md), "Signing in"). `jj auth status` says who it is signed in as; `jj auth
logout` revokes the credential and removes it.

Both `clone` and `create` make the repository on ok first (`create`) or look it up (`clone`),
make the workspace on ok, lay out its own jj repository with the `ok` stores (a real checkout's in
`.jj/repo`, a virtual workspace's in its okfs state directory), and — in one operation — set
`main@ok` to ok's head and put the workspace's working copy on a new change on it. Nothing is ever
local-only: the operation log lives on ok, one per workspace. Every workspace is isolated from
every other, the same person's too ([scm.md](scm.md), "Decisions"): it sees only its own changes,
a `jj sync` in one touches no other, and work moves between workspaces only as CLs (upload,
submit, `jj sync`, `jj cl get`). `jj workspace add` and `jj workspace rename` are refused; `jj
clone` makes another workspace. After that it is ordinary jj.

Built-in configuration sets `trunk()` to `main@ok`, `ok.endpoint` (where new clones come from) to
the ok the machine signed in to, `snapshot.max-new-file-size` to 50 MiB (the largest file the SCM
holds; upstream stops at 1 MiB; a larger file stays untracked, and one tracked anyway — the setting
raised — is refused as it is snapshotted, naming it, and never staged), and `user.name` and
`user.email` to the person who signed in. A
clone's CL links point at the ok it came from, unless `ok.web-url` says otherwise.

ChangeLists:

```bash
jj new 'trunk()' -m 'add rate limiter'   # …edit…
jj cl                                     # upload @ (or @- when @ has no description) and its
                                          # mutable ancestors, one CL per change; prints each URL
jj cl --ready --reviewer grace            # send them for review, assigning a reviewer
jj cl get 42                              # someone else's CL 42 as cl/42@ok; jj new cl/42@ok
jj sync                                   # main@ok to ok's main, retire landed changes, follow
                                          # each cl/<n>@ok, rebase the rest onto main
```

The rest of a CL's life is `jj cl`'s too — where it stands, following it (`-f`) until it is
settled or a milestone, review, threads, submit, checks and their logs, and deployments — or the
CL's page:

```bash
jj cl status -f                           # a line each time it moves; exits once it is settled
jj cl ready --reviewer bob                # send a draft for review
jj cl review 87 --lgtm --approve          # as a reviewer
jj cl submit -f                           # submit, and follow it until deployed
jj cl status -f --until deployed --json   # ok control's JSON, one object per line
```

They call ok control's `ok.control.v1.ChangeLists` service with the machine's credential: `review/`
is the client — every call on the store's runtime, `--json` read from the service's REST surface
over the same connection so its JSON is ok control's own — and how a status is shown and when
following ends (`render.rs`), with the commands in `cli/lifecycle.rs`. `jj sync` never makes a
change look divergent: a local change whose CL landed is retired onto the landed commit (which
carries the same change ID), and edits made after the landed patchset survive as a new change.

## Ignoring files

ok's jj honors a `.ignore` file in any directory, in `.gitignore` syntax: a new file one names
is not tracked, a directory's `.ignore` applies beneath it, a deeper file's rules win over a
shallower one's, and an `.ignore` written together with the files it names applies in that same
snapshot. A file already tracked stays tracked. jj's own `.gitignore` handling is unchanged, so a
`.gitignore` is honored too; where the two name the same path, `.gitignore` wins (jj consults it
after the `.ignore` rules).

Write ignore rules as `.ignore`. ripgrep and fd — and the editors and agents that search with them — read `.ignore` in
any directory, but `.gitignore` only inside a git repository, which an ok checkout is not (VS
Code's search is the exception: it reads both anywhere).

How it works (`workingcopy/`): ok's working copy is jj's local working copy, wrapped. Before
each snapshot it walks the working copy for `.ignore` files, in parallel, reading exactly the
directories jj's snapshot reads — skipping `.jj`, `.git`, nested repositories, and every
directory the rules ignore, unless `jj file track` forces it in — and hands them to jj as the
snapshot's base ignores. On a repository of 65,000 files the walk takes about 9 ms. A workspace records its
working copy as `ok`; one recorded as jj's `local` (a clone made before ok's jj had its own)
loads as ok's, since the state on disk is the same. `jj debug local-working-copy` and `jj debug
watchman` need jj's own working copy, and refuse an ok workspace.

## Virtual workspaces

```bash
jj clone app ~/w                           # mounted at once; files read from ok as they are used
jj clone app ~/w2 --attach <workspace>     # take an existing workspace over here, at its latest checkpoint
jj fs status                               # the daemon, each mount, its lease, what it fetched
jj fs checkpoints                          # this workspace's checkpoints, newest first
jj fs restore <id>                         # the working copy as a checkpoint had it (jj undo undoes it)
jj fs prefetch [paths…]                    # read files into the cache ahead of time
jj fs watch                                # print what changes as it changes, checkouts included
jj fs attach                               # take this workspace back from the machine holding it
jj fs forget                               # unmount it and remove its state here (it stays on ok)
```

The daemon also **presents** commits for ok's workers and deployer — a commit's tree mounted
read-only, moved to another commit at once (`Present`; `the roles' checkout`'s `Virtual`, okfs.md
"ok's services"); `jj fs status` lists presentations too.

The daemon is this binary (`jj fs daemon`), one per user, run as the systemd user service
`okfs.service` that the first virtual clone installs and enables (where there is no user
manager, a detached process logging to `~/.local/share/ok/daemon.log`). A jj command that finds
a daemon of another build restarts it; systemd keeps every mount's connection across the restart,
so open files read on. Its state: `~/.local/share/ok/` — `workspaces/<id>/` (`repo/` is the
workspace's own jj repository — stores, outbox, operation heads, index; `jj/` is the workspace's
`.jj`, which the mount shows as a symlink; `overlay/` written files; `inodes.snap`/`inodes.log`),
`workspaces.toml` (what to mount), `machine` (this machine's ID, for leases and checkpoints) — and
the control socket in `$XDG_RUNTIME_DIR/okfs/`. `OKFS_STATE_DIR`, `OKFS_RUNTIME_DIR`, and
`OKFS_DAEMON` (`spawn`: no systemd; `external`: only connect) override them, which the tests use.

A repository lists in `.ok/redirections` the paths (one a line) a virtual workspace presents as
symlinks to directories on local disk — heavy ignored outputs like `node_modules/` — which are
never tracked.

Inside, it is ordinary jj, with three differences: `jj st` and every snapshot read only what was
written (not every file); a checkout swaps the tree without writing files, so it reports no file
counts; and the working copy's type is `okfs`. How it works, and what is measured, is
`okfs.md`.

## Layout

The Cargo workspace at the repository's root; every crate is under `jj/`.

| Crate | What it is |
|---|---|
| `jj/cli` (`ok-jj`, the `jj` binary) | `main.rs` (`CliRunner` with ok's stores, working copy, commands, and configuration), `auth.rs` (`jj auth`), `commands.rs` (`jj clone`, `jj create`), `changelists.rs` (`jj cl`, `jj cl get`, `jj sync`), `lifecycle.rs` (`jj cl status` and the rest of a CL's lifecycle), and `fs.rs` (`jj fs`) |
| `jj/workingcopy` (`ok-working-copy`) | jj's local working copy honoring `.ignore` files, and the workspace loader that gives every ok workspace it |
| `jj/cl` (`ok-cl`) | The ChangeList workflow over jj-lib — what `jj cl` refuses before asking the SCM, the upload (publishing first), `jj cl get`, and `jj sync` — so tests drive it without the command line |
| `jj/store` (`ok-store`) | jj's `Backend`, `OpStore`, and `OpHeadsStore` over the SCM, with the cache, the outbox, publishing, prefetch, and laying out `.jj/repo` for a clone; `changelists` holds the SCM calls behind the CL commands |
| `jj/review` (`ok-review`) | The client of ok control's ChangeLists service, and how `jj cl status` shows a CL (`render.rs`) |
| `jj/encoding` (`ok-encoding`) | The canonical object encoding over jj-lib's own types — encode, decode, BLAKE3-256 IDs, a streaming hasher, and commit signed data |
| `jj/proto` (`ok-proto`) | ok's protocols, compiled from `proto/ok` with protox (no `protoc`): prost messages and tonic clients |
| `jj/credential` (`ok-credential`) | The machine's credential: the device login, the files `ok auth login` writes, renewal, and the proof every request carries |
| `jj/okfs` (`okfs`) | Virtual workspaces: the model (`model.rs`), its log (`persist.rs`), the FUSE operations (`fs.rs`), the snapshot walk (`snapshot.rs`), the daemon (`daemon.rs`) and finding or starting it (`client.rs`), the control socket (`control.rs`), systemd's protocol (`systemd.rs`), and the `okfs` working copy (`workingcopy.rs`) |
| `jj/okfs/fuse` (`okfs-fuse`) | The FUSE kernel protocol over `/dev/fuse`, mounting (directly, or through `fusermount3`), a multithreaded session, notifications, and `testns` (a process in a user and mount namespace of its own, for tests) |
| `jj/testscm` (`ok-testscm`) | An `ok scm` for the end-to-end tests (see *Tests*) |

## The stores on disk

Everything lives under `.jj/repo/store`, shared by the repository's workspaces on the machine:

| Path | What it holds |
|---|---|
| `ok.toml` | which SCM repository this is: the endpoint, the repository's id, its name at clone time, and — for a loopback development SCM only — the principal the clone acts as |
| (the cache) | not here: the machine's, shared by every clone and virtual workspace of the repository — `~/.cache/ok/<host>/<repository id>` (`$XDG_CACHE_HOME`, `$OK_CACHE_DIR`). Objects, files, chunks, manifests, and trees' sizes in segments with sorted index runs (`store/segments.rs`), each verified on arrival and on read, kept as ok sends them, compressed, and retired a segment at a time past 8 GiB — never an entry an outbox holds or references (`scm.md`, "Cache, outbox, and prefetch") |
| `outbox/` | everything written and not yet acknowledged by the SCM, fsynced before the write returns; the next `update_op_heads` publishes all of it, whatever command staged it |
| `quarantine/` | an operation the SCM can never accept (its parent operation was collected), moved aside with an error so it blocks nothing |

A command that dies before its publish leaves its transaction in the outbox and nothing on the
SCM; the next command publishes it along with its own.

## What it asks ok

Every connection to ok's services records each RPC by method — calls, failures (a status not
OK, or a broken connection or stream), time in all and the longest, and bytes each way — and every batched fetch by kind
(objects, commits, operations, files, manifests, chunks); each repository it opens records the
reads that waited on ok (`store/calls.rs`). Two ways to see them:

- **`jj fs status`** shows the okfs daemon's since it started, and each workspace's and
  presentation's reads that waited.
- **`OK_STATS=<file>`** has any `jj` command write its own as JSON when it exits:

  ```bash
  OK_STATS=/tmp/clone.json jj clone app ~/w
  ```

They are the client's side of what `ok scm` reports in its Prometheus metrics.

## Tests

```bash
cargo test --workspace
```

The unit tests need nothing. The end-to-end suites — `jj/store/tests/e2e.rs`, `jj/cl/tests/e2e.rs`,
and `jj/cli/tests/cli.rs` — run a real `ok scm` (`jj/testscm`: `ok scm serve -dev`, started once
per test process on a loopback port, the `ok` binary from `$OK_BINARY` or the `PATH`) over
PostgreSQL and S3, which the environment names; without them they are skipped, and with
`OK_TEST_INTEGRATION=require` they fail instead. A caller there names its principal (a development
SCM trusts it on loopback): a clone's recorded principal, and the harness's clients act as anyone
— alice, bob, or ok control. Each test makes its own repository. The tests that mount need
`/dev/fuse` and user namespaces. [building.md](building.md) has the environment.

- **The stores** (`store/tests/e2e.rs`): one workspace on two machines sharing its operation log,
  a person's two workspaces isolated from each other, atomic and interrupted publishes,
  divergence, operation prefixes, quarantine, signing, and copy records.
- **ChangeLists** (`cl/tests/e2e.rs`): uploading a stack and a patchset, refusing an upload to
  someone else's CL, stacking on someone else's CL and following it through a new patchset and
  its landing, landing by fast-forward and by splice, a path conflict refused until `jj sync` and a
  resolution, a moved head refused and re-validated, a stack landing bottom-up with a dry run that
  agrees, and `jj sync` retiring landed changes without divergence while keeping later edits.
- **The binary** (`cli/tests/cli.rs`): as two people — `jj create`, `jj clone`, `jj cl`, `jj cl
  get`, stacking, and `jj sync` after landings; files up to 50 MiB; and ignore files. Each runs
  twice: real (`--real`), and virtual — in a namespace of its own, each person running a daemon
  (`OKFS_DAEMON=external`). And what only virtual workspaces do: checkpoints with no jj command, a
  takeover by a second machine, a crash and a restart, `jj fs checkpoints` and `restore`, `jj fs
  forget` and attaching again, working offline behind a proxy the test cuts, `jj fs watch`, and
  redirections. `cli/tests/nofuse.rs` checks a virtual clone is refused where `/dev/fuse` does not
  exist. Both run on Linux; elsewhere the workspace builds, and its unit tests run.

A plaintext `http://` endpoint is accepted only on a loopback address — where the client names its
principal itself, a clone's recorded principal, else `OK_DEV_PRINCIPAL` — or with `OK_PLAINTEXT=1`,
on a network the installation trusts, where it presents the machine's credential as over TLS.

## Dependencies

jj is pinned **exactly** (`=0.45.1`): the encoding mirrors that release's stored structs field for
field, so upgrading jj means re-checking those structs against [scm.md](scm.md) ("Objects and
identity") and allocating a new kind for any changed shape. Default features are off — no `git`,
no `watchman`. TLS is rustls with the aws-lc-rs provider and `prefer-post-quantum`, verifying ok's
certificate against the system's trust store.

## The encoding contract

The Go server (`internal/scm/encoding`) and this crate implement one encoding, held together
by the golden vectors in `internal/scm/encoding/testdata/vectors.json`. The Rust test compiles
them in from there. Any change to the encoding changes the vectors
first, and both sides' tests must pass against them:

```bash
cargo test -p ok-encoding
go test ./internal/scm/encoding
```

Decode errors carry the codes the vectors name (`Code::as_str`), and the decoder reports the
first violation in encoding order, exactly as the Go decoder does. Two codes exist only when
encoding in Rust, where jj's IDs are unsized byte vectors: `id-length` and `signed-data`.
