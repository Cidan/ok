# ok

ok is source control and code review built on [jj](https://github.com/jj-vcs/jj): a jj-native
server that stores repositories and their ChangeLists, Critique-style review with OWNERS, pre-submit
and post-submit automation with a landing queue that keeps `main` green, and deploys after landing
— every job a [Bazel](https://bazel.build) command.

## Status

ok is being built in the open from a working system, part by part. What runs today:

- **ok identity**: the directory of people and robots, synced from your system of record, signing
  in, and the credentials every other part of ok checks. See [docs/identity.md](docs/identity.md).
- **The SCM**: repositories, their objects and operation logs, `main`, and ChangeLists — upload,
  path-level landing, rollbacks, the change feed, browsing, and garbage collection — with file
  contents in an S3 bucket and everything else in PostgreSQL. See [docs/scm.md](docs/scm.md).
- **ok's jj**: upstream jj with ok's stores and commands — `jj auth login`, `jj clone`, `jj cl`,
  `jj sync` — and **okfs**, virtual workspaces: a clone ready at once, files read from ok as they
  are used, every write kept in ok within a second. See [docs/jj.md](docs/jj.md) and
  [docs/okfs.md](docs/okfs.md).
- **ok control**: the web UI and review — the repository browser, ChangeLists with LGTM and
  Approval over OWNERS and the groups-and-ACLs tree, inline comments that follow the code across
  patchsets, and submit. See [docs/changelists.md](docs/changelists.md),
  [docs/owners.md](docs/owners.md), and [docs/web.md](docs/web.md).
- **ok automation**: pre on every patchset and post on the exact commit that would land, planned
  from what each change affects in the Bazel graph — builds, tests, lint, and the workflow jobs a
  repository declares — and a landing queue that moves `main` only on what passed, speculating
  through conflicts; workers run Bazel on commits checked out through okfs or written to disk. See
  [docs/automation.md](docs/automation.md).
- **The deployer**: after a landing, the migrations the change affects — any program speaking ok's
  migrate protocol, with a PostgreSQL migrator built in — then its images pushed and each affected
  service rolled blue/green in watched stages and waves, rolled back on failure; and ok identity's
  groups-and-ACLs tree applied from the repository that holds it. See [docs/deploy.md](docs/deploy.md).
- **Packaging**: `ok serve` runs the server roles in one process; the `ok` and `ok-runner` images;
  a Helm chart with the deployer's grants and admission policy; release binaries of `ok` and `jj`
  for Linux and macOS. See [docs/install.md](docs/install.md).

Every role reads one configuration file, and is reached at one public address:
[docs/configuration.md](docs/configuration.md).

| Package | What it is |
|---|---|
| `internal/identity` | Names under your domain — `ada.human.example.com`, `ci.robot.example.com`, `eng.group.example.com`, `eng.acl.example.com` — and the groups-and-ACLs tree: sets declared as folders in a repository, whose nesting is ownership, changes to which are approved by the ACL that owns each part. |
| `internal/directory` | The directory and the system of record it follows — Google Workspace, Microsoft Entra ID, LDAP (Active Directory, OpenLDAP, FreeIPA), SCIM provisioning (Okta, Entra, OneLogin, JumpCloud), or a static file — and the sync, which makes, suspends, renames, and deletes people and groups as the source says, and never gives a name to two principals. |
| `internal/id` | The identity role: its store, its CA, its gRPC services, sign-in in a browser (OpenID Connect, LDAP, passkeys), the device login a command line signs in with, and robots signing in with Kubernetes and CI tokens. |
| `internal/scm` | The SCM role: the canonical encoding of jj's objects, packs, the content index, the store (publish, reads, ChangeLists, landing, rollbacks, workspaces, GC, fsck), and its gRPC services. |
| `internal/idclient`, `internal/pgdb`, `internal/blob` | What every role but identity shares: its callers resolved through identity and its own credential; its tables in a PostgreSQL schema of its own; S3-compatible object storage. |
| `internal/serve` | The serving runtime every role runs on: one port for gRPC, REST, and web pages; each request's caller resolved and held to its service's declared callers; Prometheus metrics. |
| `internal/owners` | OWNERS files: Chromium/Gerrit syntax, resolved nearest-only, validated, and checked so no change is left unapprovable. |
| `internal/workflow` | Workflow files (`.ok/workflow/*.yaml`): the jobs a directory's changes run before and after submit, on Bazel targets, and its deploys; and a repository's settings (`.ok/repository.yaml`). |
| `internal/buildsys/bazel` | What a change affects in a Bazel workspace, found by comparing every target's hash at the change and its base — edits to `.bzl` files, macros, and attributes included — and the Bazel commands jobs run. |
| `cmd/ok` | The `ok` command: the roles' servers, signing in, administering, and checks. |
| `internal/control` | ok control: its database, review (approval over OWNERS and the groups-and-ACLs tree, reviewer suggestions, threads ported across patchsets, submit), the ChangeLists service, the change-feed follower, and the web site; `internal/control/pages` are its templ pages, over the shell every page shares (`internal/web`). |
| `internal/automation` | ok automation: its store (runs, plans, logs, submissions), the Runs, Pushes, and Work services, the change-feed follower, the planner, the landing queue, and the worker; `internal/checkout` checks a commit out for a worker, through okfs or to disk. |
| `internal/deploy` | The deployer: its store, the planner (which deploys a move of `main` makes due, in waves), the engine (pushes, migrations, the tree, rolls), the rolls and their watch, the observer of what the cluster runs, the Deployments service, and a simulated cluster for its tests. |
| `migrate`, `migrate/postgres` | ok's migrate protocol, for a repository's migration programs, and a migrator of ordered PostgreSQL files. |
| `jj/` | The Cargo workspace of ok's jj: the `jj` binary, its stores over the SCM, the ChangeList workflow, the credential, okfs and its FUSE layer, and the protocols compiled from `proto/`. |

## Contributing

This repository is published from where ok is developed, one commit per change landed there. It
does not take pull requests.

## Installing

The Helm chart, on Kubernetes: [docs/install.md](docs/install.md). Each release has `ok` and `jj`
for Linux and macOS (amd64 and arm64), the images `ghcr.io/cidan/ok` and `ghcr.io/cidan/ok-runner`,
and the chart at `oci://ghcr.io/cidan/charts/ok`. Virtual workspaces are Linux's; on macOS, `jj`
makes real checkouts.

## Building

```bash
go build ./cmd/ok
cargo build --release -p ok-jj     # target/release/jj
go test ./... && cargo test --workspace
```

Tests that need PostgreSQL and S3 skip without them; [docs/building.md](docs/building.md) has how
to run them, and how to regenerate the generated files.

## Using it

```bash
ok identity ca init -dir /etc/ok/ca -name "Example"     # once: the identity CA
ok serve -config ok.yaml                                 # identity, the SCM, automation, and control in one process
ok config check -config ok.yaml                          # read a configuration as the roles do
ok identity serve -config ok.yaml                        # the identity role
ok scm serve -config ok.yaml                             # the SCM role
ok control serve -config ok.yaml                         # ok control: the web UI and review
ok automation serve -config ok.yaml                      # ok automation: pre, post, the landing queue
ok worker run -config ok.yaml                            # a worker: Bazel on each run's commit
ok deploy serve -config ok.yaml                          # the deployer: migrations, rolls, the tree
ok scm fsck REPOSITORY                                   # verify a repository
ok auth login -server https://ok.example.com             # sign this machine in
ok admin status                                          # the sync from the system of record
ok admin robot create ci                                 # a robot: ci.robot.example.com

ok identity check -domain example.com identity/          # a groups-and-ACLs tree
ok owners check -domain example.com .                    # every OWNERS file in a repository
ok workflow check .                                      # every workflow file, and the settings
git diff --name-only main | ok affected -base ../main -change .   # what a change affects

jj auth login https://ok.example.com                     # sign this machine in, for jj
jj clone app ~/src/app                                   # a virtual workspace
jj new 'trunk()' -m 'add rate limiter' && jj cl          # upload a change as a ChangeList
jj sync                                                  # follow main
```

## Repository settings

`.ok/repository.yaml` says how ok checks a repository:

```yaml
build: bazel                  # or none: brought in as it was; changes pass review alone
bazel:
  flags: [--config=ci]
  lint:
    aspects: ["//tools/lint:linters.bzl%golangci_lint"]
    format: //tools/format
identity: identity            # the groups-and-ACLs tree, in identity.tree_repository
```

## License

MIT; see [LICENSE](LICENSE).
