# okfs — virtual workspaces for ok's jj

okfs makes a jj workspace on ok a virtual file system: a clone is ready at once, whatever the
repository's size, a file's contents come from ok the first time something reads it, every write
is kept in ok as a checkpoint within a second, and a workspace can be attached on any machine. It
is modelled on Google's CitC. On Linux the kernel reaches it over FUSE; on macOS, over NFS
([macOS](#macos)).

Related: [jj.md](jj.md) (ok's jj: its stores, cache, outbox, and working copy), [scm.md](scm.md)
(the SCM: objects, the client, the protocol), [identity.md](identity.md) (the credential jj
presents).

## Why

A real checkout makes `jj clone` stream the whole operation log, then every file of the
working-copy tree, and write them all to disk before the first command can run; every new
workspace pays that again, and so does every fresh pod of a service that checks code out. Most
work touches a sliver of the tree; nobody should wait for the rest. And a workspace that lives only
on one disk is lost with it.

## What CitC is, and what okfs takes from it

CitC ("Clients in the Cloud") is how most of Google works in Piper: a cloud storage backend and
a Linux-only FUSE file system. A workspace is a directory showing the whole repository at the
point it was synced to, with the developer's changes overlaid on it; only the files they change
are stored in the workspace (the average workspace holds fewer than ten), every write is kept
automatically and can be rolled back, and the workspace can be reached from any machine.
*(Potvin and Levenberg, "Why Google Stores Billions of Lines of Code in a Single Repository",
CACM 59(7), 2016.)* jj was built with this in mind: its storage is behind traits so it could live
"in the cloud at Google", and its working copy is one of them.

okfs takes all of it:

| CitC | okfs |
|---|---|
| a workspace at a sync point of Piper's head | a jj workspace whose working-copy commit sits on a commit of `main` (or any commit) |
| the repository presented whole, fetched on demand | the working-copy commit's tree presented whole; each file's contents read from ok on first access |
| only changed files stored in the workspace | an overlay holds only what was written; everything else stays a reference into ok |
| every write saved in the cloud, can be rolled back | every write is in a **checkpoint** in ok within about a second; any checkpoint can be restored ([Checkpoints](#checkpoints)) |
| reachable from any machine | a workspace **attaches** on any machine at its latest checkpoint, instantly ([Attaching](#attaching-a-workspace-on-another-machine)) |

## The user's view

`jj` is the only tool. A workspace is virtual unless asked otherwise:

```bash
jj clone app ~/app                       # a virtual workspace: mounted at once, contents on demand
jj clone app ~/app-real --real          # a real checkout, every file written to disk
jj create hello                         # virtual; --real for a real one
jj clone app ~/w --attach laptop-app     # attach an existing workspace here, at its latest checkpoint
jj fs checkpoints                       # this workspace's checkpoints, newest first
jj fs restore 4182                      # the working copy as it was at checkpoint 4182
jj fs status                            # the daemon, its mounts, what it fetched, its calls to ok, and how long reads waited
jj fs prefetch 'glob:apps/ok/**'      # read files into the cache ahead of time
jj fs forget                            # unmount this workspace and remove its local state (it stays on ok)
```

Inside, everything is jj as it is today: `jj st`, `jj new`, `jj edit`, `jj cl`, `jj sync`,
conflicts, sparse patterns, and every tool that reads and writes files. The two kinds behave
the same; what differs is how long things take:

| | real | virtual |
|---|---|---|
| clone | the operation log, then every file, written to disk | the operation log; nothing else until it is read |
| `jj st`, and the snapshot every command takes | stats every file in the tree | reads only the paths written since the last snapshot |
| `jj new`, `jj edit` to a far commit | rewrites every file that differs | swaps the tree; files are read when next opened |
| reading a file the first time | local disk | one round trip to ok (chunk by chunk for large files), then local |
| a write | on local disk only, until the next jj command | on local disk, and in ok within about a second |
| the machine is lost | changes since the last jj command are lost | about a second of changes is lost |
| offline | everything works | what has been read before works; reading something never read fails; writes are kept and published when back |

## Architecture

```
  jj (CLI, per command)                    okfs daemon (one per user, the jj binary)
  ┌──────────────────────────┐            ┌──────────────────────────────────────────┐
  │ okfs working copy      │──control──▶│ mounts: one FUSE connection per workspace │
  │  snapshot / check_out /  │  (protobuf │ inodes ▸ base tree ▸ overlay ▸ journal    │
  │  reset / sparse          │  on a Unix │ checkpointer ▸ attachment lease           │
  │ ok stores (as today)   │  socket)   │ ok's stores, one per workspace ─────────┼──▶ ok scm
  └──────────────────────────┘            └───────────────▲──────────────────────────┘   (Store,
                                                          │ /dev/fuse                     Browse,
                                     any process ── VFS ──┘ kernel FUSE                   Workspaces)
```

### The daemon

One long-lived process per user serves every virtual workspace on the machine, as EdenFS does:
one process shares fetched data across workspaces of the same repository, and there is a single
thing to keep running. It is the `jj` binary itself, started as a hidden subcommand
(`jj fs daemon`), so the daemon and the CLI are always the same code and version. It runs as a
**systemd user service** (`okfs.service`) that the first virtual clone installs and enables:

- `Type=notify`, `Restart=always`;
- `FileDescriptorStoreMax=` and `FileDescriptorStorePreserve=on-success`: every FUSE connection's
  `/dev/fuse` descriptor is kept in systemd's file descriptor store, so the daemon can restart —
  to upgrade, or after a crash — without unmounting anything (*Lifecycle*);
- lingering (`loginctl enable-linger`), offered on the first virtual clone, so mounts outlive the
  login session.

On macOS it is the **launchd agent** `ok.okfs` instead ([macOS](#macos)). A service manager always
starts it; jj never runs a copy of itself. On a machine with neither, run `jj fs daemon` under
whatever supervises its processes: a jj command that finds no daemon says so, as does one that
finds a daemon of another build, which only what started it can replace.

It talks to ok as the user, with the machine's credential (`jj/credential`),
renewing the certificate as the CLI does, and reconnecting when a renewal replaces it.

### The control socket

jj reaches the daemon over a Unix socket, `$XDG_RUNTIME_DIR/okfs/control.sock` (in a `0700`
directory), carrying length-delimited protobuf messages (`okfs.proto`, prost — already
vendored; the vendored tonic is client-only). The requests are what jj needs: mount and unmount a
workspace, the paths changed since a journal position, check out a tree, reset to a tree, set
sparse patterns, list and restore checkpoints, prefetch, and status; and a subscription that
streams the journal for tools. A command run in a virtual workspace whose daemon is not running
starts the service and waits for it.

### Where state lives

| Path | What it holds |
|---|---|
| `~/.local/share/ok/workspaces/<id>/` | one virtual workspace: `repo/` (its own jj repository — `store/` (ok's stores: `ok.toml`, `outbox/`, `quarantine/`), `op_heads/`, `op_store/`, `index/` — the workspace's alone, as its operation log is), `jj/` (its `.jj`: `repo` is a file naming that repository, `working_copy/` its working-copy state), `overlay/` (written files, by inode), `inodes` (the persistent inode table), `journal`, `lease` |
| `~/.cache/ok/<host>/<repository id>/` | the repository's content-addressed cache — objects, operations, and views by their IDs — shared by every workspace of the repository on the machine |
| `~/.local/share/ok/workspaces.toml` | every virtual workspace: its mount point, id, repository, and workspace name |
| `~/.local/share/ok/machine` | this machine's id: random, made once, named in leases and checkpoints |
| `$XDG_RUNTIME_DIR/okfs/` (`/tmp/okfs-<uid>/` without one, as on macOS) | the control socket |
| `~/.local/share/ok/nfs.toml` | macOS: the NFS server's port and handle key |
| `~/.local/share/ok/workspaces/<id>/nfs-mount.toml` | macOS: what the workspace's mount was mounted from, and the export its handles name |

A real checkout keeps today's layout (`.jj/repo` inside the workspace). A virtual workspace's
repository is its own, in its state directory, and goes when the workspace is forgotten: every
workspace is isolated from every other (scm.md, "Decisions", 3). Only the cache is
shared, and only because it holds content-addressed data, never state: a second clone of the
large repository fetches nothing the first already read.

The inode table is a snapshot (`inodes.snap`) and the records logged since (`inodes.log`).
Compacting it writes the new snapshot to a temporary file, syncs it, renames it into place, and
syncs the directory — the snapshot durable, name and all — before it empties the log; the
snapshot's first record marks the log it was compacted from, by its length and BLAKE3 hash. So a
power loss mid-compaction leaves the old snapshot and its whole log, or the new snapshot and a
log that was emptied or still starts with exactly the marked bytes, which loading skips: no
record is lost, and none is replayed over a snapshot that holds it. Loading rewrites the log only
to drop such bytes or a torn last record, and then as compaction writes the snapshot.

### `.jj` inside the mount

jj finds a workspace by its `.jj` directory, so the mount presents one: `<mount>/.jj` is a
symlink to the workspace's `jj/` directory on disk (jj follows it). That directory is small — the
working copy's state and the absolute path of the workspace's repository — and everything large
(the index, the store, the cache) is reached through that path, outside the mount. `.jj` cannot be
removed or replaced through the mount.

## The file system

### Inodes

Every path the kernel has looked up has an inode. An inode is one of two kinds, as in EdenFS:

- **Virtual**: a reference into a tree — a directory's tree, or a file's content ID, executable
  bit, and size. Its contents are ok's, immutable, and read on demand. A virtual directory or
  file can sit at a new place in the tree (a rename) without being copied.
- **Materialized**: its contents live in the overlay because something wrote it. A directory is
  materialized when its entries change (a file added, removed, or renamed); a materialized
  inode's parent is always materialized.

Inode numbers are persistent (`inodes.snap` and `inodes.log`: every change to an inode logged as
it is made, the whole compacted into the snapshot), so tools that compare `st_ino` across runs see
a stable file system, and the daemon survives restarts with the kernel's references intact. In
the model (`jj/okfs/model.rs`) a virtual inode is *derived* (its parent directory's
source at its name) or *fixed* (a tree value placed somewhere new — a rename, a chmod — still
ok's); a materialized directory is an *overlay directory*: its slots (written or moved children,
and whiteouts of deleted names) over its source (the base at its path, a moved tree, or nothing).

### Metadata

`getattr` needs a file's size, which jj's tree objects do not carry. The daemon gets sizes
without reading contents, a directory at a time:

- from each tree's **sizes record** (`scm.md`, "Tree sizes"): every file's size in a
  directory, which the daemon learns when it loads the directory's tree, so a `stat` asks ok
  nothing. A virtual clone's tree closure brings every directory's record with its trees; a tree
  loaded later brings its record in a batched `Store.ReadTreeSizes`. Records are cached on disk
  with their trees, in the machine's cache (`scm.md`, "Cache, outbox, and prefetch");
- from the file manifests already in the cache or the outbox (every file read or written
  locally);
- for what neither answers — a tree no commit has published yet — from the manifests-only form of
  `Store.ReadFiles` (`ReadFilesRequest.manifests_only`: each file's size and chunk list, no
  contents), batched, and cached as the file's manifest for later ranged reads.

Directory listings come from tree objects through ok's stores (cached, batched as they are
today), with `READDIRPLUS` so a listing returns its entries' attributes in one round trip. After a
mount the daemon prefetches every tree under the root in the background (`Store.ReadTreeClosure`
— trees only, no contents), so `ls -R` and globbing are local soon after a clone. A presentation
(*ok's roles*) instead reads the commit's checkout before it mounts or moves ([scm.md](scm.md),
"Checkouts"): every tree with its sizes and the files planning reads, in one request — after the
first, the one layer of what the commit changed — so its listings, its stats, and planning's reads
are local at once.

Times: a virtual file's `mtime` is when its content last changed *in this workspace* — the
checkout that brought it, or the write that changed it — so make-style tools see an edit or a
`jj edit` as a change, and an untouched file's time never moves. The kernel caches a virtual
inode's attributes and entries for a long time (they cannot change without the daemon
invalidating them) and its pages across opens (`FOPEN_KEEP_CACHE`): after the first read, a file
is served from the page cache as on any local disk.

### Reads

A read of a virtual file is served from ok's cache, by chunk: the manifest names each chunk
and its size, so the daemon reads only the chunks a request covers, and fetches only the missing
ones — with a ranged `ReadFiles` (`FileRange.offset`/`length`) — so reading the head of a 50 MiB
file does not fetch 50 MiB; the kernel's own read-ahead asks for the chunks after a sequential
reader. Chunks arrive as ok stores them, zstd-compressed where that shrank them
(`ReadFilesRequest.stored`, scm.md, "Reads"), and are decompressed and verified before
they are cached. Every fetch goes through the stores' batchers, so concurrent reads share one
request, and a read of something a request is already fetching waits for that request instead of
asking again: a small file (≤ 256 KiB) whose contents are not local is fetched whole, with any other
files being read at the moment — whether or not its size is known, as a presentation's always is —
and a larger file's missing chunks go out with the other reads' missing chunks. When three files of
one directory are first opened within two seconds — a build reading a package — the daemon
prefetches the directory's other files of up to 1 MiB in the background (their manifests in one
request, then their contents in another), and a read of one of them waits for that rather than
fetching it again. A fetch that ok does not answer in 30 seconds fails the read with `EIO`.

### Writes

The first operation that changes a virtual inode materializes it: open for write, truncate,
`setattr` of size or mode, create, `mkdir`, unlink, rename, symlink, link. Materializing a file
copies its contents into the overlay — a reflink (`FICLONE`) where the file system supports it
(btrfs, XFS), a copy otherwise — and marks its ancestors materialized.
Deletions are recorded as whiteouts in the materialized parent. Writes reach the daemon as they
are made, each written to the overlay file before the reply; `fsync` flushes the overlay file and
the inode log to disk. (The kernel's writeback cache, `FUSE_WRITEBACK_CACHE`, which would coalesce
small writes, is not used: with it the kernel, not the daemon, is the authority on a cached file's
size and times, and what it would save has not been measured to matter.) A snapshot or checkpoint
reads written files from the overlay directly, which every write has already reached.

Everything a file system is asked to hold works in the overlay, tracked or not: hard links
between written files, FIFOs and sockets (language servers create them), extended attributes and
POSIX ACLs (below), `flock` and POSIX locks (kept by the kernel). A snapshot records what jj
records — files, executables, and symlinks — and skips the rest, as jj's local working copy does.

POSIX ACLs (`system.posix_acl_access`, `system.posix_acl_default`) are kept as a disk file system
keeps them (`acl.rs`, after Linux's `fs/posix_acl.c`), and over FUSE the kernel enforces them
(`FUSE_POSIX_ACL`): setting an access ACL sets the mode, and one the mode can say is not kept; a
chmod changes the access ACL; only a directory takes a default ACL, and what is made under one
inherits it, with the umask not applied (the kernel leaves the umask to okfs, `FUSE_DONT_MASK`,
which applies it where there is no default ACL; over NFS the client has applied it). An ACL kept
verbatim by an earlier build, as any other xattr, is brought in step with its inode's mode when the
model loads. A connection an earlier build negotiated, which the daemon adopts across a restart,
has the kernel neither enforce ACLs nor leave the umask to okfs until the workspace is mounted
again; the ACLs themselves are kept the same way either way.

### Secrets

In a repository that keeps secrets ([secrets.md](secrets.md)), a file under `secret/` that is
neither an `acl.yaml` nor an OWNERS file is a secret: the tree and the overlay hold its pointer, and
over FUSE the mount serves its value instead.

- **Reading.** A read-only open fetches the value from ok secrets (`ReadSecret`), using one of the
  person's unseals, checks it against the pointer, and holds it for that open alone: its release
  drops it. With no unseal the open fails with `EACCES`, and the daemon's log says what to run. A
  file holding no pointer — a conflict's markers — reads as it is.
- **Writing.** An open to write asks ok secrets whether the person writes the folder (`EACCES` if
  not); the value is then held in the daemon's memory, never the overlay, until the next snapshot
  or checkpoint seals it: `WriteSecret` keeps it as a new version, and its pointer is written to
  the overlay (`SealSecret` on the control socket, for jj's snapshot). A value equal to the one its
  pointer names stores nothing; a value that is itself a pointer sets the pointer.
- **The kernel.** A secret is `0600`, its size what an open holds (else 0), its attributes never
  cached, and its reads and writes bypass the page cache (`FOPEN_DIRECT_IO`). A value is bound to
  its path: renames and links into, out of, or within `secret/` are `EXDEV`, which `mv` meets by
  copying; symlinks, FIFOs, and sockets there `EPERM`; names that are not UTF-8 `EINVAL`.
- **Whether the repository keeps secrets** is ok secrets' answer for `main`, or the workspace's own
  `.ok/repository.yaml` when it has a `secrets:` stanza, so values stay off disk from the moment a
  workspace starts keeping them.

Over NFS (macOS), whose client caches file data so no read can be made the only one, and in a
presentation, a secret's file is its pointer: `jj secret cat` reads it.

### The journal

Every operation that changes the tree records its path in the workspace's journal, with a
sequence number; a checkout records an event of its own. The journal is in the daemon's memory —
the change count the checkpointer compares, and the stream `Subscribe` sends tools (*Tools*) — and
is not kept across restarts: what changed is in the overlay itself, which a snapshot and a
checkpoint walk.

## jj's working copy

okfs is a jj working copy, `okfs`, registered beside ok's local one
(`jj/workingcopy`) and implementing jj-lib's `WorkingCopy` and `LockedWorkingCopy`. Its
state on disk (`.jj/working_copy/okfs.toml`) is the workspace's id and name, the tree and
operation it was last updated to, the commit it was checked out at, and the sparse patterns.

- **snapshot.** Walk the file system over the control socket (`ListDir`), descending only into
  directories that differ from the recorded tree — a directory the overlay never touched, whose
  source is the recorded tree's own, is the same and is skipped — and compare every name of the
  directory's source, its overlay, and the recorded tree: a moved value is taken as it is, a
  written file is hashed from the overlay (or its content ID taken, when the daemon remembers it
  for that version of the file), and new files are tracked by the `.ignore` and `.gitignore` rules
  of their directories, `snapshot.auto-track`, and the size limit. Its cost is the size of the
  change, not of the tree. The same walk (`jj/okfs/snapshot.rs`) computes a checkpoint's
  tree in the daemon; every snapshot whose tree changed is also recorded as a checkpoint. A file
  materialized with conflict markers is parsed back as jj's local working copy does.
- **check_out(commit).** jj sends the daemon the old (recorded) and new trees, and the new tree's
  conflicts that differ from the old, materialized with conflict markers (in the user's
  configured style) as the local working copy writes them. It counts no files: nothing is
  written. The daemon reconciles the overlay with the two trees — a tracked written file the
  change touches is dropped, so the new tree's version shows; an untracked one stays, and is
  reported where the new tree puts something of its own, as jj does; a directory left empty goes —
  and points the workspace at the new tree. It tells the kernel what changed only in the
  directories the kernel has looked up, reading only those whose old and new trees differ
  (`FUSE_NOTIFY_INVAL_ENTRY` per changed name, `FUSE_NOTIFY_INVAL_INODE` per changed directory and
  file); a looked-up file whose content changed keeps its inode number with the checkout's time as
  its `mtime`. Its cost is what the workspace holds — its overlay and what the kernel has looked
  at — not the size of the change: a fresh clone of the large repository checks out reading one tree. All
  trees it needs are gathered first, together, so nothing changes while one is missing.
- **reset(commit)**, **recover(commit).** Point the workspace at the commit's tree without touching
  the overlay: what differs from it is then a change.
- **Sparse patterns.** Paths outside the patterns are not shown, which keeps `jj sparse`
  meaningful in a virtual workspace; with on-demand reads it is rarely needed.

`jj clone` and `jj create` make a virtual workspace unless given `--real` (decided: virtual by
default everywhere): they make the workspace on ok, make its own jj repository in the
workspace's state directory, record the workspace in its operation log (`add workspace`, as jj's
own initialization does, so a clone interrupted after it is an ordinary stale working copy),
register it with the daemon, which mounts it, lay out its `.jj` in the state directory (pointing
at that repository), and check out as today — except that the tree is not prefetched, since
nothing is written. `jj workspace add` is refused, and so is okfs's factory for the workspace it
would add: a workspace added to a repository would share its operation log. So is `jj workspace
rename`: a workspace's views name it as its operation log is keyed. `jj fs forget`
unmounts a virtual workspace and removes its state on this machine — its repository with it —
after the daemon has put its latest state in a checkpoint. It records nothing in the workspace's
operation log and needs no snapshot, so a stale working copy is forgotten as readily as any; the
workspace stays on ok, where `jj clone --attach` mounts it again at that checkpoint.

## Checkpoints

Every write is kept in ok within about a second, without touching the operation log.

A **checkpoint** is a record in ok's SCM: *this workspace, on this machine, at this moment, had
this tree.* It is not a commit and not an operation — `jj op log`, `jj undo`, and the commit graph
never see it — so writes can be kept as often as they happen without rewriting `@`, rebasing what
is stacked on it, or contending for jj's locks.

- **When.** The daemon's checkpointer takes one after the workspace's writes have been quiet for
  one second, and at least every ten seconds while they continue. It computes the tree a snapshot
  would make — from the journal, so its cost is the change — publishes the tree's new objects
  through the stores' outbox (`publish_objects`, the path every jj write already takes, verified
  and deduplicated: an edit uploads only its new chunks), and records the checkpoint
  (`Workspaces.RecordCheckpoint`). Every jj snapshot in a virtual workspace that changed the
  tree is recorded as one too. A tree the server already holds is not recorded again: one equal
  to the latest checkpoint's, or — before the workspace has one — to the tree jj recorded (its
  working-copy commit's), so a workspace in which nothing was written has no checkpoint, however
  soon or late its daemon checks it (at every mount: `worth_recording`).
- **What.** Exactly what a snapshot takes: tracked files, and new files the ignore rules and
  `snapshot.auto-track` would track, up to `snapshot.max-new-file-size`. Ignored files — build
  outputs — are not kept, as jj does not keep them.
- **History.** Each checkpoint names its parent, the checkpoint it followed on that machine.
  `jj fs checkpoints` lists them; `jj fs restore <checkpoint>` makes the working copy that
  checkpoint's tree (a snapshot, so the current state is itself a checkpoint first and the restore
  can be undone).
- **Retention.** Every checkpoint for 7 days; then the last of each hour for 30 days; then the
  last of each day for a year; a workspace's latest is always kept. ok scm's garbage collection
  thins them; a checkpoint kept past its first week has its trees pinned, and the rest's trees are
  roots of its mark.
- **Real checkouts** take no checkpoints: the daemon does not serve them, and their changes reach
  ok at each jj command's snapshot, as today. A real workspace can be attached elsewhere as a
  virtual one, from its working-copy commit.

With checkpoints, a virtual workspace's local overlay is a write-back cache of what ok holds:
losing the machine loses about a second of work.

### The SCM's side

A new service, `Workspaces`, in `internal/scm/proto` — every call about the caller's own
workspaces, keyed by (repository, principal, workspace name), as the operation log is:

| RPC | What it does |
|---|---|
| `Attach(repository, workspace, machine, takeover)` | takes the workspace's attachment lease for a machine (*Attaching*); returns the lease (epoch, expiry) and the latest checkpoint |
| `Renew(lease)` | extends the lease; says whether another machine has asked to take over |
| `Release(lease)` | gives the lease up |
| `RecordCheckpoint(lease, parent, tree, base)` | records a checkpoint whose tree's objects ok already holds; it becomes the workspace's latest when the lease is current, and a fork (kept, not latest) when it is not |
| `ListCheckpoints(repository, workspace, before, limit)` | a workspace's checkpoints, newest first, forks marked |
| `ListWorkspaces(repository)` | the caller's workspaces: attachment and latest checkpoint |

Tables (`internal/scm/schema`): `workspace` (the lease: holder machine, epoch, expiry, a
requested takeover, the latest checkpoint) and `checkpoint` (id, workspace, machine, parent,
tree ids — a working copy's tree can be a conflict, so they are a merge's terms, as a commit's
are — base commit, fork, created).

## Attaching a workspace on another machine

`jj clone <repo> <dir> --attach <workspace>` mounts an existing workspace on this machine at its
latest checkpoint (or, for a workspace with none, its working-copy commit's tree): the base is that
tree and the overlay is empty, so it is instant, and everything is read on demand. jj's state comes
from the workspace's own operation log, which a repository made for it on this machine loads from
ok.

One machine writes to a workspace at a time, so two never silently diverge:

- **The lease.** Attaching takes the workspace's lease in ok (`Attach`), renewed every few
  seconds by the daemon; it expires 30 seconds after its last renewal.
- **Taking over.** Attaching a workspace another machine holds asks it to hand over. That machine's
  daemon, at its next renewal, takes a final checkpoint, releases the lease, and stops accepting
  writes to its mount (they fail with `EROFS`, and its log says why); the new machine then attaches
  at that final checkpoint. Nothing is lost.
- **When the holder is offline.** The new machine takes over once the lease expires. The old
  machine keeps working offline; its writes are kept, and when it reconnects its checkpoints are
  recorded as a **fork** — their parent is not the workspace's latest — never discarded. `jj fs
  checkpoints` shows the fork, and `jj fs restore` brings it back, to merge by hand as with any
  jj divergence. The old mount turns read-only once it learns it lost the lease.

## Lifecycle and recovery

- **Starting.** At login (or the first command that needs it) the service starts and mounts every
  workspace in `workspaces.toml` through `fusermount3`, the setuid helper every distribution ships
  with FUSE 3 — no root, no privileged helper. The mount is private to the user (no
  `allow_other`).
- **Restarting.** Each mount's `/dev/fuse` descriptor is in systemd's file descriptor store. A
  restarted daemon receives them back (`LISTEN_FDS`), reloads each workspace's inode table and
  journal from disk, and carries on; the kernel holds new requests until it reads them, so open
  files and running processes see a pause, not an error. It then checkpoints each workspace,
  reading from ok the trees of the written directories it has not looked up yet (a fetch blocks
  on the store's runtime through a channel, so a snapshot — itself run inside an executor — can
  make one). Upgrading ok's jj restarts the service when its
  version differs from the CLI's.
- **Crashing.** The same, except for requests the dead process had read and not answered: the new
  daemon asks the kernel to send them again (`FUSE_NOTIFY_RESEND`), so they are answered rather
  than hung. Every reply follows its journal entry and overlay write, so the on-disk state is
  never behind what a process was told.
- **Rebooting.** Mounts are gone; the service mounts every workspace again from its persistent
  state. Nothing written is lost: the overlay and journal are on disk, and in a checkpoint.
- **Offline.** Reads of what was read before are local. A read that needs ok fails with `EIO`
  — at once when ok refuses the connection, after 30 seconds when it does not answer — and `jj
  fs status` says the daemon is offline and why (a renewal that could not reach ok, a
  checkpoint that failed). Writes always work. A failed checkpoint is tried again after a pause
  that doubles to 30 seconds, and the first renewal that reaches ok again records what was
  written meanwhile; jj's outbox publishes with the next jj command.
- **Stopping.** SIGTERM (what systemd sends) makes the daemon take a last checkpoint and compact
  each workspace's log; under systemd the mounts stay, held in its descriptor store, and a
  started daemon adopts them. (jj-cli's own handler, which exits at once, is replaced in the
  daemon.)

## Tools

- **Build tools.** A build reads its inputs through the mount; Bazel's output base is outside the
  workspace (`~/.cache/bazel`), so build outputs never land in the overlay. A sandbox that binds
  the workspace into a nested user namespace mapping the caller as themselves reads the user's
  mount without `allow_other`: the kernel admits a process to a FUSE mount when its real,
  effective, and saved uid and gid are the mount owner's, compared as kernel ids
  (`fuse_allow_current_process`, `fs/fuse/dir.c`). The extended attribute `user.ok.file_id` gives
  a file's ID in the SCM — for a file written here, once it is recorded; until then it has none —
  so a tool can tell whether two files are the same without reading either.
- **Heavy ignored outputs** (`node_modules/`, `target/`) work in the overlay, slower than a local
  disk. A **redirection** makes them native: a path the repository lists in `.ok/redirections`
  (one repo-relative path a line, `#` starting a comment) is presented in a virtual workspace as a
  symlink to its own directory on local disk, `~/.local/share/ok/workspaces/<id>/redirections/
  <path>` (EdenFS's symlink redirections, which need no privilege). The daemon keeps them as the
  file the workspace shows declares, at every mount and checkout: a missing one is made (its parent
  directories too), one no longer declared is removed (its directory stays until the workspace is
  forgotten), and a path where something else stands is left alone, and logged. A redirection is
  never snapshotted, whether or not an ignore file names it. Real checkouts have no redirections:
  their files are on local disk already.
- **Editors and watchers.** Writes made through the mount raise inotify events as on any file
  system. A `jj edit` or `jj new` that swaps the tree does not: the daemon changes what the kernel
  sees without going through the kernel, and FUSE cannot raise fsnotify events for that. The
  journal is the answer (decided, 2026-10-01) — a subscription on the control socket that streams
  changed paths, for editor integrations and watchers — and what editors miss is measured once it
  runs. `jj fs watch` prints it: `changed <path>` for each path written, created, moved, or
  removed, and `checkout` when a checkout moves the workspace to another tree.
- **Search.** ripgrep or `grep -r` over the whole tree reads the whole tree, as on a real
  checkout; afterwards it is local. ok's code search, not the mount, is the tool for searching
  everything.

## macOS

macOS's kernel has no FUSE, and its replacement, FSKit, loads only file systems that Apple's paid
developer program has signed (`com.apple.developer.fskit.fsmodule` is a restricted entitlement), so
a binary anyone builds could not use it. Every Mac has an NFS client, so there okfs is an **NFSv3
server** on 127.0.0.1 that the kernel mounts — as EdenFS does on macOS. The same model serves both:
`fs.rs` answers FUSE, `nfs.rs` answers NFS, and nothing above them knows which.

- **The protocol is ours** (`jj/okfs/nfs`, okfs-nfs), as the FUSE layer is ([Decisions](#decisions),
  1): ONC RPC over TCP, the MOUNT protocol, and NFSv3's procedures, answered by a pool of threads in
  any order.
- **One server, one port**, for every workspace: the daemon's, on a port kept in `nfs.toml`. A
  workspace is mounted with `mount_nfs` — no root — with `vers=3,tcp,locallocks,intr,nobrowse`:
  locks kept by the kernel itself (as FUSE keeps them; `nolocks` would fail every `flock`), waits
  that a signal interrupts, and no entry in the Finder's sidebar. Spotlight does not index it.
- **Only the user, only the kernel.** The port is open to every local process, and NFS's
  credentials say whatever a caller writes, so handles are what admit: each carries a keyed hash
  (BLAKE3, the key kept in `nfs.toml`), and a process that was never given one cannot make one. The
  first comes from `MNT`, which accepts a one-time path that the daemon's own `mount_nfs` uses up;
  another caller's using it first fails that mount, and the workspace is then served under a new
  export, whose handles no one else holds. A call on a good handle comes from the kernel, which
  says truly whose process made it, and anyone but the user — root included, as FUSE refuses
  others — is refused.
- **No forget.** The kernel may use any handle it was given at any time, across daemon restarts, so
  every inode it was given is kept: until it is removed, or for ten minutes after a checkout leaves
  no name reaching it (a process may still have it open).
- **Telling the kernel of a checkout.** NFS has no notifications. A checkout moves on the times of
  the directories and files it changed that the kernel has, and each is asked for through the
  mount with a chmod to the mode it has, which changes nothing here: the reply carries the new
  times, and the kernel drops what it cached under them and looks again — the moment the jj
  command returns, a changed file reads its new contents, even one an editor holds open. The jj
  command that asked for the checkout makes those calls, not the daemon: macOS keeps network
  volumes, a mount of 127.0.0.1 among them, from background processes.
- **The daemon is a launchd agent**, `ok.okfs` (`~/Library/LaunchAgents/ok.okfs.plist`), in the
  user's domain, so it runs with or without anyone logged in at the screen, kept alive, and as an
  interactive process (programs wait on its answers). launchd holds its port's socket across
  restarts: the kernel's mounts stay, the restarted daemon serves them on — handles and all — and a
  read made meanwhile waits about a second for it. Upgrading ok's jj restarts the agent.
- **What macOS writes beside files** — AppleDouble files holding extended attributes (`._name`;
  NFSv3 has no extended attributes), the Finder's `.DS_Store`, and `.nfs.*`, NFS's name for a file
  removed while still open — is ignored: never tracked or checkpointed, they stay where they are,
  as build outputs do.
- **Permission.** A terminal (or editor) reaching a workspace for the first time may be asked to
  allow access to files on network volumes, as for any NFS mount.

Measured on a Mac (M-series, macOS 26) with 13,880 files from ok across a LAN: a virtual clone in
0.5 s against a real one's 3.1 s; `find` over every file and `grep -r` as fast as in a real
checkout; reading every file again 0.5 s against 0.3 s; `jj st` 30 ms against 100 ms.

## Performance

- The FUSE connection is read by one thread per CPU (`FUSE_DEV_IOC_CLONE` gives each its own
  descriptor), and fetches are asynchronous (tokio, as the stores already are), so a slow fetch
  never holds up a cached read.
- `FUSE_PARALLEL_DIROPS`, `READDIRPLUS`, a 1 MiB `max_read`/`max_write` (`FUSE_MAX_PAGES`), long
  entry and attribute timeouts with explicit invalidation, and `FOPEN_KEEP_CACHE` for virtual
  files.
- **FUSE over io_uring** (Linux 6.14+) cuts the per-request cost further. It is off in the
  kernel unless an administrator enables it (`/sys/module/fuse/parameters/enable_uring`); the daemon uses `/dev/fuse` otherwise, and notifications and
  interrupts use `/dev/fuse` either way.
- **Passthrough** (Linux 6.9+, a file's I/O going straight to a backing file) would make reads of
  fetched files native, but registering a backing file requires `CAP_SYS_ADMIN`; an unprivileged
  per-user daemon cannot use it. Not planned.
- Targets, measured on the large repository: a virtual clone in seconds; `jj st` in a clean workspace
  under 50 ms; a snapshot proportional to the change; a warm build within 10% of a real
  checkout's.

## Security

- The daemon runs as the user, holds only what jj holds (the device credential, used the same
  way), and its mounts admit only the user's own processes.
- `~/.local/share/ok/` and the runtime directory are `0700`; the overlay is the user's data,
  like a real checkout's files.
- Every byte read from ok is verified against its content ID as today: a chunk on arrival
  against its ID, a file's chunks against the file's ID.
- Checkpoints and leases are their owner's: ok scm admits only the principal they belong to.
  Their trees are ordinary objects, readable as every published object is.
- No privilege anywhere: mounting is `fusermount3` (on macOS, `mount_nfs`), and nothing needs
  `CAP_SYS_ADMIN`.
- On macOS, NFS handles are unforgeable and calls from other users refused ([macOS](#macos)).
- A secret's value is never written to disk: the overlay holds its pointer, and the value is in
  the daemon's memory only while an open reads it or until a snapshot seals what was written
  ([Secrets](#secrets)).

## ok's roles

ok's workers and its deployer check out each commit they work on through okfs, as a
**presentation**: the commit's tree mounted read-only at the role's checkout directory, moved to
another commit at once, each file read from ok the first time something reads it. A fresh pod
plans a landing without writing the whole tree first, and a worker's job reads what it builds and
nothing else.

- **The daemon.** The role runs the daemon a workstation runs — `jj fs daemon`, the jj binary in
  its image — as a child process, its state (the shared stores, their caches) and its control
  socket under the role's state directory, never in the checkout. Jobs run in a sandbox that
  reaches neither the socket nor the caches, which hold other repositories' objects.
- **Its identity.** The daemon reaches the SCM as the role's robot, with the role's own
  credential (`OK_CONFIG_DIR` naming its directory; [configuration.md](configuration.md),
  "How the roles reach each other"). On a network the installation trusts to carry plaintext — a
  service mesh — `OK_PLAINTEXT=1` admits a plaintext endpoint on a cluster address; the credential
  is presented all the same.
- **Mounting.** A container usually cannot mount, so the role is started in a user and mount
  namespace of its own, where it is root with full capabilities over the mount namespace and the
  daemon mounts directly (FUSE is mountable in a user namespace): `unshare --map-root-user --mount
  --propagation private -- ok …`, as the chart starts it (`checkout.Launch`). ok never makes the
  namespaces itself — only a single-threaded process can enter a new user namespace — and never
  runs itself again to become one. At start it checks it is in them (`checkout.RequireIsolated`:
  not the host's user namespace, and able to make every mount private), refusing with how to start
  it otherwise. Only processes in the namespace see the mount. The pod's init is its pause process
  (`shareProcessNamespace`), which reaps what Bazel leaves behind; ok is not PID 1.
- **Presenting.** A `Present` on the control socket mounts the commit's tree read-only
  (`MS_RDONLY`, and `EROFS` from the daemon); each later one moves the presentation to the new
  commit's tree the way a workspace's checkout does, reading only the trees of directories the
  kernel has looked into and that changed, so a file a job never reads is never fetched. A commit
  with conflicts is refused (`Response.refused`). Files a `Present` names are shown at the root
  beside the commit's until the next one: a job's context file. A presentation shows no `.jj` and
  applies no redirections.
- **Reading ahead.** A `Present` can name globs of files to have local before it returns — the
  deployer's planning reads the build graph's files and nothing else — and the trees they are
  found through, and every file's size, are read with them. What the commit's checkout held
  (scm.md, "Checkouts": the files planning reads among them) is local already, and not read again.
- **Recovery.** If the daemon exits, the next `Present` starts it again and presents again,
  detaching the dead mount first. Presentations are not persisted: they are the role's, and end
  with it.

## Testing

- **The model without a kernel.** The inode, overlay, and journal layer is a library driven by
  FUSE-shaped operations in-process: materialization, renames across materialized and virtual
  directories, hard links, deletion and re-creation, snapshot and check-out sequences, and
  recovery from an interrupted write.
- **The kernel.** Tests that mount need `/dev/fuse`. Each enters its own user and mount namespace
  and mounts there, which needs no privilege (FUSE is mountable in a user namespace:
  `FS_USERNS_MOUNT`); `okfs_fuse::testns` runs a process there. On a real mount: POSIX behaviour,
  mmap, locks, and the daemon's restart and crash recovery.
- **NFS.** okfs-nfs's server over TCP, called as the kernel's client calls it (`client.rs`), on
  any platform: handles, one-time mounts, paged listings, retransmitted creates, and a starting
  server holding calls for its exports. okfs's NFS adapter the same way, from the model's fixture
  trees: writes, a SETATTR that changes nothing leaving the model as it was, a checkout's moved
  times, and inodes let go. On macOS, a real `mount_nfs` of the fixture, driven by file-system
  calls, including a checkout seen the moment it is asked for.
- **The SCM.** `Workspaces` and the sizes-only read against PostgreSQL: leases, takeovers, forks,
  retention, and garbage collection keeping checkpoint trees.
- **jj, both ways.** ok's jj suites run their scenarios in a real and a virtual workspace against
  a real `ok scm`, so "functionally the same" is a test, not a claim. How to run them is
  [building.md](building.md), "Tests".

## Decisions

1. **The FUSE protocol layer is ours**, over `/dev/fuse`, with `libc`, `rustix`, and `nix`
   (vendored already) — not the `fuser` crate. okfs needs what a crate would have to grow:
   adopting descriptors from systemd's store, `FUSE_NOTIFY_RESEND`, one descriptor per thread, and
   an async core shared with ok's stores.
2. **One store per repository per user** for virtual workspaces.
3. **Virtual by default** for `jj clone` and `jj create`; `--real` for a real checkout.
4. **Every write durable in ok**, as checkpoints outside the operation log ([Checkpoints](#checkpoints)).
5. **Attaching on any machine**, one writer at a time under a lease, with forks kept
   ([Attaching](#attaching-a-workspace-on-another-machine)).
6. **Watchers learn of checkouts through the journal subscription**; what editors still miss is
   measured once it runs.
7. **Lingering is offered on the first virtual clone.**
8. **macOS over NFS**, served by okfs itself: FSKit needs Apple's paid signing for every build,
   and macFUSE a kernel extension ([macOS](#macos)).

