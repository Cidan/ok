# ok's SCM: a jj-native source control service

The SCM is ok's source of truth for code: repositories, their objects and operation logs, `main`,
and ChangeLists. This is its design. Related: `docs/identity.md` (who callers are).

## What this is

ok is built of roles on one runtime, and a client; three matter here:

- **The SCM** (`internal/scm`) is the source of truth for code. It is a Go gRPC service
  that implements jj's storage interfaces server-side: content-addressed objects (file contents in
  packfiles in object storage, every other object in its database), one operation log per workspace — each
  workspace isolated from every other — each repository's `main`, and the ChangeList lifecycle —
  identity, author, status, patchsets, stacks, and landing.
- **ok control** is the web UI and the review process: reviewers,
  LGTM/Approval grants, comments and their porting, the approval evaluation, OWNERS, suggested
  reviewers, the change log, and automation. It reads code through the SCM and drives CL
  transitions and submit through it.
- **The client** (`jj/`) is a custom `jj` binary: upstream `jj-cli` and `jj-lib` with the SCM's
  stores registered and ok's commands (`jj clone`, `jj cl`, `jj sync`) built in. It is the only way
  code enters ok.

There is no git anywhere: no git objects or refs, no SSH transport, no hooks, no `Change-Id`
trailers, no git binary. There are no branches in the user flow either. A repository has exactly
one line of history, `main`, which only submit moves; everything else is a ChangeList.

## Decisions (locked)

1. **jj-native.** The SCM implements jj's `Backend`, `OpStore`, and `OpHeadsStore` over gRPC and
   covers jj's whole surface: first-class conflicts, copy tracking, commit signing, the operation
   log, and workspaces. The index, working copy, workspace store, and submodule store stay jj's
   defaults, on the client.
2. **Multi-repository, keyed by repository id.** The SCM keeps each repository's ID and content;
   the directory — ok identity — names it, owns it, and says who reads it (hosted.md, "Tenancy, as
   built"). A repository is made under the entry the SCM reserves in the directory, for the
   person who asks: `app`, the installation's organization's, or `acme/app`, an owner's (a full
   name; its name is 1–64 lowercase letters, digits, `.`, `_`, and `-`, starting and ending with a
   letter or digit, and words ok's pages use are reserved). The SCM finds a repository by full
   name — or one it had — through the directory, and shows the full name it has now. Renaming,
   transferring, and visibility are the directory's.
3. **The operation log is server-side, one per workspace: (repository, principal, workspace).**
   A workspace is the CitC-client analogue, and it is isolated from every other — the same
   person's too — always: its operation log and its view are its own, it never sees another
   workspace's changes or working copy, and nothing done in one reads or changes another. The
   server is the only thing workspaces share, and work moves between them only through it, as
   CLs: upload, submit, `jj sync`, `jj cl get` — as between people. One workspace attached on
   several machines (`jj clone --attach`) is one log. A workspace is made by `CreateWorkspace`
   (`jj clone`, `jj create`), once and exclusively, and a request naming a workspace that was
   never made is refused, so two clones can never come to share a log, and so is a request
   naming no workspace. Objects — commits, trees, files — are the repository's, not a
   workspace's: a publish of objects alone names none.
4. **BLAKE3-256 over a canonical encoding** defined here, implemented in Go (server) and Rust
   (client) against shared golden vectors. Objects travel over the wire as their canonical bytes,
   and both ends verify every hash they receive.
5. **Storage split.** File contents live in packfiles in object storage, FastCDC-chunked above 128 KiB. Every
   other object and all state live in the SCM's database: PostgreSQL, or Spanner.
6. **CLs only.** Uploading creates and updates CLs; `main` moves only by submit. Only a CL's
   author uploads its patchsets.
7. **Submit is path-level, as in Piper.** A CL lands on `main` when none of the paths it changes
   have changed on `main` since its base. There is no server-side content merge; content merges
   happen only in jj, on the client.
8. **The SCM owns the CL lifecycle; ok control owns review.** The line is the write path: every
   invariant that upload or landing must enforce atomically lives in the SCM's database.
9. **Every jj transaction is one atomic write.** The client computes IDs locally and stages new
   objects in a durable outbox. When the transaction ends, one `Publish` exchange sends them
   together with the operation-heads update, so nothing a transaction wrote becomes visible
   without its operation. A command that snapshots the working copy and then changes something is
   two jj transactions, so two publishes.
10. **Repositories are never deleted** (decided 2026-09-27). Neither the protocol nor ok
    control offers it: the risk of an accidental, unrecoverable loss outweighs the need, and
    deletion will be designed when one arises. Garbage collection reclaims what nothing reaches
    inside a repository; a repository itself stays.

## The workflow

The CL workflow is `docs/changelists.md`'s, minus the git transport:

```
jj clone home ~/src/home    # a workspace; the op log lives on the server
cd ~/src/home
jj new 'trunk()' -m 'add rate limiter'
…edit…
jj cl                       # upload @ (and its mutable ancestors) as CLs; prints their URLs
                            # review happens in the web UI; the author submits there
jj sync                     # advance main@ok, retire landed changes, rebase the rest onto main
jj sync --upstream          # in a fork: take its upstream's newer work first
```

- **`trunk()` is `main@ok`**, a remote bookmark the client keeps current. It is untracked, so it
  never becomes a local bookmark, and jj's default `immutable_heads()` (which includes `trunk()`
  and untracked remote bookmarks) makes `main`'s history immutable. Local bookmarks still work as
  personal labels; the server never interprets them.
- **Workspaces.** Every `jj clone` is a workspace of its own, isolated from every other
  (Decisions, 3): as many side by side as there are lines of work, each seeing only its own
  changes. `jj workspace add` is refused — a jj workspace added to a repository shares its
  operation log — and so is `jj workspace rename`, since a workspace's log is keyed by its name. `jj clone --attach <workspace>` mounts the same workspace on another machine.
- **Stacks.** `jj new` on top of a change, then `jj cl` uploads the chain, one CL per change. A
  stack lands bottom-up.
- **Other people's CLs.** `jj cl get <n>` brings a CL's current patchset into your view as the
  untracked remote bookmark `cl/<n>@ok`, which jj treats as immutable. You can test it or stack
  your own changes on it (they upload as CLs stacked on it); you cannot upload to it. `jj sync`
  moves the bookmark to the CL's current patchset, and to the landed commit once the CL lands,
  rebasing your changes along with it.

What a git-based review system needs and ok does not: SSH keys and the SSH transport,
`jj git clone`, `jj gerrit upload`, `Change-Id` trailers, `refs/cl/*`, landed changes that
`jj sync` has to abandon as leftovers, and new repositories needing an initial commit (`main`
starts at jj's root commit).

## Where jj's stores live

| jj store | Implementation | Lives |
|---|---|---|
| `Backend`: commits, trees, files, symlinks, copy histories | `ok` | SCM; cached on the client |
| `OpStore`: operations and views | `ok` | SCM, per operation log |
| `OpHeadsStore` | `ok` | SCM, per operation log |
| `IndexStore` | jj default | client, built locally from prefetched commits |
| Working copy | jj default (`local`) | client disk; `jj sparse` works as usual |
| `WorkspaceStore`: workspace name → local path | jj default | client; a path only means something on its own machine |
| `SubmoduleStore` | jj default | unused: submodules are rejected |

The index stays local because revset evaluation calls it constantly and needs local latency. jj
builds it incrementally, one operation at a time. The one expensive build — a fresh clone, which
indexes every commit the operation log references — is fed by a bulk prefetch (see "The client").

## Objects and identity

### Canonical encoding

Every object has exactly one byte encoding. Primitives:

| Type | Encoding |
|---|---|
| `u8`, `u32`, `i32`, `u64`, `i64` | fixed-width little-endian |
| `bool` | `u8` 0 or 1 |
| `id` | exactly 32 bytes (a BLAKE3-256 digest), no length prefix |
| `change-id` | exactly 16 bytes, no length prefix |
| `bytes` | `u64` length, then the bytes |
| `str` | `bytes` holding valid UTF-8 |
| `seq<T>` | `u64` count, then the elements in order |
| `opt<T>` | `u8` 0 (absent), or `u8` 1 then `T` |
| `set<T>` | `u64` count, then the elements in strictly increasing bytewise order |
| `map<K,V>` | `u64` count, then `K`,`V` pairs in strictly increasing bytewise order of the raw key (the string's bytes or the id's bytes, not the length-prefixed form) |
| `merge<T>` | `u64` count (odd, ≥ 1), then jj's `Merge` terms in jj's order: add₀, remove₀, add₁, remove₁, …, addₙ |
| `timestamp` | `i64` milliseconds since the epoch, `i32` time-zone offset in minutes |
| `signature` | `str` name, `str` email, `timestamp` |

Decoders reject anything that isn't canonical, so `encode(decode(b)) == b` always holds and an
object's ID is a function of its meaning. Each rejection carries a code both implementations
share:

| Code | Rejects |
|---|---|
| `truncated` | input that ends inside a value |
| `trailing` | bytes after a complete object |
| `kind` | an unknown kind byte |
| `bool` | a `bool` above 1 |
| `option` | an `opt` tag above 1 |
| `utf8` | a `str` or symlink target that isn't valid UTF-8 |
| `unsorted`, `duplicate` | set elements, map keys, or tree entry names out of strictly increasing order, or repeated |
| `merge` | a `merge` count that is even, including 0 |
| `tag` | a tree value tag other than 0, 1, or 2 |
| `name` | an invalid tree entry name |
| `path` | an invalid copy history path |
| `copy-id` | a copy ID of neither 0 nor 32 bytes |
| `state` | a remote ref state above 1 |

A decoder reads in encoding order and reports the first violation it meets, so both
implementations give the same code for the same input. A length or count larger than the bytes
remaining is `truncated` as soon as it is read: every element encodes to at least one byte, so
the count can't be satisfied, and refusing it early bounds what a hostile count can make a
decoder allocate.

### Object kinds

An object's ID is `BLAKE3-256(kind ‖ body)`. The kind byte separates the domains, so no two kinds
can collide.

| Kind | Object | Body |
|---|---|---|
| `0x01` | file | the raw contents |
| `0x02` | symlink | the target's raw bytes (UTF-8) |
| `0x03` | tree | `seq<entry>`, where `entry` = `str` name, `value` |
| `0x04` | copy history | `str` current_path, `seq<id>` parents, `bytes` salt |
| `0x05` | commit | `seq<id>` parents, `seq<id>` predecessors, `merge<id>` root_tree, `merge<str>` conflict_labels, `change-id`, `str` description, `signature` author, `signature` committer, `opt<bytes>` signature |
| `0x06` | operation | `id` view_id, `seq<id>` parents, `op-metadata`, `opt<map<id, seq<id>>>` commit_predecessors |
| `0x07` | view | `set<id>` head_ids, `map<str, ref-target>` local_bookmarks, `map<str, ref-target>` local_tags, `map<str, remote-view>` remote_views, `map<str, ref-target>` git_refs, `map<str, ref-target>` git_heads, `map<str, id>` wc_commit_ids |
| `0x10` | chunk (storage only; not a jj object) | the chunk's raw bytes |

The compound fields:

- **Tree `value`** is a `u8` tag followed by its fields, with tags matching jj's `TreeValue`
  ordinals:
  - `0` is a file: `id` file, `bool` executable, `bytes` copy_id (empty, or 32 bytes);
  - `1` is a symlink: `id`;
  - `2` is a tree: `id`.

  jj's `GitSubmodule` has no encoding.
- **Tree entries** are sorted by name bytes with no duplicates, which is jj's
  `RepoPathComponentBuf` order. A name is non-empty and contains no `/` or NUL, and is neither `.`
  nor `..`. jj itself forbids only empty names and `/`.
- **A copy history's `current_path`** is non-empty, and each of its `/`-separated components is a
  valid tree entry name.
- **`op-metadata`** is `timestamp` start, `timestamp` end, `str` description, `str` hostname, `str`
  username, `bool` is_snapshot, `opt<str>` workspace_name, `map<str,str>` attributes.
- **`ref-target`** is `merge<opt<id>>`.
- **`remote-view`** is `map<str, remote-ref>` bookmarks, `map<str, remote-ref>` tags.
- **`remote-ref`** is `ref-target` target, `u8` state (0 new, 1 tracked).

These mirror jj's structs field for field (`lib/src/backend.rs`, `lib/src/op_store.rs`),
because the stores must round-trip exactly what jj hands them. Two points need care:

- **Commit predecessors are encoded but weak.** jj still records them in commits by default
  (`experimental.record-predecessors-in-commit = true`). They are also what keeps two otherwise
  identical rewrites distinct: `CommitBuilder::write` refuses a new commit whose ID already
  exists, and jj's own backends hash predecessors for this reason. They are never
  closure-checked and never followed by GC, since the operation's `commit_predecessors` is jj's
  record of evolution.
- **The signature is stored alone.** A signed commit's signed data is the kind byte plus the
  commit's body with `signature` absent. The encoding stores only the signature bytes, and
  `read_commit` rebuilds jj's `SecureSig { data, sig }` from them.

jj is pinned to 0.45.1 (`//third_party/jj`), and the shapes above are that release's. When an
upgrade adds a field to a stored struct, a new kind value is allocated for the new shape, and
both shapes stay readable.

The encoding is implemented twice: in Go for the server (`internal/scm/encoding`) and in Rust
for the client (`jj//encoding`, over jj-lib's own types). Golden vectors
(`internal/scm/encoding/testdata/vectors.json`) hold the two together: every kind and edge
case with its annotated bytes and ID, and every rejection with its code. Both test suites run the
whole set.

### IDs and roots

- **ID lengths.** Commit, tree, file, symlink, copy, operation, view, and chunk IDs are 32 bytes.
  Change IDs are 16 random bytes that the client generates, as jj does.
- **The root commit** is 32 zero bytes, with change ID 16 zero bytes. It is virtual: never stored,
  and read back as jj's `make_root_commit`.
- **The empty tree** is `BLAKE3(0x03 ‖ u64 0)`. It is the same for every path, as jj requires.
  It is virtual on both ends, never stored and always present, because jj never writes the empty
  tree itself: a new repository's first working-copy commit references it.
- **The root operation** is 32 zero bytes, and so is its view's ID. The view is jj's
  `View::make_root`. Both are virtual.
- **File IDs are content IDs**, independent of chunking. Chunking is a storage policy that can
  change without changing any ID.

### What the server verifies on every write

- Each object's bytes decode canonically and hash to its claimed ID.
- A file's chunks, concatenated in manifest order, hash to the file's ID. Chunks the server
  already holds are read back to do this.
- **Closure.** Every ID an object references resolves to an object the repository already holds
  or one arriving in the same publish:
  - a tree's entries (files, symlinks, trees, and non-empty copy IDs);
  - a commit's parents (or the root) and its root-tree terms;
  - a copy history's parents;
  - an operation's view, its parents (or the root operation), and the commits in its
    `commit_predecessors`;
  - every commit a view references;
  - an operation-heads update's new head, which must be in the operation log of the caller's
    workspace.

  Commit predecessors are the one reference exempt from this (see above).
- A commit has at least one parent. A tree entry is never a submodule.
- **A file is at most 50 MiB**, refused from its manifest before any of it is sent, naming its
  path. ok's jj snapshots nothing larger by default; this holds whatever a person's
  configuration says.

The client verifies everything it receives the same way. Neither end trusts the other's bytes.

## The client

`jj/` is a `rust_binary`. Its `main` is `CliRunner::init()` with
`add_store_factories` (the `ok` backend, op store, and op heads store), `add_subcommand`
(ok's commands), and a built-in configuration layer:

- `trunk()` is `main@ok`, written as
  `latest(remote_bookmarks(exact:"main", exact:"ok") | root())` — exactly `main@ok`
  whenever it exists — because `immutable_heads()` is built on `trunk()` and must evaluate in
  every repository state, including a clone's before `main@ok` is set (jj's own `trunk()` is
  written the same way);
- the SCM endpoint, `ok.endpoint = "https://ok's address"`, which a clone records in
  `store/ok.toml`;
- `snapshot.max-new-file-size = "50MiB"`, the largest file the SCM holds (upstream's default is
  1 MiB): a larger new file is left untracked, with jj's warning;
- `user.name` and `user.email` defaulting to the caller's principal (handle and
  `<handle>@<email domain>`).

### Stores

- **Backend (`ok`).**
  - **Reads** check the local cache, then the outbox, then the server. Concurrent reads are
    coalesced into batched `ReadObjects` and `ReadFiles` calls, and `concurrency()` is 64.
  - **Writes** encode and hash the object, stage it durably in the outbox, and return the ID
    immediately. `write_file` chunks as it streams: files under 128 KiB are one chunk, larger ones
    are FastCDC-chunked (minimum 16 KiB, average 64 KiB, maximum 256 KiB).
  - **`write_commit`** computes the ID from the commit's encoding. If jj passes `sign_with`, it
    signs the unsigned encoding first.
  - **Copy tracking.** `read_copy`, `write_copy`, and `get_related_copies` store and serve
    `CopyHistory` objects. jj's working copy writes only placeholder copy IDs today
    (`local_working_copy.rs`), so `get_copy_records` does what jj's git backend does: it diffs the
    root commit's tree against the head's with rename and copy detection. An exact content match
    comes first, then 50% similarity, with copies drawn from modified files, capped at 1,000
    candidates. Each record has `target_commit` = head and `source_commit` = root.
- **OpStore (`ok`).** Reads work the same way (cache, outbox, server). `write_view` and
  `write_operation` stage into the outbox. `resolve_operation_id_prefix` combines the server's
  answer with the operations still in the outbox (jj can leave an operation unpublished, for
  example with `--no-integrate-operation`). `gc`, reached from `jj util gc`, calls
  `CollectOperations`.
- **OpHeadsStore (`ok`).**
  - `get_op_heads` asks the server.
  - `update_op_heads(old, new)` is `Publish`: it drains the whole outbox — the transaction's
    commits, trees, and files, its view and operation, and anything an earlier snapshot staged —
    and swaps the heads, all in one server transaction.
  - `lock()` returns a guard that holds nothing. jj documents the lock as unnecessary for
    correctness (`op_heads_store.rs`): it only stops two processes from resolving the same
    divergent operations, and heads that diverge anyway are resolved on the next load. A real
    lock would cost a round trip on every transaction.

jj writes a transaction as `write_view`, `write_operation`, the index write, then the lock and
`update_op_heads` (`lib/src/transaction.rs`). Publishing at `update_op_heads` therefore makes the
transaction's writes atomic: either all of them become visible with the new operation head, or
none do.

### Cache, outbox, and prefetch

- **The cache** is the machine's, one per SCM and repository, shared by every clone and virtual
  workspace of it: `~/.cache/ok/<host>/<repository id>` (or under `$XDG_CACHE_HOME` or
  `$OK_CACHE_DIR`). It is content-addressed and immutable, verified on arrival and again on
  every read, and its entries live in **segments**, not a file each (`jj//store/segments.rs`):
  - **Segments** of up to 256 MiB, each entry a few bytes of header and its bytes. An entry whose
    ID is its hash carries no key: a read checks the bytes against the ID it asked for, so damage
    is only ever a miss. A derived record (a manifest, a sizes record alone) carries its key and a
    checksum.
  - **Index runs**, immutable and memory-mapped: a 256-entry fan-out and 12 bytes an entry (a
    48-bit ID prefix, a segment, an offset), merged geometrically like git's multi-pack index. The
    segment being written is indexed by records appended beside it. Nothing is held in memory per
    entry, and readers take no lock; writers append under one.
  - **What an entry is.** An object's canonical bytes, a tree's with its sizes record as a
    trailer; a file of one chunk (most files) whole, found by its file ID and its chunk's, with no
    manifest; a larger file's chunks and its manifest.
  - **Compressed as sent.** Chunks are kept as the SCM sends them — zstd, as packs store them, or
    zstd with the network's dictionary, which is kept beside them and marked used whenever it
    decodes one (*Dictionaries*) — and decompressed on read; what is written here is compressed
    (ruzstd) before it is kept. What was
    read recently is kept decoded in memory (64 MiB).
  - **Retired a segment at a time**, oldest first, past an 8 GiB budget, never by reading the
    whole cache: an entry read since it was written (a 2-bit counter in its header, S3-FIFO's
    rule) or pinned is copied forward; the rest leave with the segment. An entry is pinned while an
    outbox holds or references it — every outbox sharing the cache registers itself there — so the
    client can always re-send what a publish turns out to need.
  - Its metadata is a few bytes an entry where a file each cost a block and an inode: a clone of
    the monorepo's `main` — measured in this document, "The client's store".
- **The outbox** (`.jj/repo/store/outbox`) is written and fsynced before a write call returns,
  because the working-copy state can record a tree ID before the next publish. Entries leave the
  outbox only after the server acknowledges the `Publish` that carried them.
  - A publish refused for missing references (`PublishRejection`) is retried once, with those
    objects re-sent from the cache.
  - An operation that still can't be published — its parent operation was collected after a
    `jj op abandon` elsewhere — is moved to `.jj/repo/store/quarantine` with a warning. The client
    then reloads from the server's heads, so one bad entry never blocks later commands.
- **Prefetch.**
  - `jj clone` streams the whole operation log — its operations and views
    (`PrefetchOperations`) and the commits they reference (`PrefetchCommits` with `op_log` set) —
    and the working-copy tree (`ReadTreeClosure`, then `ReadFiles`) before jj builds its index
    and checks out.
  - A cache miss on a commit fetches it together with up to 1,000 of its uncached ancestors
    (`PrefetchCommits` with `limit`).
  - A cache miss on an operation fetches it together with its uncached ancestors and every one's
    view, up to 1,000 (`PrefetchOperations`): jj builds its index by walking the operation log an
    operation at a time, each view after its operation, which was two round trips per operation
    on a fresh workspace. The fetch names as held the client's **operation frontier**
    (`op_frontier` in the cache): the newest 64 operations whose whole ancestry the cache
    received — the heads a clone fetched, the operations a prefetch completed, and each operation
    the client publishes — so the server sends only what lies beyond them. An operation or view
    the prefetch did not bring (one eviction took) is read directly.
  - `jj sync` prefetches `main`'s new commits.
  - A working copy's move to another commit reads, before jj's checkout, the new tree's closure
    against the old one and each changed file against the one it replaces (*Deltas*).
  - **Checkout layers** a presentation took in are listed in the cache (`checkout_layers`, the
    newest 64) and named as held when it reads the next commit's checkout (*Checkouts*).

### Commands

- **`jj create <name> [dest] [--workspace <name>]`** creates a repository on the SCM
  (`Repositories.CreateRepository`, the caller becoming its creator) and then does exactly what
  `jj clone` does for it: the new, empty repository (main at the root commit) is tracked from the
  first command. It never creates a local-only repository, and it refuses a name that already
  exists rather than attaching to it — that is `jj clone`.
- **`jj clone <repo> [dest] [--workspace <name>]`**
  - Makes the workspace on ok (`Workspaces.CreateWorkspace`), with an operation log of its
    own: named `<host>-<directory name>` (the host's first label), numbered past the person's
    workspaces, unless `--workspace` names it — refused when the person has one of that name.
  - Lays out the workspace's own jj repository — a real checkout's in `.jj/repo`, a virtual
    workspace's in its directory of okfs's state (`docs/okfs.md`) — with the `ok` store
    types and `store/ok.toml` (the endpoint, the repository's id and name, and the workspace).
  - Loads it from the workspace's operation heads (the root operation, in a new workspace)
    through the registered store factories.
  - Runs `jj sync`'s first step, so `main@ok` and `trunk()` resolve from the first command.
    The workspace's working-copy commit is a new change on `trunk()`.
  - With `--attach <workspace>`, mounts an existing workspace instead: its own log, loaded into a
    repository of its own on this machine.
- **`jj cl [-r <rev>] [--ready | --wip] [--reviewer <handle>]…`** uploads the change and its
  mutable ancestors — everything between it and `immutable_heads()`, so it stops at `main@ok`
  and at any `cl/<n>@ok` it is stacked on — bottom first, as `Upload`. The change is `@` by
  default, or `@-` when `@` has no description, as in `changelists.md`. A reviewer is a handle or
  `<handle>@<email domain>`. Before calling the server, `jj cl` refuses:
  - a merge commit;
  - a conflicted change;
  - an empty description;
  - an empty change.

  It publishes anything still staged, so every commit is on the SCM, then prints each CL's
  number, its outcome (created, new patchset, or unchanged), and its URL,
  `<ok.web-url>/<repository>/cl/<n>` (`ok.web-url` defaults to ok's public URL; the repository is
  the full name the clone recorded — `acme/app`, or an installation's `app`, whose bare path ok's
  site redirects to `/example/app`). A
  refusal names the change by its change ID and says what to run.
- **`jj cl get <n>`** prefetches CL `n`'s current patchset and points the untracked remote
  bookmark `cl/<n>@ok` at it. It refuses a landed CL (it is on main) and a CL whose change is
  already in the view as anything but that bookmark — your own CL, which would only turn
  divergent. Getting a CL you already have moves the bookmark to its current patchset, and what
  is stacked on it follows, as in `jj sync`.
- **`jj sync`** runs in one transaction:
  1. `GetRepository` for the head H. It prefetches H's new commits, adds H as a head, and sets
     `main@ok` (untracked) to H.
  2. `LookupChanges` for the change IDs of every visible mutable commit and every `cl/<n>@ok`.
  3. **Landed changes.** For each local commit X whose CL has landed, with landed patchset C (base
     P) and landed commit L:
     - **X is one of the CL's patchset commits**, or X's tree equals C′ (C rebased onto X's
       parent: jj's merge of X's parent and C over P). Then X adds nothing beyond what landed, and
       it is recorded as abandoned onto L (`MutableRepo::record_abandoned_commit_with_parents`):
       its descendants and bookmarks move to L, and a working copy on X gets a new change on L.
       Recording X as rewritten into L instead would put that working copy on L itself, an
       immutable commit on main.
     - **Otherwise** X carries edits made after the landed patchset, and they survive. A new
       change N replaces X, with a fresh change ID, parent L, X's description and author, and as
       its tree jj's merge of L and X over C′ (exactly X's edits since C, applied to L). N is
       written as a rewrite of X, so `jj evolog` connects them.

     Either way X's descendants rebase onto the replacement, and the change never looks
     divergent.
  4. **Other people's CLs.** Each `cl/<n>@ok` moves to the CL's current patchset, or is removed
     once the CL lands. The commit it pointed at is recorded as rewritten into the new patchset
     (or into the landed commit), so your changes stacked on it follow. Immutable commits never
     follow: someone else's CL stacked on the moved one moves by its own bookmark.
  5. Every remaining root of the user's mutable changes built on main's history is rebased onto
     H with jj's own merge: each parent on main's history becomes H, and any other parent stays,
     so a local merge keeps its other side. Conflicts become first-class jj conflicts, and
     changes emptied by the rebase are abandoned.

  "Mutable" is the user's own `immutable_heads()`, resolved against the transaction as it
  goes, so `main@ok` and every `cl/<n>@ok` are immutable, and so is whatever else the user
  configures.
  6. It lists changes whose CL is abandoned, without touching them (the author may restore the
     CL).

  A working copy left on main's history — on a change that landed as itself, so is main's own
  commit now — gets a new, empty change on H, as one on a retired change does: nobody is left
  editing an immutable commit.

### Credentials

The SCM takes each caller's principal from ok's serving runtime, which resolves every request
before a service sees it (`docs/identity.md`): a person's machine presents its device credential
and a proof signed by its key, a role presents its robot's credential, a browser its session. A
request no method attributes is refused `UNAUTHENTICATED` by every SCM service. Which callers
reach each service is declared in the protocol and enforced by the runtime (*Authorization*). The
REST surface makes each request a call through the same gRPC server, so a REST caller is
identified — and refused without an identity — exactly as a gRPC one.

A workstation's `jj` signs in once (`jj auth login`, ok's device login) and presents the machine's
credential on every call.

## The server

`internal/scm` is a the serving runtime-served, pure-Go binary (`ok scm`) exposing nine gRPC services on
the serving runtime's single port, split along who may call them (*Authorization*):

- **`Repositories`** for the repository registry;
- **`Store`** for jj's storage surface;
- **`ChangeLists`** for uploads and CL reads;
- **`Review`** for ok control's CL transitions and rollbacks;
- **`Landing`** for ok automation's landing queue: candidates and landing;
- **`LandingChecks`** for dry runs of a landing (ok control's Submit button);
- **`Browse`** for code reads by ok control and automation;
- **`Events`** for each repository's log of events;
- **`Workspaces`** for okfs's attachment leases and checkpoints (`docs/okfs.md`).

Replicas are stateless: all coordination is in the database, and file contents are in a private
object store the installation names (`scm.objects`: an S3 or GCS bucket, or a directory). The SCM brings its tables to its schema when it
starts (*Schema*). `ok scm fsck [-repair] REPOSITORY` verifies one repository against its
invariants.

### Publish

`Publish` is one bidirectional stream per jj transaction, two round trips:

1. The client sends `begin`: the repository, the ID of every staged object, and a manifest (chunk
   IDs and lengths) for every staged file.
2. The server replies `missing`, naming the objects and chunks it lacks. It also touches every one
   it already holds (see "Garbage collection").
3. The client sends the missing object bodies and chunks, then `commit`, optionally carrying an
   operation-heads update (`old_ids`, `new_id`).
4. The server verifies everything ("What the server verifies"). It then:
   - writes the new chunks, and the trees, as packs to the object store: each pack is held in
     memory as the chunks arrive and sent once it holds 64 MiB, or once the publish commits — or
     sooner, when the publishes on a replica at once hold all of `scm.publish_memory` (512 MiB):
     one that then finds too little left for its next chunk sends what it holds, and waits only
     if that is still too little. Nothing touches the server's disk. A file whose chunks went out
     in an earlier pack is verified reading that pack back a 4 MiB window at a time, the files in
     the order their chunks lie;
   - runs one database transaction that:
     - locks the caller's `op_log` row whenever the publish carries operations, views, or a heads
       update;
     - inserts the packs', chunks', files', objects', commits', operations', and views' rows;
     - touches every pre-existing object the new ones reference, and every operation and view it
       re-sends;
     - applies the heads update: insert `new_id`, delete each old ID other than `new_id`;
   - replies `done` with the resulting heads.

A failure anywhere leaves nothing visible. A publish whose references don't resolve fails with
`FAILED_PRECONDITION`, and a `PublishRejection` in the status details lists the missing IDs. A
pack uploaded by a publish that then failed has no `pack` row and is deleted as an orphan, a day
after.
Operations and views land in the operation log of the caller's workspace the request names, and
only there: a workspace's views name only its own working copy, and its operations only itself
(`INVALID_ARGUMENT` otherwise).

In a repository that keeps secrets ([secrets.md](secrets.md)), a publish whose `begin` does not
set `guards_secrets` — a client that does not refuse, itself, to store a secret's value — is
refused before anything is exchanged; and a publish, or a checkpoint, whose new trees hold under
`//secret` a file other than a pointer, an `acl.yaml`, or an OWNERS file is refused, naming each
(`FAILED_PRECONDITION`) — so is a tree whose own settings start keeping secrets. A `//secret`
subtree found to hold no value is remembered by its ID.

### Reads

- **A commit's checkout** — its trees, their sizes, and the files planning reads — is one stream of
  layers, derived once (*Checkouts*); **a changed file** may come as a delta against the version
  the client holds (*Deltas*).
- **Every object except file contents** is read from the tail in PostgreSQL, then — trees,
  symlinks, copy histories, and trees' sizes records the tail lacks — from the repository's
  segments (*Segments*), then — a tree a publish stored — from its pack, located as a chunk is
  (*Storage*), behind an in-memory cache of decoded objects. Objects are immutable, so the cache
  is never invalidated.
- **File contents** are read many files at a time, never one by one: every requested file's
  manifest (its size and chunks) with one query per 1,000 files, then the chunks a window at a time
  — up to 16 MiB of them, located with one query — each window's packs read together before its
  frames go out, in request order. Chunks whose data lie within 128 KiB of each other in a pack are
  read with one range GET, up to 4 MiB, the bytes between them discarded; packs keep a
  directory's files together (compaction orders chunks by path), so a directory is usually one
  read. Up to eight range GETs run at once per call. Every chunk is verified against its ID as it
  arrives and kept in an in-memory cache as its pack stores it, compressed; a read whose pack was
  retired by repacking re-reads the chunk's location once. `ReadFiles`, `DiffFiles` (all of a
  call's pairs, up to 32 MiB of contents at a time), `Export`, and every internal read go through
  this path.
- **Chunks travel as stored.** A `ReadFiles` that sets `stored` gets each chunk exactly as its
  pack holds it — one zstd frame where compressing shrank it, the raw bytes where it did not —
  with its codec (`ChunkCodec`) and raw length on the frame, so the SCM neither decompresses what
  it serves nor sends more than it stores (source code compresses about fivefold). The client
  decompresses, checks the raw length, and verifies the chunk against its ID, as it always did; it
  refuses a frame claiming more than a message could carry raw (16 MiB) before decompressing it.
  ok's `jj` and okfs (ruzstd, pure Rust) and ok's workers and deployer (the Go checkout
  client) all read this way, and keep what they read as it came
  (*Cache, outbox, and prefetch*). A client that leaves `stored` unset receives raw bytes and no
  codec. One that sets `dictionaries` too receives the chunks compaction compressed with the
  network's dictionary as they are (`CHUNK_CODEC_ZSTD_DICT`) and fetches each dictionary its
  frames name once (`ReadDictionary`); one that does not receives those chunks raw (*Dictionaries*).
- **Manifests** — a file's size and its chunks' IDs and offsets, with no contents — are read in
  bulk with `ReadFilesRequest.manifests_only`, one query per batch of files rather than two per
  file. okfs reads them for what a directory's sizes record does not answer (*Tree sizes*), then
  only the chunks a read covers (a ranged `ReadFiles`).
- **The three caches** — objects (256 MiB), chunks (512 MiB, compressed as stored), and diffs
  (64 MiB), in each
  replica's memory — hold only values derived from content-addressed IDs, keyed by repository and
  those IDs (a diff by its two files' IDs and its context lines, or by the IDs alone for counts), so
  an entry is never stale and replicas never disagree: a replica either holds a key's one value or
  computes it. Nothing mutable (`main`, operation heads, CLs) is cached. A cached diff holds its
  lines' text in a buffer of its own, never the files it came from, and is served only after the
  repository is seen to hold both files. They evict with S3-FIFO (Yang et al., SOSP 2023): a new entry
  waits in a small queue, a tenth of the budget, and moves to the main queue only if it is read
  twice there, or if a ghost queue of recently dropped keys remembers it; the main queue evicts like
  CLOCK. A clone or an export streams through the small queue without displacing what is read
  every day.

### Tree sizes

A `stat` needs a file's size, and a tree does not carry it: jj's tree lists each entry's name,
kind, and ID. So the SCM derives, for each tree, its **sizes record** (`encoding.TreeSizes`;
Mononoke calls the like "fsnodes"): one size per entry, in the tree's order — a file's length, a
symlink target's, 0 for a subtree. A client that has a directory's record answers a `stat` of any
file in it without asking the SCM, where it once read each file's manifest (51,816 of them, more than
half of a cold build's requests in a virtual workspace).

- **Derived from the tree alone.** A record needs the tree's entries and, in one query, its files'
  sizes and its symlinks' targets — never its subtrees. So any tree, at any depth, in any commit,
  derives on its own: there is no backfill, no walk of history, and no recursive totals whose
  derivation would chain down a tree (a directory's total size is left out for this reason).
- **Derived when first read, then kept.** `TreeSizes` serves a record from memory, then the
  `tree_sizes` table, and derives the rest, keeping them; concurrent derivations write the same
  record, and a tree's deletion cascades to it. Trees no client reads virtually never get one. A
  tree a publish stored, while it is unpinned — a workspace's — has its record kept in each
  replica's memory alone: compaction moves a kept record into segments once its tree has no row,
  and a record that outlived its tree there would be kept for good. Pinned, it is kept, as pinning
  is permanent.
- **Self-describing.** A record is keyed by its tree's ID; its bytes are self-describing — kind byte `0x20`, a version, then LEB128
  varints, about two bytes an entry where a tree spends about 75 — and held to one encoding by
  golden vectors both languages pass (`internal/scm/encoding/testdata/tree_sizes.json`).
- **On the wire.** `Store.ReadTreeSizes` answers up to 100,000 trees in one call, and
  `ReadTreeClosure` with `with_sizes` sends each level's records after its trees
  (`OBJECT_KIND_TREE_SIZES`), so a virtual clone or a presentation receives every directory's sizes
  with its trees, in one stream; a commit's checkout holds every tree's record (*Checkouts*).
- **Trust.** A record is derived, so it cannot be verified against its tree's ID; it is as trusted
  as a manifest's size, which was always the SCM's word until the contents were read and verified.
  The client checks a record's entry count against its tree and ignores one that differs.

### Checkouts

A commit's checkout is what a checkout of it reads besides its files' contents — every tree under
it with its sizes record, its symlinks and copy histories — and the files planning reads, whole: the
build graph (`MODULE.bazel`, its lock, `.bazelrc`, `.bazelversion`, every `BUILD`, `BUILD.bazel`,
and `.bzl`), `.ok/repository.yaml`, and the workflow files (`store.PlanningReads`), each of at most
1 MiB. The SCM derives it once for each commit and keeps it in the bucket, and `Store.ReadCheckout`
sends it in one stream (`internal/scm/store/checkouts.go`).

- **Layers.** A checkout is a list of **layers**, oldest first, which together hold the whole. A
  layer is immutable and named by the BLAKE3 of its bytes — `OKCHKLAY`, a version, then one zstd
  frame of its records, each a kind, an ID, and a body, sorted by kind and ID
  (`internal/scm/layer`, `encoding.EncodeLayerRecords`; held to one format in Go and Rust by
  `testdata/layer.json`). Each layer names the tree it and the layers below it hold the checkout
  of. A commit's checkout is its first parent's layers and one more holding what the commit
  changed from the top one's tree: each tree whose ID differs from that tree's at its path, with
  its sizes, the symlinks and copy histories those name that the tree there does not, and the files
  planning reads that are new where they are. Then the top layers merge, as git's commit-graph
  chains and LSM trees do: while a layer is no more than twice the size of the one above it (or
  there are more than 16), the two are replaced by one layer of what the commit changes from the
  tree below both. So a checkout is a whole layer at the bottom and a few smaller ones above it,
  each less than half the one under it, and the whole layer stays the bottom of every checkout
  along a line of commits until the layers above it outweigh half of it — or a merge would take it
  in — when the commit's checkout is one layer of the whole instead. A parent with no checkout is
  derived first, up to four deep; past that, or for a parent with conflicts, the checkout is whole.
- **Reading one.** `ReadCheckout` names the commit and the layers the client holds, of any commit;
  it answers the checkout's tree and its layers' IDs, then the bytes of each layer the client lacks,
  in pieces of up to 1 MiB. A client checks each layer against its ID, takes its records in as if
  it had read each, and holds the layer from then on. A commit with conflicts has no checkout
  (`INVALID_ARGUMENT`), as `Export` refuses one.
- **Moving.** A client that holds a tree's whole checkout — the one it moves from — names it
  (`have_tree`), and is answered one layer of what the commit changes from it, made for it on the
  fly and kept in each replica's memory by the pair (`CheckoutHeader.since_tree` names the tree it
  completes); nothing, for the same tree, or for a kept checkout whose every layer the client
  holds — moving back to a commit it has seen. So a move costs what changed between the two
  commits, never a layer some other move rewrote, and a commit only moved to — a patchset's, a
  candidate's — derives no checkout of its own. A tree the repository does not hold is answered
  the commit's checkout.
- **When.** Main's head is derived as main moves (`CheckoutWork`, keyed work marked by every
  `HeadMoved` event, run by every replica's `ok/scm/checkouts` node, one repository by one replica
  at a time); any other commit — a patchset, a candidate, a pre run's — the first time a checkout
  of it is asked for, one derivation at a time on a replica, the rest waiting for it.
- **Kept.** `checkouts/<repository>/c/<commit>` (the checkout: its tree and layers) and
  `checkouts/<repository>/l/<layer>` in the SCM's bucket; each replica keeps those it read in
  memory (`CheckoutCacheBytes`, 512 MiB, S3-FIFO as the other caches). Garbage collection's pass
  deletes the checkouts of commits other than main's head older than `CheckoutRetention` (14 days),
  then the layers no kept checkout names, past a day's grace for one a derivation wrote and has not
  named yet. A replica asked for a layer gone since it read the checkout derives the checkout
  again. A repository deleted, or moved to another cell, takes its checkouts with it.
- **Who reads them.** okfs's presentations — ok's workers', the deployer's, deploy agents' —
  take a commit's layers in before they mount or move to it; a real checkout (legacy only) moves
  to a commit by comparing its tree with the one it holds from the layers it holds, walking only
  the subtrees whose IDs differ. Nothing else needs to: a layer is a cache of what the trees
  already say.

### Deltas

A client that holds a file's previous version — the file at the same path in the tree it had — can
name it as the file's **base** (`FileRange.base_file_id`, with `ReadFilesRequest.deltas`). Where the
repository holds the base and both are whole files of at most 4 MiB, the SCM sends the file as one
frame of `CHUNK_CODEC_DELTA`: copies of runs the base shares and the bytes it does not, compressed
(`encoding.EncodeDelta`; held to one format in Go and Rust by `testdata/delta.json`) — when that is
smaller than the file's chunks as stored, else the chunks, as without a base. A delta's frame
carries the whole file's size and the base it was made against; the client applies it to the base
and verifies the result against the file's ID, as it verifies every file. An edited source file is
a few hundred bytes against tens of thousands. Each replica keeps the deltas it made, and that none
paid, by base and file (`DeltaCacheBytes`, 64 MiB).

- **A real checkout** (legacy only) names, for each file that replaces another at its path, the
  file there as its base, read from disk and checked against what the checkout recorded writing.
- **ok's jj**, moving a working copy to another commit (`jj new`, `jj edit`, `jj sync`), reads the
  new tree's closure with the old tree as its base (`ReadTreeClosureRequest.base_tree_id`: a subtree
  whose ID is the base's at its path, and a symlink or copy history the base's tree there names, are
  not sent), then each changed file with the file it replaces as its base, before jj's checkout
  reads them (`jj//store/checkouts.rs`, `move_tree`). A sparse working copy reads as it did.

### Workspaces

A workspace is made by `CreateWorkspace`, with its operation log; okfs's virtual workspaces
(`docs/okfs.md`) keep two more things in the SCM, each the caller's own, keyed by
(repository, principal, workspace name) as the operation log is:

- **An attachment lease** (`workspace`): the machine holding it, its epoch (bumped on every
  grant), its expiry (`LeaseTTL`, 30 s after its last renewal), and a machine waiting to take
  over. `Attach` grants a free lease of a workspace `CreateWorkspace` made — `NOT_FOUND` for any
  other — (or the holder's own again); a live lease held by another
  machine is refused, or — with `takeover` — the holder is asked to hand over, learning so at its
  next `Renew`, and the asker calls `Attach` again. A lease renews while no grant has been made
  since it, even after it expired.
- **Checkpoints** (`checkpoint`): what a workspace's working copy held at a moment — its tree, as
  a merge's terms, the working-copy commit it was taken on, the machine, and its parent.
  `RecordCheckpoint` requires the trees in the repository (published first: a transitive touch
  verifies them and freshens them for garbage collection) and records the checkpoint as the
  workspace's latest when the lease is current and its parent is the latest, and as a fork
  otherwise — kept, never refused.

Garbage collection thins checkpoints (every one for 7 days, then the last of each hour for 30,
then the last of each day for a year; a workspace's latest always). A checkpoint kept past its
first week has its trees pinned — kept for good, in segments, out of the database's rows — and the
trees of the rest are roots of the mark (*Garbage collection and compaction*).

### Browse

`Browse` serves code to ok control and automation as structured messages, not jj objects. They
work on resolved trees, which is all `main` and patchsets ever hold.

| RPC | What it serves | Who uses it |
|---|---|---|
| `GetCommits` | commit metadata | every commit view |
| `Log` | `main`'s history, newest first, optionally only commits that changed given paths — a file, or a directory and anything in it (from `commit_path`) — paged both ways | commit and file history; a directory's latest commit; the "recent reviewers and editors" suggestion |
| `ReadTree`, `ReadFileAt` | a directory listing (with, for a commit on `main`, each entry's last commit), and a file (or a byte range of it), at a commit and path | the repository view; the file view; `/raw` |
| `ReadFilesAt` | many paths of a commit at once, in one stream: each path's value and the head of each file (up to 1 MiB each, 64 MiB a call), or that nothing is there; the trees along the paths read a level at a time, the heads together | OWNERS for a CL's changed files, and a change to `//identity`, in one call each |
| `DiffTrees` | changed paths between two trees, with rename and copy detection | the CL file table; approval's owner sets; automation's affected sets |
| `DiffFiles` | line hunks (histogram diff) for file pairs, several at once, with the lines each adds and removes, flagging binary and oversized files; with `stats_only`, the counts alone, computed without hunks. A pair diffed before is served from the diff cache | every diff view and file list; comment porting |
| `Annotate` | blame for a file at a commit: back along the commit's own first parents to `main`, then `main`'s history of the path (renames are not followed) | blame |
| `IsAncestor` | ancestry between two commits | stack and base checks in ok control |
| `Export` | a checkout of a commit (optionally under path prefixes) as one stream; with `entries_only`, its entries (paths, file IDs, sizes, modes) without contents | ok control's reads of a subtree (the groups-and-ACLs tree's); `ok load`'s synthetic jobs. ok's workers read a commit's checkout instead (*Checkouts*) |

Uploads refuse merge commits, so `main` is a chain of single-parent commits: the commit
`head_log` records at sequence n has generation n. Every `Log` page is therefore an index range
scan — over `head_log` for the whole history, over `commit_path` for given paths — and its page
tokens name a generation and a direction (older or newer). `commit_path` records each changed
leaf and every directory above it, so a directory's history and its entries' last commits are
single index probes, however much lies beneath. Diffs of a file pair are the histogram
algorithm's; `DiffFiles` marks a pair binary (a NUL byte in its first 8,000 bytes) or too
large (over 8 MiB or 200,000 lines) instead of diffing it. Every walk over trees — `DiffTrees`,
`Export`, and the diffs upload validation, landing, and rollback make — reads the trees it
needs a level at a time, each level in one query (up to 1,000 trees), before walking them in
path order; `Export` fills its leaves' sizes and symlink targets 2,000 at a time. A walk of the
monorepo's 13,000 directories is a dozen queries, not one per directory.

### Storage

Packs live at `nets/<network id>/packs/<pack id hex>` in the `ok scm` bucket. A pack holds chunks
of one fork network — a repository and its forks (hosted.md, "Fork networks, as built"), one
repository's alone until it is forked — whose chunk index says where each chunk is (*The content
index*), and the trees the publish that wrote it stored, after its chunks: a publish writes no row
of `object` for a tree, so a workspace's trees cost the database nothing but their content index
entries. A landing's splice writes the trees it makes as rows, unless a publish stored them. The pack ID is the BLAKE3 of the pack's bytes, and the nonce makes every pack written unique (version 1 packs, written before
it, have none and are read the same way).

```
magic      "OKSCMPAK"                        8 bytes
version    u32 = 2
nonce      16 random bytes
records    repeated:
  id             32 bytes: a chunk's, or a tree's
  codec          u8     0 = stored, 1 = zstd, 2 = zstd with a dictionary
  raw length     u32
  stored length  u32
  data           stored-length bytes
index      u64 count, then (id, u64 offset of its record), sorted by id
footer     u64 offset of the index, magic "OKSCMEND"
```

Chunks are stored with zstd (level 3) unless compression doesn't shrink them; compaction later
compresses small ones with the network's dictionary (*Dictionaries*), which the frame names.
The records and index make a pack self-describing, so `ok scm fsck` can verify every pack
against the content index (hashes, locations, lengths) and rebuild its locations from the object
store; a chunk compressed with a dictionary is verified with it. A record carries no kind: an ID
hashes its kind's byte with the bytes, so a record is a chunk's or a tree's as its bytes hash to its
ID, which fsck checks against what the content index says it is, and a reader decodes a record only
as the kind it asks for. A tree's record is its canonical body, compressed as a chunk is.

### Segments

What a repository holds pinned leaves PostgreSQL for immutable sorted files in the bucket,
`nets/<network id>/segments/<kind>/<segment id>` — a repository's under its network's keys, beside
what its network's chunk index folds, since a fork's manifest names its parent's (*Forks*) —
(`internal/scm/segment`; hosted.md, "Segments, as built"), so a repository's rows are its mutable state and its recent work. So far
three kinds move: trees, symlinks, and copy histories (each its kind's byte, then its body, by ID),
trees' sizes records (by tree), and copy histories' parent edges reversed (a parent's ID, then the
copy's). Pinned is permanent, so segments hold nothing a sweep could take, and every walk — touch,
refresh, pin, and GC's mark — works on rows as before: one that reaches an object in a segment
finds it pinned, and stops. Commits move too, as two kinds: the commit graph (each commit's
generation and parents, by ID: small records, many to a block) and their bodies. A walk over
history goes through the graph's segments in memory and through the tail with one recursive query
each time it enters it, so a deep walk over compacted history costs no query once cached. Main's
path history moves (a path, a NUL, the generation, and the commit, read by path: a segment's Bloom
filter holds its paths too, so `Log` by path, `ReadTree`'s last commits, and `Annotate` skip the
segments that never saw a path), and so do events once the relay has published them and ten
minutes have passed since they were written (by place in the log; a read whose page the tail holds
without a gap reads no segment): on Spanner a writer reads its events' places back by its
transaction once its pass has run, which only the tail records, and another replica's pass may have
placed them, and the relay published them, before it reads. Landed CLs move as they land; a read of
CLs reads their rows and their patchsets in one snapshot, so it finds a CL compaction moves meanwhile
whole, in the tail or in a segment. Operation logs'
operations and views move once settled, touched 15 days before: collection, finding one only a
segment holds unreachable, records a tombstone of it, which hides it from every read and moves into
a segment in turn; re-publishing one only a segment holds puts its row back in the tail, which
collection's check of the last touch then sees; merges drop deleted logs' records, and at the
oldest level the tombstones with what they bury.

- **Compaction** is keyed work per repository (`segments`), marked by every pin. Once 4,096
  pinned rows wait — or, in the daily collection, however few — it writes up to a million of
  each kind as a segment, then in one transaction records the segments in the manifest
  (`segment`) and deletes the rows they hold, each object only if still pinned. A replica that
  then misses a row in the tail finds it in the segment: a lookup the segments its manifest names
  cannot answer is tried again with the manifest read anew, and every manifest change is announced
  (`ok_scm_segments`) to mark replicas' manifests stale.
- **Finding what moves.** Pinned objects and commits, settled CLs, and events are found through
  indexes; every path history row and tombstone moves. What no index finds — trees' sizes records
  and copies' edges of what is pinned, and operations and views settled — a pass looks through a
  window of, a batch of each, from where the replica's last pass of the repository moved what it
  found (or found nothing to move), round from the start once a window reaches the end, so a pass
  costs a batch however long the tail; looking through all of them each pass, compaction, which
  every pin asks for, read 13,000 operations a pass ten minutes into a herd, and growing. Compacting
  fully (a fork's) looks through the whole tail from its start.
- **Merging.** Four segments of a kind at one level are merged into one at the next, and retired.
  Segments are read newest first by age: one written from the tail takes the next; a merged one,
  the newest of its inputs' — no segment outside a merge comes between, since a merge takes every
  segment at its level — so an index record a newer segment superseded never wins.
  A retired segment stays in the bucket a day, for readers whose manifest still names it; the
  repository's collection then deletes it. A segment with no row — a compaction that failed after
  writing it — is deleted once a day old. Every segment carries a nonce, so no deleted key is
  written again.
- **Reading.** A replica keeps each segment's fence of block keys and its Bloom filter in memory
  (`segment_readers`, a quarter of `SegmentCacheBytes`), and blocks it read, decoded, in an S3-FIFO
  cache of their own (`segment_blocks`, 256 MiB): a lookup the filter refuses reads nothing, and
  one it passes reads one block. A batch of lookups reads the blocks its keys lie in that the cache
  lacks with one request for each run of consecutive blocks, several at once; a walk over a range
  reads its first block as a lookup does, then twice as many consecutive blocks each request as the
  last, up to 4 MiB, those read ahead left out of the cache — so a scan of a segment, an object
  store's tens of milliseconds a request, costs a request per 4 MiB, not per block. Within a
  transaction, the manifest is read through it, after the tail.
- A sweep that takes an unpinned tree or copy history takes its sizes record and edges with it.

### The content index

What the SCM knows of files and chunks is held in two **content indexes**
(`internal/scm/internal/contentindex`), each a log of what transactions changed, in the database,
and **runs** — sorted files in the bucket, the segments a fold writes — rather than rows per file
and chunk, on one machinery and in one set of tables, each index by its scope; they are ok scm's
only record of them:

- **A repository's** holds the files, chunks, and trees publishes stored it holds — each file's
  size and chunks, each chunk's and tree's presence — and for all three whether it is pinned and
  the last day anything wrote or referenced it. It is what the repository may read: a file it
  lacks is not found through it, whatever its network holds. The walks — touch, refresh, pin —
  and garbage collection's mark go through its trees as through rows, reading each tree's body
  from its pack.
- **Its network's chunk index** holds where each chunk and tree is: its pack, offset, lengths, and
  codec, pinned when a repository pins it, so a pinned location folds into the network's runs as
  its entry folds into the repository's. A read finds a file or tree in the repository's index and
  its bytes through the network's; repacking moves chunks and trees in the network's alone.
- **Which writes which.** A publish records the files, chunks, and trees the repository lacks in
  its index, and the location of each chunk and tree its network lacks one of in the network's —
  one the network stores already keeps its location, and the new copy is dead bytes compaction
  reclaims. A sweep in a network of more than one repository releases a chunk or tree rather than
  drop its location, and the network's collection drops it once no repository holds it.
  The touch and refresh walks touch the repository's; the pin walk pins in both. A sweep drops a
  chunk from the repository's, and in a network of one its location too, in one transaction; in a
  network of more, it releases the chunk (`chunk_release`), and each collection pass drops the
  location of a released chunk no repository in the network holds, under the network's
  chunk-index lock — which a publish relying on a location holds in share mode, so it sees either
  the chunk held or its location gone (*Writing it*). A location goes wherever it is: one pinned, in the chunk
  index's segments, was pinned by a repository deleted since. Its drop hides the segment's record
  at once, and the next fold writes a tombstone of it into the newest segment. A transaction
  that writes both takes the repository's lock before the network's.

- **Runs.** A fold writes what the log changed since the fold before into runs, every entry whole
  as it now is — its updates applied, in order, to its newest record — or a tombstone of one it
  dropped that a run holds; a small file costs about 120 bytes, its file entry and its chunk's.
  A newer run's record supersedes an older's, so a lookup stops at the newest it finds, through
  the runs' Bloom filters; runs merge only with their neighbours in age, so no merge puts an older
  record above a newer one. A repository's index is two kinds of run: pinned (`segIndex`, *Segments*),
  which forks inherit, and its **unpinned index** (`segIndexUnpinned`), its unpinned entries and
  their tombstones, which they do not — a fork holds what its parent pinned, not its parent's
  workspaces. Pinned is permanent there, so a lookup takes the pinned kind's record when it has
  one, and the unpinned kind's newest otherwise. Each unpinned run carries the day its fold
  committed (`segment.day`), and merges within its **day group**, the runs one day's folds wrote: a
  group's two newest while the older holds at most four times the newer's records, and a group
  whose day has ended into one run. A network's chunk index is one kind, `segIndex`: pinned and
  unpinned locations, moves, and tombstones, merged by level as segments are. Each pack is named by
  its number (`pack.num`), not its 32-byte ID.
- **The log.** Every transaction that changes a file or chunk — a publish storing one, a walk
  touching or pinning one, a sweep dropping one, compaction moving one — writes what it changed
  as `content_log` rows in the same commit — one for each index it changed, or as many as keep
  each under 4 MiB, in order (a database's value has a size limit) — and notifies
  `ok_scm_content` with the index's scope. Log rows apply once each, in the order their
  transactions committed — or in one whose difference changes nothing: puts, touches, pins, and
  moves commute, and a drop, which does not, is of an entry no writer beside it relies on
  (*Writing it*) — a transaction's own in theirs. A drop applied
  again after a later put of its entry would drop it, so a reader never applies a row twice: it
  keeps its **place** in the log, and reads only what committed after it. On Spanner a row carries
  its transaction's commit timestamp (`logged_at`, a staged row its batch marker's), and a
  place is a read's timestamp: what committed by then the read saw, and whatever commits later
  commits after it. The index a reader from a place reads through, by commit timestamp, carries a
  shard before the timestamp, which each row takes at random, and a reader reads every shard: a
  writer holds its index entry at its pending commit timestamp until it commits, and with no shard
  every writer of the index held the same one. On PostgreSQL a row carries its transaction's ID (`xid`), a place is a
  snapshot, and a reader reads the rows its snapshot sees that its place's did not; a row's
  `stamp` orders it — a writer stamps the index's sequence (`content_index.log_seq`), as it stands
  when it holds the index in share mode, or as it writes its rows when it holds no lock, and one
  holding the index alone stamps the next and moves the sequence past it: a writer holding no lock
  that commits after one holding the index alone may be ordered before it, which changes nothing.
  Rows are numbered in their transaction's order, from no counter of the index's, which every
  logging transaction would write.
- **Writing it.** A transaction that relies on what the index holds — a publish deciding what is
  already stored, the touch, refresh, and pin walks — reads it once, as it stands, holding no lock:
  the replica's view brought up to date by the log since its place, read with the index's
  generation at one moment (`ContentLogCurrent`: one statement on PostgreSQL, through the
  transaction's own connection — a read that asked the pool for a second connection while holding
  one could, with every connection held so, wait forever — and a read-only transaction on Spanner,
  where a read in the read-write one would lock what it read). Its own puts, touches, and pins count
  for the rest of it. It holds no lock while it relies only on what nothing can take before it
  commits: entries pinned, entries absent, and unpinned entries changed in the last 12 days — a
  sweep drops only an unpinned entry changed before its cutoff, 14 days ago, so it reaches one
  changed since two days after the transaction read it, and a transaction that would commit more
  than an hour after it read the index is refused and runs again. A lookup that finds an unpinned
  entry changed earlier has the transaction take the index's row lock in share mode
  (`content_index ... FOR SHARE`; on Spanner, which locks cells, a read of its `generation`, the
  cell a lock reads `FOR UPDATE` and a fold writes) and read the index again after it, the lock
  holding the generation and its runs as they are, and log rows only ever added; so does one whose
  replica has no view yet, or whose view's generation a fold moved since — the rows a fold read go
  once it commits. A network's location of a chunk or tree is dropped once no repository holds it,
  whenever that is, so a publish relying on the location of one its repository does not hold — its
  own copy then dead bytes — decides under the network's lock too, the repository's taken first.
  A sweep's drops and compaction's moves take the lock exclusively, and decide after it. So a
  writer never commits against an entry a sweep dropped, and a sweep never drops what a writer
  touched, pinned, or relied on first; writers run beside each other and beside what holds the
  index alone, their changes commuting with its. Two publishes storing one new chunk at once both
  log it, and the index keeps the copy logged first: the other is dead bytes compaction reclaims.
- **Folds.** Each garbage collection pass ends by folding the index, and so does compacting a
  repository fully (a fork's, which takes what is pinned from the runs); otherwise a log folds once
  it changed 65,536 entries — compaction then folds it, or a replica that finds it so marks its
  fold due (`fold-due`, `fold-network-due` keyed work) and one replica's folds node runs it. A fold is
  a generation every replica loads again, its manifest and its log: compaction, which every pin asks
  for, once folded at 4,096, every ten seconds at a monorepo's peak. A fold
  reads, in one snapshot, the generation's place, the log since, and the runs, and writes a run of
  each kind of what the log changed; one short transaction, under the index's lock alone, records
  the runs and makes the next generation, which records the snapshot's place
  (`content_index.folded_through`): it holds every row committed by then, and a reader of it reads
  only the rows after, over its runs. Then it deletes the log rows it folded, a bounded transaction
  at a time, and the staged rows of transactions that never committed, an hour old, and merges the
  unpinned index. A fold holds its index's claim (`fold`, `fold-network` keyed work) for its length,
  so two never overlap; one that finds another running changes nothing. Nothing in the transaction
  that makes the generation reads the log, so writers beside it, logging all the while, do not
  abort it. Runs that change otherwise than by a fold, a merge, or garbage collection's retirement —
  a fork's sync, which takes its parent's pinned runs — make a new generation too. A new repository
  starts with an empty index. A move carries each index's generation, its runs, and the log rows
  its fold did not read, which the SCM exports itself in the order they apply, numbered 1, 2, … in
  it: a row's order is otherwise the database's that wrote it (commit timestamps, stamps), which a
  move does not carry, and so numbered, the destination's database reads that order from the rows
  alone. Freezing a network is one write, however long its logs.
- **Garbage collection reads a day of it.** A run's day is the day its fold committed, so a record
  is in a run of its day or later. The index keeps the day its last complete pass swept to
  (`content_index.swept_before`). A pass lists as candidates the unpinned entries last changed
  before its cutoff's day that the unpinned index's runs of days from that day through the cutoff's
  hold — normally a day's — and the log, each resolved as the index now holds it. The mark keeps
  what it reaches, which the pass touches: live for the window again. It ends by folding, waiting
  for a fold under way, so every candidate's outcome — a tombstone, a touch — is in a run of today;
  then no run of a day before the cutoff holds what a lookup needs, and one transaction retires
  them and records the index swept to the cutoff. A record folded late, its run's day after its
  own, is a candidate in the pass whose window reaches that day.
- **Compaction works from it.** Which chunks and trees each pack holds, how many of its bytes are
  live, and which packs are drained come from the network's chunk index; a record moves only if the
  index still has it where compaction read it. Chunks are ordered by the path on `main` of a file
  that holds them (the path hint a publish sends goes unused), and trees follow them, copied as
  stored: the dictionary is files'.
- **Reading it.** Each replica holds a content index's log since its generation's fold in memory,
  each entry's updates in order, with the runs its manifest named when it read the generation; a
  lookup applies an entry's updates to its newest record in the runs. A merge or a retirement
  since changes no lookup's answer, so a replica reads the runs again only for a new generation, or
  when a run it names is gone from the bucket. A transaction looks up what it decides by in one
  batch, through the view it read once it held the index's lock. Scans of the whole index —
  compaction's, fsck's, dictionary training's, a deletion's — merge every run with the log, the
  pinned kind's records first. What a replica holds of a generation is its log to a place, which
  within the generation only grows — so it holds nothing that changes — and every read refreshes it
  first, from one snapshot: the log rows committed after its place, or a new generation's runs and
  log — what it reads is what is new; no news of another's commit is waited for or trusted. The log's rows are read in one statement, a seek into each of its 16 shards
  from the place (on Spanner an apply join over the shards: `shard IN UNNEST(...)` read each shard
  from its start). A transaction reads the log with the generation it follows (*Writing it*), or,
  holding the index's lock, knows the generation from the lock, which no fold moves while held,
  and reads the log alone. Readers together share one refresh: one that began after a reader asked
  serves it, so a busy repository's readers cost each replica a refresh at a time, not one a read (`ok_scm_index_refresh_wait_seconds`, `ok_scm_index_refresh_seconds`). Files'
  manifests and sizes, which a file's ID fixes, and a publish's first look at what the repository
  holds — which its touch, under the index's lock, checks again — read the replica's index as it
  is, refreshing only for what it lacks; chunks' locations, which compaction moves, a transaction,
  and garbage collection read it refreshed. A file
  or chunk it lacks is looked for again after a refresh — it may have committed since — and is
  absent after that; a read whose pack was retired refreshes before it locates its chunks again. A
  network's chunk index names packs by number, which a replica resolves as it needs them — just
  those numbers, and keeps them: a number is one pack's for good, and a network writes a pack a
  publish.
- **Checking it.** `ok scm fsck` verifies every pack of the repository's network against the chunk
  index's locations in it, every file the repository holds against its ID, and that the repository
  holds every chunk of its files; with `-repair`, it rebuilds the location of a chunk a file uses
  that the network lacks one of, from the pack that holds it, and records held a chunk the
  repository's files use. It downloads each pack once and
  verifies, while that copy is in memory, every file whose first chunk the pack holds, reading
  from the bucket only a file's chunks in other packs: verifying a repository reads its packs, not
  a request per file.

### Dictionaries

A small file compresses badly alone: zstd learns from what came before, and a 3 KB file has
nothing before it. A **dictionary** gives it a past (zstd's training; RFC 8878 §5). Measured on
the monorepo's files, one trained on half of them, at zstd's highest level, made the other half's
files to 4 KiB store 26% smaller, to 16 KiB 17%, and to 64 KiB 9%; against how publish stores them
(level 3, no dictionary), 28%, 22%, and 18% — 21% of all small files' bytes.

- **One per network, trained by GC.** A network's dictionaries are trained by one of its
  repositories, its first (`network.trainer_id`). Its pass trains one when the network has none,
  or its newest is older than 90 days, from up to 8,000 of the files of 64 B to 16 KiB it has
  pinned — never work published but not uploaded — those whose IDs follow a random point, which,
  IDs being uniform, is a uniform sample read through the primary key whatever the repository's
  size. A repository with fewer than 1,000 such files trains none.
  Dictionaries are 110 KiB at most, kept in the `dictionary` table, and never deleted: a newer one
  joins the older, which still decodes the chunks that name it.
- **Compaction compresses with it.** A small chunk (64 KiB or less) compaction copies is re-encoded
  with the newest dictionary, when that shrinks it (`codec` 2), at klauspost/compress's
  `SpeedBetterCompression`: each chunk is one `EncodeAll`, which resets the encoder, and the highest
  level's reset with a dictionary copies its large tables — measured, 4.5 ms a small chunk against
  37 µs, for 0.2% less output. Training is at the same level, for the same reason: it encodes each
  sample to fit the dictionary's tables, 30 s for 8,000 samples at the highest level against about a
  second, for a dictionary that encodes 0.05% smaller. A pack holding at least 16 small chunks the
  newest dictionary has not yet considered is rewritten for them, within the pass's 1 GiB; each pack
  records the dictionary that considered it (`pack.dictionary_id`), and one with fewer is marked
  without being rewritten, so no pass counts its chunks again until a newer dictionary arrives.
- **Named by the frame.** A chunk's zstd frame names its dictionary, 32768 plus the row's ID (RFC
  8878 leaves IDs from 32768 below 2^31 to private use), so a stored chunk carries everything
  needed to decode it within its network: an ID is its network's — another network, which may
  have moved in from another cell, can hold the same — so every lookup, and the decoders the SCM
  keeps, are by network and ID.
- **Clients ask for it.** `ReadFiles` sends such a chunk as it is only to a client that sets
  `dictionaries`, which fetches the dictionary with `ReadDictionary`, and keeps it beside what it
  decodes; to any other, raw. A dictionary holds pieces of the files it was trained on, so a
  repository may read only those it trained and those it was granted: a fork, or a sync, grants
  what its parent may read then, no newer, all trained on content the fork holds (*Forks*). A chunk
  compressed with a dictionary the reader may not read is sent raw even to a client that asks for
  dictionaries, and `ReadDictionary` refuses it. A dictionary is never trusted: what decodes with it is checked
  against its ID like any chunk.

### Forks

A fork is a repository in its parent's network (hosted.md, "Fork networks, as built"):
`Store.Fork` compacts the parent's tail until everything it holds pinned — `main` and its history
with it — is in segments, then in one transaction makes the fork, its `main` its parent's, and
records in its manifest a reference to each of the parent's live segments of the kinds it inherits:
objects, trees' sizes, copies' edges, the commit graph and bodies, the content index, path
history, and main's log. CLs, events, operation logs, and workspaces start empty, and the parent's
work never uploaded is not inherited. Forking writes a few rows and one per segment, whatever the
parent's size.

- **Copy on write.** Segments are immutable, and a fork's inherited ones are never merged; a
  merge retires only the parent's references. A segment object leaves the bucket only when no
  row in the network names it, live or retired within its grace, and repacking and chunk
  collection are the network's. So nothing a parent does — landing, compaction, merging,
  repacking, collection, fsck — changes what a fork reads.
- **History up to the fork.** A fork's main's history follows its parent's up to a place
  (`repository.parent_seq`): where it forked, or last synced. The inherited segments of path
  history and main's log are read only up to there (`segment.bound`) — the parent may have
  compacted later landings into the same segments — so `Log`, `Annotate`, last commits, and
  whether a commit is on main see the fork's main alone. Its own landings continue from that place.
- **Isolation.** Each side reads only what its own presence holds: a fork's commits and files are
  never readable through its parent, nor the parent's later ones through the fork before a sync.
  A publish asks for the chunks the repository lacks, never what the network stores, so no client
  learns what another repository holds; a chunk the network stores already keeps its location.
- **Deleting** a repository (`Store.DeleteRepository`, the storage half) deletes its row and tail
  in one transaction, clears its forks' parent links — each keeps everything it inherited — and
  hands the network's dictionary training to its oldest other repository; then it releases every
  chunk the repository held, retires its segment references, deletes its content index, and
  collects the network's releases. The last repository's deletion deletes its network: every
  object under `nets/<network id>/`, and its rows. A deletion cut short is finished by the
  repository's purge (`server.Purge`), which the transaction that deleted its row marked.
- **Syncing** (`Store.Sync`) compacts the parent's tail as forking does and takes the parent's
  live segments in place of those taken before. A fork that has landed nothing of its own since
  moves its `main` to its parent's; one that has keeps its `main`, its history bounded as before,
  and can read, and land, its parent's later work.

## ChangeLists in the SCM

### Who owns what

| SCM (the write path) | ok control (review) |
|---|---|
| CL identity: repository, ID, number, change ID | reviewers |
| the author (the principal who first uploaded the change) | LGTM and Approval grants |
| status (`draft`, `open`, `submitted`, `abandoned`) and the transitions between them | comments, threads, resolution, and porting across patchsets |
| patchsets: commit, base, uploader | the `approved` projection and the live approval evaluation (OWNERS read through `Browse` at the head it then lands against) |
| stack parent | suggested reviewers |
| landing: candidates, the landed commit, the submitter, and `main` | the change log (its own events plus the SCM's feed), the Checks tab, and automation |
| rollbacks: the inverse change, and what a rollback CL rolls back | who may roll back, and a rollback's expedited review |

ok control is a privileged SCM caller. It invokes `Transition`, `PrepareCandidate`, and `Land`
on behalf of a human it has already authorized under `changelists.md`'s rules (the author, or a
repository admin where the lifecycle allows one), and the SCM records that human as the actor.
Who counts as a repository admin is ok control's to define; the SCM has no notion of it.
Clients never call those RPCs.

### Upload

`Upload(repository, commits bottom-first, readiness, reviewers)` validates the whole stack before
it writes anything. The commits must already be stored; `jj cl` publishes before it uploads. One
bad commit rejects the upload, naming the commit, the reason, and the fix. For each commit, in
order:

1. It has exactly one parent, a resolved root tree (no conflicts), a non-empty description, and a
   non-empty diff against its parent.
2. **Its parent is known.** The parent must be:
   - an ancestor of `main`;
   - an earlier commit in the upload (so one upload can carry several stacks); or
   - the current patchset of an open or draft CL, whose row the upload locks for the rest of the
     transaction.

   A parent that is an older patchset, or the patchset of a CL that landed by splice, is
   refused with a hint to run `jj sync`; one that is an abandoned CL's patchset says to restore
   that CL. "An ancestor of `main`" is one probe: `main` is linear, so its commits are the root
   and every landed commit (`head_log`'s new heads).
3. **The CL, found by (repository, change ID):**
   - **None exists:** create one. The caller becomes the author, and it starts as a draft, or
     open with `ready`.
   - **One exists and the commit is its current patchset:** unchanged, whoever uploads it.
   - **Otherwise it is a new patchset:** the caller must be the CL's author ("you can't upload to
     someone else's CL"), and its status must be draft or open. A closed CL refuses uploads; an
     abandoned one says to restore it first.
4. The upload holds at most one commit per change ID. Two would mean a divergent change.
5. **Every configuration file the commit edits is valid.**
   - **OWNERS files**, per `docs/owners.md` "Validation at upload", with the parser and
     validator in `internal/owners`, a pure-Go package that both roles import. The
     unknown-user check asks ok identity whether a handle names a person.
   - **Workflow files**, per `docs/automation.md` "Trust": what needs no build graph (the
     schema, the includes, the placeholders), with the validator in `internal/workflow`,
     likewise a pure-Go package both the SCM and automation import. The rest is checked in pre.
   - **`//secret`**, in a repository that keeps secrets or a commit that starts keeping them
     ([secrets.md](secrets.md), "The SCM"): the tree's rules (`internal/secrets`), every set an
     `acl.yaml` it changes names known to ok identity, every pointer it changes a version ok
     secrets keeps at that path, and every secret it adds, changes, or deletes in a folder `main`
     has changed by one of the folder's writers there — the last two asked of ok secrets
     (`Versions`). Refused as `INVALID_SECRETS`, every problem listed.

   Validators read the uploaded tree straight from the store.
6. **Every reviewer names a principal**, given as a handle or `<handle>@<email domain>`. ok
   identity's directory is asked about reviewers and OWNERS users once per upload, before its
   transaction; a handle is known when `<handle>.human.<domain>` is a person who is not deleted.

If every commit is unchanged, the upload is refused with "no new changes". Otherwise one
transaction does all of the following:

- creates the CLs and patchsets;
- sets each CL's stack parent (the CL whose patchset is its parent commit, or none);
- applies `ready` (draft → open) and `wip` (open → draft) to the CLs the upload creates or gives
  a new patchset. Every such CL is the caller's (rule 3), so these are always the author's to use;
- pins the new patchset commits (see "Garbage collection");
- emits one `uploaded` event per created or updated CL (with its resulting status and the
  reviewers to assign), and a `transitioned` event for each status that `ready` or `wip`
  changed.

Every patchset write takes the CL's row lock, so an upload, a transition, and a landing on the
same CL are strictly ordered. An upload locks every CL it can touch — its changes' and its
parents' — in number order. A new CL gets a random ID, which keys it and its patchsets inside the
SCM, and a number, by which everything outside knows it — monotonic in the order the uploads that
make CLs commit, with no gap (see "Sequencing"). On PostgreSQL the number comes from the
repository's own counter row (`changelist_counter`), taken after those locks, just before the
event counter; on Spanner the repository's sequencing pass gives it just after the upload commits,
and the upload answers with it. A landing locks the repository row first and CL rows after, so
an upload never takes the repository row, and the two never wait on each other in a cycle. Two
uploads creating the same change's CL at once are decided by the change ID's uniqueness: the
loser runs again and finds the CL.

### Lifecycle

`Transition` applies `changelists.md`'s lifecycle table exactly:

| Transition | From | To |
|---|---|---|
| request review | draft | open |
| return to draft | open | draft |
| abandon (optional reason) | draft or open | abandoned (the previous status is remembered) |
| restore | abandoned | the status it was abandoned from |

Each transition takes the CL's row lock, refuses a CL outside its from-set, and emits an event.
Submitting is `Land`.

### Rollbacks

`Rollback(repository, number, actor, reason, reviewers)` is ok control's, for the person who
asked (`docs/automation.md`, "Rollbacks"). It creates a CL that rolls back a landed CL:

- **Secrets.** In a repository that keeps them, a rollback that would change a secret is refused
  (`NOT_WRITER`) unless the person rolling back writes its folder on `main`, as ok secrets says.
- **What it rolls back:** the CL, and every landed CL above it in its stack — one with a patchset
  uploaded stacked on it, or on one above it (each patchset records the CL it was stacked on when
  it was uploaded) — so `main` never keeps a stack's top without its bottom. A CL that has not
  landed is refused, and so is one an open or draft rollback CL already rolls back.
- **The inverse change.** Each landing L, from its parent B on `main`, is undone from the topmost
  down: landing's splice run backwards, `splice(P = L, C = B, H)`, onto `main`'s head. A path
  whose value on `main` is neither L's nor B's — `main` changed it after the landing — is **left**
  as `main` has it, with everything under it, and so is a path the result would make both a file
  and a directory. The landings are undone again until the paths left are all they leave, so no
  path is half rolled back. A rollback that would change nothing is refused: `main` has none of
  the CLs' changes, or `main` changed every path they changed.
- **The rollback CL** is a new commit on `main`'s head — a new change ID, the result tree, the
  actor as author and committer — and a new CL: the actor's, open for review, its first patchset
  that commit, with the reviewers ok control names and the authors of the CLs it rolls back
  assigned (its `uploaded` event carries them). Its description's
  title is "Roll back CL n: ‹its title›", then the reason, then each landing it undoes. The CL
  records what it rolls back: the CLs, the paths its first patchset rolled back, and the paths it
  left, which are rolled back by hand in a CL stacked on it (`jj cl get <n>`, then
  `jj new cl/<n>@ok`), reviewed like any change, landing with it through Submit Stack.

### Events

Every SCM transaction that changes a repository or a CL writes its events in the same
transaction, for the repository's log, where each has its place, `repository_seq`: the log runs
1, 2, 3, … with no gap, in the order the transactions commit, and two repositories' writes never
wait on each other (hosted.md, "The per-repository log"; "Sequencing" below says how each backend
places them). Events are never pruned. A placed event is pending until the relay — every replica
runs one — publishes it on the bus's stream (topic `scm.events`, keyed by repository, numbered by
`repository_seq`) and forgets it. On PostgreSQL each relay takes pending events with `FOR UPDATE
SKIP LOCKED`, so relays beside it take others, and deletes them in the transaction that took them
once they are published, so one that dies first leaves them for the next. On Spanner a relay reads
pending events without a lock, takes those no relay holds for a lease in a transaction that reads
them by key, publishes them, and deletes them by key: its scan holds nothing every placement
writes. Once events are placed, the store signals `ok_scm_events` on the bus (docs/hosted.md,
"Signals"), which every replica listens on and which wakes its relay; the relay also looks every
second in case a signal was lost. `Events.Read(repository, after)` reads a repository's log from a
place in it, a page at a time: what a follower of the stream fills a gap with.

ok control, ok automation, and ok's deployer follow the stream, every replica of each a
member of the role's group, which shares the repositories among them (`internal/bus/follow`;
hosted.md, "Consumers"). Each keeps a cursor per repository — the place in its log of the last
event applied — and applies an event once: one at or before the cursor is a duplicate, and one
after a gap waits for `Events.Read` to fill it. As it starts, a follower makes its group, so
what is published from then reaches it, then catches up every repository whose log went past its
cursor (`Events.Heads`) — events published before the group was, or while the follower was away
longer than the stream keeps them. ok control follows to keep its repository and CL projections current (for the finder and
the change log), to assign reviewers passed with an upload, to re-evaluate approval when a
patchset arrives, and to re-derive every active CL's approval when a landing edits OWNERS.
`Transition`, `Rollback`, and `Land` return the last event they wrote, by its place in its
repository's log, so ok control can wait until its follower has applied its own action before it
renders the page that follows.

### Sequencing

A repository's CL numbers and its log's places are each given once, in the order its
transactions commit: two orders every request of a repository shares. Where they are given is
the backend's.

- **On PostgreSQL**, in the transaction, from the repository's counters (`changelist_counter`,
  `repository_event_counter`), the last locks it takes: a row lock is waited for, never aborted,
  and the statements after it take microseconds, so writers queue on the counters briefly, and
  the counter's order is commit order.
- **On Spanner**, after the transaction commits. Spanner aborts the younger of two transactions
  that meet on a row and holds every lock to commit, so a row every request of a repository writes
  served one request at a time, aborting the rest: at a working day's peak, half of a monorepo's
  uploads. So a transaction leaves what it makes pending (`sequence_pending`), keyed by its commit
  timestamp, a random tie of its own, and its place in it: each CL it makes, numbered meanwhile
  by its ID negated — unique as numbers are, so it keys the CL and names it as a parent until
  then — and each event, its CLs named so. Just after it commits, the writer runs the
  repository's **sequencing pass**, a transaction of a few statements: it reads the pending rows
  without a lock, in commit order, takes those still pending by key, gives the CLs the next
  numbers and the events the next places — the CLs and patchsets stacked on a new CL, and the
  events naming one, theirs too — and deletes them. A strong read sees every row committed before
  it, and one committed after carries a later timestamp, so each pass gives after every earlier
  one, whichever replica ran it. One pass of a repository runs at a time in the installation,
  holding the repository's claim (`sequence-pass` keyed work), each serving every writer that
  committed before it began, so under load one pass sequences many writes. As it ends, a pass
  records when it began (`sequence_pass`, never moving back) and says so (`sequenced/<repository>`,
  `internal/wait`): a writer whose repository another replica is passing waits on that, in an
  inbox registered before it reads the record again, and a pass that began after it committed, by
  its replica's clock less a margin (100 ms), serves it; a writer whose rows a wider skew left
  pending waits for another, and one whose pass's holder died, for its claim to lapse. A replica's own passes, nine at once, met
  on the rows and counters they took: a herd of 3,000 uploads waited two minutes for its numbers.
- **A request answers once its pass has run**, with its CLs' numbers and its events' places,
  however many replicas' passes ran meanwhile: a writer finds its events by its tie
  (`event.txn`). A provisional number is never shown outside the SCM: a read that meets one runs
  the repository's pass first and reads again, and the API refuses a number below 1.
- **Nothing is left unsequenced.** A writer's transaction that leaves rows pending enqueues, as it
  is about to commit, a sequence job naming the repository and its tie, due five seconds on: the
  replica that leases it runs the repository's pass if the transaction's rows are pending still —
  its writer stopped between its commit and its pass — and otherwise does nothing. No replica looks
  for pending rows. A move's freeze runs the passes of the network's repositories, so its
  export carries nothing pending.

## Submit: path-level landing

### The rule

For a resolved tree T, let `flat(T)` map every leaf path (a file or symlink) to its full value:
file ID, executable bit, and copy ID, or symlink ID. Directories exist only implicitly, as path
prefixes.

A CL's patchset commit C, with base (parent) P, changes the paths
`Δ = { p : flat(P)[p] ≠ flat(C)[p] }`. It lands on the head H when:

- **No path in Δ changed on `main` behind it.** For every p in Δ, `flat(H)[p]` equals `flat(P)[p]`
  (untouched since the base) or `flat(C)[p]` (already carries the CL's value).
- **The result is a tree.** In `R = flat(H)` with each p in Δ set to `flat(C)[p]` (a deletion
  removes it), no path is both a leaf and a prefix of another leaf. This catches a CL adding file
  `a/b` while `main` added directory `a/b/`, and the reverse.

The landed tree is R. Directories left empty vanish, as they do in jj. The implementation walks P,
C, and H in parallel and descends only into subtrees whose IDs differ, so the cost is proportional
to the change, not to the repository.

This is why landing never lands anything unreviewed. Every file on `main` after a landing is
either byte-for-byte the file in the approved patchset or byte-for-byte what `main` already had.
When two CLs change the same file, the second one's author runs `jj sync` (jj's content merge,
locally, with first-class conflicts) and re-runs `jj cl`. Grants carry across patchsets, so that
costs a re-upload, not a re-review.

### Landing

ok control checks a CL's approval against OWNERS read at a specific head, and automation's
landing queue checks what landed since post ran at a specific head too. `Land` makes that head
part of the contract: the caller passes the head it validated against as `expected_head`, and a
landing onto any other head is refused (`HEAD_MOVED`). The caller then re-validates against the
new head and calls again. Only automation's landing queue lands (`docs/automation.md`,
"The landing queue"): ok control, which submits to it, dry-runs a landing for its Submit
button, so `main` moves only once post passes. Validation is a few reads, so under contention the retry costs milliseconds, and nothing
can land against checks made at a head that no longer exists.

`Land(repository, [(number, patchset, candidate?)] bottom-up, expected_head, actor, dry_run)`
plans the landing from one snapshot, then writes it in one database transaction:

1. **Plan: check.** It reads the head H, refusing the landing unless H is `expected_head`, and
   each CL, checking in order:
   - the CL is open (a draft is refused with "request review first");
   - its current patchset is the one named (a patchset uploaded mid-submit refuses the landing);
   - its stack parent is submitted, or earlier in this call;
   - its patchset's root tree is resolved;
   - a named candidate was prepared from this CL at this patchset.
2. **Plan: compute each landed commit L, in order**, applying the rule against the running head:
   - **The candidate was prepared on the running head:** L is the candidate, byte-identical to
     what post tested.
   - **P is the running head:** L is C itself. What was reviewed is what landed, down to the
     commit ID.
   - **Every p in Δ already carries C's value on `main`:** refused as already on `main`.
   - **Otherwise:** L is a new commit with:
     - parents `[running head]`, predecessors `[C]`, root tree R (resolved), and conflict labels
       resolved to `""`;
     - C's change ID, description, and author;
     - committer set to the actor's signature at landing time;
     - no signature.

   It reads what it needs a batch at a time, whatever the number of CLs: every commit the landing
   starts from in one read, every candidate in one, and the trees of every CL's change and of its
   splice onto H a level of all of them to one read. Trees and commits never change, so the plan
   reads them outside the transaction that holds `main`; the transaction holds `main` only while it
   writes. A landing of 32 CLs that read each tree a round trip at a time, inside the transaction,
   held `main` for 352 of them on staging, and 685 from a replica that held none of the trees.
3. **Write and publish**, unless this is a dry run. One transaction locks the repository row
   (`FOR UPDATE`, which serializes landings per repository) and each CL's, and checks that `main`
   is still at H and each CL still as the plan read it — open, at the patchset and stack parent
   it read; a CL that changed is planned again, which says why it does not land. It then writes L's
   new trees and commits (a splice creates no file contents, so landing never touches the object
   store), advances `main` from H to the last L, appends to `head_log` (one row per L), marks each
   CL submitted with its landed commit and submitter, indexes each L's changed leaf paths for
   `Log`, drops the landed CLs' pending candidates, pins everything, and emits a `landed` event per
   CL and a `head_moved` event per L — each kind of row in a statement of its own for every CL, so
   the transaction is a few round trips however many CLs it lands.

   One transaction holds what it writes: on Spanner, 80,000 mutations (each column a row writes,
   each index entry). The plan counts what its rows cost against that, with room to spare, and
   writes the path history of as many L's as fit — in order, and no more than 20,000 paths
   (`LandingPaths`). Each other L is recorded in `path_index`; the compaction its pins mark due
   writes its paths, 10,000 to a transaction, before it moves the tail, and until then every read
   of main's path history answers L from its trees — the entry at a path in L's tree and its
   parent's, and below it only to the first difference — reading `path_index` before the rows, so
   no answer falls between the two. A landing whose other rows alone are past the budget is refused
   (`TOO_LARGE`), and the landing queue lands its CLs one at a time.

A refusal names every conflicting path. Nothing is written, and `main` does not move. There is
nothing to swap back: the head and the CL's status change in one transaction.

- **A dry run** is the plan alone: it performs every check and computes every L, then writes
  nothing and reports what would land or why not. It takes no locks, so it never delays a landing. It is the one function behind both Submit and the Submit button's
  disabled-state tooltip (`changelists.md` "Submit"), so they cannot disagree.
- **Landing a whole stack** in one call is the same loop. ok control decides when to offer it,
  and the landing queue lands it.

Because the SCM carries a CL's change ID onto `main`, `jj sync` can recognize the landing as a
rewrite of the local change instead of a divergent copy of it.

### Candidates for post-submit automation

`docs/automation.md` runs post jobs on the exact commit that would land and advances `main`
only after they pass.

- **`PrepareCandidate(number, patchset, base, actor)`** computes that commit exactly as `Land`
  would onto `base`, with the same checks. When `base` is the patchset's own base the candidate
  is C itself; otherwise it is the spliced commit, committed by the actor at preparation time.
  The base is a `main` commit, or another candidate when automation speculates on a conflict
  group (`main` + A + B); any other base is refused (`UNKNOWN_BASE`). A stacked CL's parent must
  be submitted or among the CLs of the base's candidate chain. The candidate is written,
  recorded, and kept alive as a GC root while it is pending: until it is discarded, or its CL
  lands.
- **`Land` with a candidate.** If the running head is still the candidate's base, the landed
  commit is the candidate: byte-identical to what post tested. If `main` moved, the landing queue
  checks that nothing landed since conflicts with it (automation.md, "The landing queue") and
  lands with the new head as `expected_head`. The SCM then re-splices under the same rule — the
  same tree post tested, when only what does not conflict with it landed.
- **`DiscardCandidate`** drops a candidate that will not land.
- **`ListCandidates`** lists a repository's pending candidates, with who they were prepared for
  and when: automation discards any it no longer tracks (one prepared just before it failed).

Landing needs no merge engine, only tree splices, so it stays in the milliseconds automation.md's
landing pipeline assumes.

## Garbage collection and compaction

Built in `internal/scm/internal/store` (`gc.go`, `compact.go`, `touch.go`) and run by
`ok scm` itself (see "Running it"). Nothing a repository holds is deleted any other way:
repositories themselves are never deleted (see "Decisions").

- **Pinning.** Everything reachable from `main` or from any CL patchset (of any status: comments
  anchor to old patchsets, and an abandoned CL can be restored) is permanent. Upload and landing
  set `pinned` on each newly reachable commit, tree, file, symlink, copy history, and chunk,
  stopping at objects already pinned, so the cost is proportional to what is new. An upload finds
  what it will pin before its transaction, from one snapshot — a commit's graph is immutable, and
  pinned is permanent — and its transaction pins that a kind at a time, a statement or two each,
  rather than a level of the graph at a time. A pin that finds an object missing fails the
  transaction instead of pinning a dangling graph; a landing whose splice wrote an object it found
  already stored, which a sweep then took, runs again and stores it anew.
- **Checkouts** (*Checkouts*) are derived, not history: the pass deletes the checkouts of commits
  other than main's head older than 14 days, and the layers no kept checkout names after a day.
- **Operation-log retention follows jj.** Every operation reachable from an operation log's heads
  is kept, with its view and `commit_predecessors`. That is jj's own semantics, where any past
  state can be restored, and it matches CitC's snapshot history. A user sheds history with
  `jj op abandon` followed by `jj util gc`.
  - **Settled content is pinned** (`settle.go`). An operation or view untouched for 15 days (the
    collection window and a day) is settled: compaction moves it into a segment, and first pins
    what it reaches — a view's heads, bookmarks, tags, and working copies, an operation's commits
    and their predecessors — so that content leaves the database's rows with it, and no pass walks
    it again. What an operation abandoned within the window reached is still collected; what one
    abandoned after it settled reached stays, as pinned objects do. A repository's settled
    operations from before compaction pinned their content are pinned by its next pass, once
    (`op_content_pinned`).
  - **`CollectOperations(heads, keep_newer)`** deletes operations and views that are unreachable
    from both the given heads and every current `op_head` row, and whose `touched_at` (server
    time) is older than `keep_newer`. A head another machine published concurrently is therefore
    never collected, whatever the caller passed.
  - Reachability over the whole log is computed from a snapshot without a lock. Then, under the
    operation log's row lock — which every `Publish` carrying operations also takes, so
    collection and publishing never interleave — only what was published since is walked: from
    the current heads, through operations the snapshot did not reach. The lock is held for the
    new operations, not the log's history.
  - Re-publishing an existing operation or view (undo and restore produce identical views)
    touches it.
  - An operation log of no workspace — the shared log a principal's workspaces had before each
    had its own — is one no request can reach, and garbage collection deletes it whole: its
    operations, views, and heads, and with them the commits only they kept. So is a workspace
    okfs recorded then, with no log of its own, which no clone can use: with its lease and
    checkpoints.
  - The server runs it daily with a 14-day cutoff, matching the default of `jj util gc`, for the
    operation logs that need it: only heads that moved other than forward — the new head not an
    operation whose parents are every old head, as `jj op abandon` makes — leave operations nothing
    reaches. Such a log (`diverged_at`) is collected each day until a collection past the window
    after its divergence has taken everything it left (`collected_through`); a log whose heads
    only moved forward is never walked. A log from before these were recorded is collected so,
    once.
- **Freshness is transitive and quantized.** `touched_at` holds a day (the database's
  `current_date`, UTC), not an instant.
  - **The walk.** Touching an unpinned object sets its `touched_at` to today — a file's or a
    chunk's day in the content index — and recurses into each unpinned object it references
    whose day is older than today. It recurses only
    into rows it actually updated, so an unpinned object is never fresher than anything beneath
    it.
  - **A touch never commits over a missing object.** A transaction whose walk reaches a
    reference with no row fails: `Publish`'s commit refuses with a `PublishRejection`, and its
    exchange drops the held roots that reach one, touches the rest again, and asks the client
    for what it dropped. No row is ever left fresh above a missing one.
  - **Who touches.** `Publish` touches every staged object and file the server already holds,
    every pre-existing object its new objects reference — the chunks a new file reuses among
    them — and every object it sends that its inserts find already stored (sent again because a
    sweep took part of its graph). A candidate (`PrepareCandidate`) *refreshes* its commit: it
    sets every unpinned object reachable from it to today, recursing through all of them,
    because its new rows are fresh by default but reuse older ones.
  - **Cost.** Once an object has been touched today, touching it again stops at once. So a user's
    unpinned work is walked at most once a day, not on every command, and re-using an old subgraph
    costs one walk of it.
  - **Deadlocks.** A publish aborted by a deadlock between two concurrent publishes is retried.
- **Mark and sweep, per repository.** Candidates are the unpinned rows whose `touched_at` is more
  than 15 days old (14 days, plus one day for the quantization), listed from partial indexes
  over unpinned rows, and the content index's unpinned files and chunks as old; a repository
  with none skips the mark.
  - **Roots:** every commit referenced by a view or an operation the tail holds of the
    repository's operation logs (a view's heads, working-copy commits, and ref targets; an
    operation's `commit_predecessors`), read a page at a time — a settled one's, in a segment, is
    pinned — every pending candidate, and the trees of the checkpoints not pinned. A checkpoint
    kept past its first week (*Workspaces*) has its trees pinned by the next pass, which records
    it (`trees_pinned`): the mark walks only the recent ones. Before a repository's settled
    content is pinned, every operation and view is a root.
  - **Mark:** traverse the unpinned objects reachable from the roots, stopping at pinned ones and
    at ones touched within the cutoff (their subgraphs are no staler, so hold no candidate).
    Commits are walked in one recursive query over `commit_parent`; trees, files, and chunks
    breadth first.
  - **Sweep:** delete the unmarked candidates — commits, then trees and the other objects, then
    files, then chunks, so a row a reader finds rarely points at one already gone — in batches.
    Each `DELETE` re-checks `pinned` and `touched_at` under the row's lock; files and chunks are
    dropped from the content index under its lock, re-checked there.
  - A pass is proportional to what was written within the windows — the tail's operations and
    views, the last week's checkpoints, the logs that diverged — not to the repository's history;
    the object walk, to the stale, unpinned objects still reachable.
- **Why no reachable object is ever swept.** An object reachable when the mark ran is marked. One
  that became reachable afterwards got there through a publish, an upload, a landing, or a
  candidate:
  - **A publish** touched it and everything unpinned beneath it, so their `touched_at` is today,
    well inside any sweep cutoff. A sweep that re-checks after that commit leaves them alone. A
    sweep that deleted one first makes the publish's touch find it missing, and the publish fails
    instead of committing a dangling reference.
  - **An upload or a landing** pinned it, which the sweep also re-checks.
  - **A candidate** refreshed it, likewise.
  - A view published after the mark read its roots references only commits that publish
    touched; a view that existed then was a root.
- **Compaction and repacking.** Packs the content index has no chunk in are retired outright.
  Packs more than half dead — bytes no chunk in the index is, beyond what a pack of just its live chunks
  spends on its header, record headers, and index — and the small ones (under 16 MiB) when there
  are two or more, and those with small chunks the repository's newest dictionary has not yet
  considered (*Dictionaries*), are rewritten into packs of
  up to 256 MiB holding only their live chunks, copied as stored (each verified against its ID),
  but for small ones the dictionary shrinks, which are re-encoded with it. New packs order chunks
  by the path on `main` of a file that holds them, then their offset in it, for read locality;
  chunks no file on `main` holds follow, by ID. One pass rewrites at most 1 GiB of source packs in a
  repository; the rest waits for the next. It reads only their live records, 32 MiB of an output at
  a time, with the range reads the read path makes (records close together in a source in one GET,
  eight GETs at once), and nothing touches the server's disk.
  1. The new pack is written to the object store as it is made: uploaded under a temporary key in
     the network's packs (`staging/…`), which a server-side copy names once its hash is known —
     and which collection deletes as an orphan should the writer stop first.
  2. One transaction moves its chunks to it in the content index — each only if the index still
     has it where compaction read it. Once every new pack is written, the source packs left with
     no chunk are retired.
  3. Retired packs are deleted from the object store an hour later, then their rows. A read that located a
     chunk in a pack before it moved still reads it, and one that finds the pack gone locates
     the chunk again.

  Packs in the object store with no `pack` row (a publish or compaction that failed after uploading) are
  deleted after a day.
- **Every pack is unique.** A pack begins with 16 random bytes (see "Storage"), so no two packs
  share an ID or a key even when they hold the same chunks. Nothing ever writes a deleted pack's
  key again, which is what makes deleting retired and orphan packs safe beside publishes.

### Running it

Every `ok scm` replica runs the GC node, and each repository's collection is keyed work
(`internal/work`; hosted.md, "Work without leaders"), done by one replica at a time — whichever
takes it — and by every replica between them, a pass at a time on each. Passes are safe to
overlap — every deletion re-checks under row locks, or the content index's — so the lease only
keeps replicas from doing the same work twice; a replica that loses it stops. No replica looks for
work: each repository's collection is booked — a day on, by the transaction that makes the
repository; then by each run, for the soonest of its next daily pass, a failed pass's retry, and the
end of its network's retired packs' and segments' grace — and its job brings it then. A
repository's collection:

- runs its pass, when it is due one: once a day, from `scm.gc_hour` (UTC, default 9). A pass
  collects the operation logs, marks and sweeps, moves the tail's pinned rows into segments, however
  few (*Segments*), compacts packs, and deletes the repository's retired and orphan packs and
  segments;
- otherwise deletes its retired packs past their hour and segments past their day;
- and books its next.

The repositories a release before this one collected by a sweep are booked once, by the first
replica of this release (`schedule`, keyed work `schedule`): each for now if it is due a pass, and for
its next daily slot otherwise.

Every replica runs the segments node too: each repository's compaction into segments, keyed work
marked by every pin, two at a time on a replica.

Each pass is recorded in `gc_pass` — what it collected, swept, compacted, and deleted, or the
error that ended it; a failed pass is tried again an hour later. `ok scm gc [-dry-run]
REPOSITORY` runs a pass by hand, and `-dry-run` reports what one would do without changing
anything.

## Authorization

Who may call each service is declared in the protocol (`ok.v1.callers`) and enforced by ok's
serving runtime before the service sees a request. What a person may do to a repository,
Authorize decides (hosted.md, "Roles and actions"), as it does for a robot that is none of ok's
roles: every request a person or such a robot makes names its repository — at its top, in its lease, or in a publish's first message — and each method a person
may call does one action to it: reading, browsing, workspaces, and publishing to one's own
workspace need read (a workspace's operation log is on the SCM, so every jj command a reader runs
records there), and `Upload` needs the repository's proposing role. A repository the person cannot
see answers as though there were none (`NOT_FOUND`); an action they may not take answers why
(`PERMISSION_DENIED`). A method given no action is refused to all but ok's roles.
The facts are ok identity's (`Authorization.Facts`), kept five seconds (`authz.Checker`). ok's own
roles act for a person they checked, and are not checked again. A person makes repositories the directory lets
them make (their own, and their organizations'), becoming its creator; ok's roles make none for
themselves. Deleting is the directory's, and the SCM's storage goes when the directory purges a
repository (hosted.md, "Deleting, renaming, transferring"). "Other people can't write to your CL" is enforced by
`Upload`, because a CL's author is fixed at creation.

The SCM records every principal by stable ID — a repository's creator, a CL's author, uploader,
and submitter, a candidate's preparer, and the principals its events name — and presents each by
name too, as the directory names them now, so a rename shows everywhere at once (hosted.md,
"Tenancy, as built"). A request that acts for someone names them by ID (`actor_id`); a reviewer
is named by handle, as people write it, and recorded by ID. The commits the SCM writes sign with
the principal's name when it writes them, as commits are text.

| Service | Callers | What they do |
|---|---|---|
| `Repositories`, `Store`, `ChangeLists`, `Browse`, `Workspaces` | people, robots, ok control, ok automation, ok's workers, ok's deployer | people and robots: what Authorize lets them do to each repository; ok's roles: every repository. A person makes repositories the directory lets them (becoming their creator); publish to their own workspaces' operation logs (a request names the workspace; the principal is the caller's identity, never named in a request); `Upload` CLs (becoming their author), and patchsets, `ready`, and `wip` to their own CLs only |
| `Review` | ok control | for a person it authorized (`actor_id`, their stable ID): `CreateRepository` (who becomes its creator), `Transition`, and `Rollback` |
| `Landing` | ok automation | `PrepareCandidate`, `DiscardCandidate`, `ListCandidates`, and `Land`, for the submissions ok control hands it (the landing queue): `main` moves only once post passes |
| `LandingChecks` | ok control, ok automation | `DryRunLand`: ok control's Submit button checks a landing, changing nothing |
| `Events` | ok control, ok automation, ok's deployer | `Read`, a repository's log; `Heads`, how far each reaches |

## Protocol

The protocol is `proto/ok/scm/v1/scm.proto` (package `ok.scm.v1`), the one source both sides
compile against: the server's Go stubs, generated beside it, and the client's Rust stubs, which
`jj`'s build generates. It defines the nine services above — each with its callers — and the
rejections a `FAILED_PRECONDITION` status carries in its details (`PublishRejection`,
`UploadRejection`, `LandRejection`). Objects travel as `EncodedObject`: their `ObjectKind` and
canonical body, whose kind values are the encoding's kind bytes. Tests on both sides hold those
values to the encoding.

gRPC status maps onto jj's errors on the client, per store:

| Failure | `Backend` | `OpStore` | `OpHeadsStore` |
|---|---|---|---|
| `NOT_FOUND`, or a `not_found` result | `BackendError::ObjectNotFound` | `OpStoreError::ObjectNotFound` | — |
| `PERMISSION_DENIED` | `BackendError::ReadAccessDenied` | `OpStoreError::ReadObject` | `OpHeadsStoreError::Read` |
| a failed `Publish` (in `update_op_heads`) | — | — | `OpHeadsStoreError::Write` |
| anything else | `BackendError::Other` | `OpStoreError::Other` | `OpHeadsStoreError::Read` |

Writes never fail at the store call itself, because they only stage into the outbox. A write
failure surfaces when the transaction publishes.

## Schema

The SCM keeps its tables in the installation's PostgreSQL, or in a Spanner database of its own
(`database.type: spanner`; `docs/hosted.md`, "The SCM on Spanner, as built"), and its packs in its
object store. Its schema is ordered migrations embedded in `ok` — PostgreSQL's SQL
(`internal/scm/store/postgres/migrations`), Spanner's DDL (`internal/scm/store/spannerstore/migrations`),
the same tables under the same names — applied when the SCM starts, under the schema's migrate
lock (`internal/lock`), so replicas starting together apply each once; a database newer than the
binary is refused. The tables:

- **`repository`** (name, creator, `head` and `head_seq` for main) and **`head_log`**, every move
  of main, one row per landed commit.
- **Content, per repository:** `object` (symlinks, copy histories, and the trees landings make,
  as canonical bodies — a publish's trees are in its pack, *Storage*), `commit` (body, change ID, generation, conflict flag) with its `commit_parent` edges,
  `copy_parent` (the reverse index `GetRelatedCopies` walks), `commit_path` (the paths each
  commit on main changed, and the directories above them, for `Log` and `ReadTree`), `path_index`
  (landings whose paths are yet to be written, *Landing*), and
  `tree_sizes` (each tree's sizes record once read, but a workspace's, *Tree sizes*; a sweep takes
  it with its tree).
  Objects, sizes records, copy edges, and commits pinned since compaction last ran are the tail of
  what `segment` records (*Segments*).
- **File contents:** `pack` (an S3 pack's size, retirement, the dictionary compaction considered
  it with, and its number), `dictionary` (each repository's zstd dictionaries, *Dictionaries*),
  and the content index — `content_index`, `content_log`, and its runs in `segment` (*The content
  index*) — which holds each file's and chunk's pinning and freshness too. Objects and commits
  carry GC's `pinned` and `touched_at`.
- **Operation logs:** `op_log`, one per workspace — (repository, principal, workspace) — holding
  `operation`, `view`, and `op_head` rows. Operations and views carry `touched_at`.
- **ChangeLists:** `changelist`, `patchset`, and `candidate`, and `changelist_counter`, each
  repository's next CL number in a row of its own (see "Upload" for why it is not the
  repository row).
- **Events:** `event`, keyed by repository and `repository_seq`, the place in its log its
  `repository_event_counter` row's lock serializes in commit order; `event_pending`, the events the
  relay has yet to publish. A repository's counter row is made by its first event (a declaration
  holds no data), as its `changelist_counter` row is by its first CL.

Two planner decisions are part of the schema:

- **Indexes beyond the keys** serve the reads that walk the other way: children by parent
  (`commit_parent_parent`, `copy_parent_parent`), commits by change ID, chunks by pack (fsck and
  repacking), file chunks by chunk, operations by view, patchsets by commit, `head_log` by new
  head ("is this commit on main"), CLs by status and by author (the CL lists), and candidates by
  CL.
- **No statistics on the tenant keys** (`repository_id`, and `op_log_id` on the operation-log
  tables): declared as each column's statistics target, 0 (`statistics.sdl`). A value `ANALYZE`
  has not seen is otherwise estimated at one row, so for a new or freshly imported repository,
  or a new principal's log, the planner costs a scan of the whole tenant the same as a
  unique-key lookup, and can pick the scan for every probe of an ancestry walk. Without
  statistics the key's estimate is the fixed default, and a full-key lookup always wins.

## ok control

ok control is the web UI and the review process, a role of its own: the SCM owns storage and the
CL lifecycle, control owns review and the web, and each scales on its own. It reads code through
`Browse`: comment porting's two diffs are `DiffTrees` and `DiffFiles`, OWNERS are read with
`ReadFileAt` at `main`'s head, and the recent-editors query is `Log` by path. Its tables hold
review and a projection of the SCM's repositories and CLs that follows `Events`.

### Product decisions

1. **Open access.** Every repository is readable by everyone signed in, and anyone may upload CLs
   to it; there are no collaborators, organizations, or teams. A repository's creator, and ok's
   administrators, administer it: they may drive any of its CLs' lifecycle.
2. **One line of history.** A repository has `main` and nothing else: no branches, no tags.
3. **Markdown only.** Markdown is the one markup ok renders; every other file shows as source or,
   when it is an image, as the image.
4. **Rename, never transfer.** Repositories are renamed; they belong to nobody, so there is nothing
   to transfer.
5. **No importer.** A repository from elsewhere comes in by being created on ok and having its
   current tree uploaded as one CL: it starts fresh, without its history. A tree that is not a
   Bazel workspace comes in **imported** — no `.ok/repository.yaml`, or `build: none` in it — and
   ok runs nothing on it until a change makes it one.

