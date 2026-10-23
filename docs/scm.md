# ok's SCM: a jj-native source control service

The SCM is ok's source of truth for code: repositories, their objects and operation logs, `main`,
and ChangeLists. This is its design. Related: `docs/identity.md` (who callers are).

## What this is

ok is built of roles on one runtime, and a client; three matter here:

- **The SCM** (`internal/scm`) is the source of truth for code. It is a Go gRPC service
  that implements jj's storage interfaces server-side: content-addressed objects (file contents in
  packfiles in object storage, every other object in PostgreSQL), one operation log per workspace — each
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
2. **Multi-repository, keyed by repository id.** Repositories have flat, unique names, belong to
   nobody, and are all public (no private repositories). A name is 1–64 lowercase letters,
   digits, `.`, `_`, and `-`, starting and ending with a letter or digit; `assets`,
   `repositories`, `healthz`, and `readyz` are reserved, because ok control serves each
   repository at `ok's address/<name>` beside those pages.
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
   other object and all state live in PostgreSQL.
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
    zstd with the repository's dictionary, which is kept beside them and marked used whenever it
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
  `<ok.web-url>/<repository>/cl/<n>` (`ok.web-url` defaults to ok's public URL). A
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

Replicas are stateless: all coordination is in PostgreSQL, and file contents are in a private
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
   - writes the new chunks as one pack to the object store;
   - runs one PostgreSQL transaction that:
     - locks the caller's `op_log` row whenever the publish carries operations, views, or a heads
       update;
     - inserts the pack, chunk, file, object, commit, operation, and view rows;
     - touches every pre-existing object the new ones reference, and every operation and view it
       re-sends;
     - applies the heads update: insert `new_id`, delete each old ID other than `new_id`;
   - replies `done` with the resulting heads.

A failure anywhere leaves nothing visible. A publish whose references don't resolve fails with
`FAILED_PRECONDITION`, and a `PublishRejection` in the status details lists the missing IDs. A
pack uploaded by a publish that then failed has no `pack` row and is deleted as an orphan.
Operations and views land in the operation log of the caller's workspace the request names, and
only there: a workspace's views name only its own working copy, and its operations only itself
(`INVALID_ARGUMENT` otherwise).

### Reads

- **Every object except file contents** is read from PostgreSQL, behind an in-memory cache of
  decoded objects. Objects are immutable, so the cache is never invalidated.
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
  repository's dictionary as they are (`CHUNK_CODEC_ZSTD_DICT`) and fetches each dictionary its
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
  record, and a tree's deletion cascades to it. Trees no client reads virtually never get one.
- **Kept where trees are.** A record is keyed by its tree's ID and stored beside the trees, so
  it moves with them when trees leave PostgreSQL for packs (`this document`,
  "Sequencing"); its bytes are self-describing — kind byte `0x20`, a version, then LEB128
  varints, about two bytes an entry where a tree spends about 75 — and held to one encoding by
  golden vectors both languages pass (`internal/scm/encoding/testdata/tree_sizes.json`).
- **On the wire.** `Store.ReadTreeSizes` answers up to 100,000 trees in one call, and
  `ReadTreeClosure` with `with_sizes` sends each level's records after its trees
  (`OBJECT_KIND_TREE_SIZES`), so a virtual clone or a presentation receives every directory's sizes
  with its trees, in one stream.
- **Trust.** A record is derived, so it cannot be verified against its tree's ID; it is as trusted
  as a manifest's size, which was always the SCM's word until the contents were read and verified.
  The client checks a record's entry count against its tree and ignores one that differs.

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
then the last of each day for a year; a workspace's latest always) and treats every kept
checkpoint's tree as a root.

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
| `Export` | a checkout of a commit (optionally under path prefixes) as one stream; with `entries_only`, its entries (paths, file IDs, sizes, modes) without contents, so a worker that keeps a checkout reads (`ReadFiles`) only what changed | automation's workers |

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

Packs live at `repos/<repository id>/packs/<pack id hex>` in the `ok scm` bucket. A pack holds
chunks of one repository only, so GC stays per repository. The pack ID is the BLAKE3 of the
pack's bytes, and the nonce makes every pack written unique (version 1 packs, written before
it, have none and are read the same way).

```
magic      "OKSCMPAK"                        8 bytes
version    u32 = 2
nonce      16 random bytes
records    repeated:
  chunk id       32 bytes
  codec          u8     0 = stored, 1 = zstd, 2 = zstd with a dictionary
  raw length     u32
  stored length  u32
  data           stored-length bytes
index      u64 count, then (chunk id, u64 offset of its record), sorted by chunk id
footer     u64 offset of the index, magic "OKSCMEND"
```

Chunks are stored with zstd (level 3) unless compression doesn't shrink them; compaction later
compresses small ones with the repository's dictionary (*Dictionaries*), which the frame names.
The records and index make a pack self-describing, so `ok scm fsck` can verify every pack
against the content index (hashes, locations, lengths) and rebuild its chunk entries from the object store; a
chunk compressed with a dictionary is verified with it.

### The content index

What the SCM knows of a repository's files and chunks — each file's size and chunks, each chunk's
pack, offset, lengths, and codec, and for both whether it is pinned and the last day anything
wrote or referenced it — is its **content index** (`internal/scm/internal/contentindex`), held
compactly in PostgreSQL rather than as rows per file and chunk; it is ok scm's only record of
them:

- **Blocks.** `index_block` holds the index of one generation (`content_index.generation`) as
  blocks of up to 256 entries in ID order — a small file costs about 120 bytes, its file entry
  and its chunk's. Each pack is named by its number (`pack.num`), not its 32-byte ID.
- **The log.** Every transaction that changes a file or chunk — a publish storing one, a walk
  touching or pinning one, a sweep dropping one, compaction moving one — writes what it changed
  as one `content_log` row in the same commit, and notifies `the SCM's database_content`. Log rows apply
  in the order they were written: two transactions that change one entry are ordered by its row
  lock, the second logging after the first committed. Puts, touches, and pins also commute, so
  applying one twice changes nothing.
- **Writing it.** A transaction that relies on what the index holds — a publish deciding what
  is already stored, the touch, refresh, and pin walks — takes the index's row lock in share mode
  (`content_index ... FOR SHARE`) and reads the index after it, once; its own puts, touches, and
  pins count for the rest of it. A sweep's drops and compaction's moves take the lock
  exclusively, and decide after it too. So a writer never commits against an entry a sweep
  dropped, and a sweep never drops what a writer touched, pinned, or relied on first; writers run
  beside each other, their changes commuting. Two publishes storing one new chunk at once both
  log it, and the index keeps the copy logged first: the other is dead bytes compaction reclaims.
- **Rebuilds.** Each garbage collection pass ends by folding the index into the next
  generation's blocks: the blocks and the log rows one snapshot sees, with entries that have only
  a touch or a pin dropped; log rows written since stay. A new repository starts with an empty
  index.
- **Compaction works from it.** Which chunks each pack holds, how many of its bytes are live,
  and which packs are drained come from the index; a chunk moves only if the index still has it
  where compaction read it. Chunks are ordered by the path on `main` of a file that holds them
  (the path hint a publish sends goes unused).
- **Reading it.** Each replica holds a repository's whole index in memory (about 10 MB for
  a monorepo of 50,000 files), loaded the first time it reads the repository, and every file, manifest, size, and
  chunk location it serves comes from there. A repository's index is marked stale when this
  replica commits a change to it or another's commit is announced, and the next read refreshes
  it first, from one snapshot: the log rows it lacks, or the whole of a new generation. A file
  or chunk it lacks is looked for again after a refresh — it may have committed since — and is
  absent after that; a read whose pack was retired refreshes before it locates its chunks again.
- **Checking it.** `ok scm fsck` verifies every pack against the index's chunks in it, and
  every file's chunks against its ID; with `-repair`, it rebuilds the entries of chunks a file
  uses that the index lacks, from the packs that hold them.

### Dictionaries

A small file compresses badly alone: zstd learns from what came before, and a 3 KB file has
nothing before it. A **dictionary** gives it a past (zstd's training; RFC 8878 §5). Measured on
the monorepo's files, one trained on half of them, at zstd's highest level, made the other half's
files to 4 KiB store 26% smaller, to 16 KiB 17%, and to 64 KiB 9%; against how publish stores them
(level 3, no dictionary), 28%, 22%, and 18% — 21% of all small files' bytes.

- **One per repository, trained by GC.** A pass trains one when the repository has none, or its
  newest is older than 90 days, from up to 8,000 of its files of 64 B to 16 KiB — those whose IDs
  follow a random point, which, IDs being uniform, is a uniform sample read through the primary
  key whatever the repository's size. A repository with fewer than 1,000 such files keeps none.
  Dictionaries are 110 KiB at most, kept in the `dictionary` table, and never deleted: a newer one
  joins the older, which still decodes the chunks that name it.
- **Compaction compresses with it.** A small chunk (64 KiB or less) compaction copies is
  re-encoded with the newest dictionary at zstd's highest level, nothing waiting on it, when that
  shrinks it (`codec` 2). A pack holding at least 16 small chunks the newest dictionary has not yet
  considered is rewritten for them, within the pass's 1 GiB; each pack records the dictionary that
  considered it (`pack.dictionary_id`), and one with fewer is marked without being rewritten, so no
  pass counts its chunks again until a newer dictionary arrives.
- **Named by the frame.** A chunk's zstd frame names its dictionary, 32768 plus the row's ID (RFC
  8878 leaves IDs from 32768 below 2^31 to private use), so a stored chunk carries everything
  needed to decode it.
- **Clients ask for it.** `ReadFiles` sends such a chunk as it is only to a client that sets
  `dictionaries`, which fetches the dictionary with `ReadDictionary` — read only through its
  repository, since it holds pieces of the repository's files — and keeps it beside what it
  decodes; to any other, raw. A dictionary is never trusted: what decodes with it is checked
  against its ID like any chunk.

## ChangeLists in the SCM

### Who owns what

| SCM (the write path) | ok control (review) |
|---|---|
| CL identity: repository, number, change ID | reviewers |
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
parents' — in number order; a new CL's number comes from the repository's own counter row
(`changelist_counter`), taken after those, just before the event counter. A landing locks the
repository row first and CL rows after, so an upload never takes the repository row, and the
two never wait on each other in a cycle. Two uploads creating the same change's CL at once are
decided by the change ID's uniqueness: the loser runs again and finds the CL.

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
transaction, in the repository's log: each takes the next place in that log from the
repository's own counter row (`repository_seq`), whose lock is held until commit, so the log's
order is commit order and two repositories' writes never wait on each other (hosted.md, "The
per-repository log"). The counter is the last lock a transaction takes, just before commit, so it
never joins a deadlock and holds the order for as short a time as possible. Events are never
pruned. The same transaction marks the events pending, and the relay — every replica runs one —
publishes them on the bus's stream (topic `scm.events`, keyed by repository, numbered by
`repository_seq`) and forgets them: each takes pending events with `FOR UPDATE SKIP LOCKED`, so
relays beside it take others, and deletes them in the transaction that took them once they are
published, so one that dies first leaves them for the next. The transaction also notifies the
PostgreSQL channel `ok_scm_events`, delivered at commit; each replica holds one connection
listening on it, which wakes its relay, and the relay also looks every second in case a
notification was lost with that connection. `Events.Read(repository, after)` reads a
repository's log from a place in it, a page at a time: what a follower of the stream fills a gap
with.

ok control, ok automation, and ok's deployer follow the stream, every replica of each a
member of the role's group, which shares the repositories among them (`internal/bus/follow`;
hosted.md, "Consumers"). Each keeps a cursor per repository — the place in its log of the last
event applied — and applies an event once: one at or before the cursor is a duplicate, and one
after a gap waits for `Events.Read` to fill it. As it starts, a follower makes its group, so
what is published from then reaches it, then catches up every repository whose log went past its
cursor (`Events.Heads`) — events published before the group was, or while the follower was away
longer than the stream keeps them. A follower of the feed that numbered every repository's
events together before (`Events.Watch`, now gone) leaves it once, seeding each repository's
cursor from where the feed had reached: each event it numbered keeps its number (`event.seq`),
and `Events.Heads` answers as of one. ok control follows to keep its repository and CL projections current (for the finder and
the change log), to assign reviewers passed with an upload, to re-evaluate approval when a
patchset arrives, and to re-derive every active CL's approval when a landing edits OWNERS.
`Transition`, `Rollback`, and `Land` return the last event they wrote, by its place in its
repository's log, so ok control can wait until its follower has applied its own action before it
renders the page that follows.

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
runs one PostgreSQL transaction:

1. **Lock and check.** It locks the repository row (`FOR UPDATE`, which serializes landings per
   repository) and reads the head H, refusing the landing unless H is `expected_head`. It then
   locks each CL's row and checks, in order:
   - the CL is open (a draft is refused with "request review first");
   - its current patchset is the one named (a patchset uploaded mid-submit refuses the landing);
   - its stack parent is submitted, or earlier in this call;
   - its patchset's root tree is resolved;
   - a named candidate was prepared from this CL at this patchset.
2. **Compute each landed commit L, in order**, applying the rule against the running head:
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
3. **Write and publish**, unless this is a dry run. It writes L's new trees and commit (a splice
   creates no file contents, so landing never touches the object store). It then advances `main` from H to the
   last L, appends to `head_log` (one row per L), marks each CL submitted with its landed commit
   and submitter, indexes each L's changed leaf paths for `Log`, drops the landed CLs' pending
   candidates, pins everything, and emits a `landed` event per CL and a `head_moved` event per L.

A refusal names every conflicting path. Nothing is written, and `main` does not move. There is
nothing to swap back: the head and the CL's status change in one transaction.

- **A dry run** performs every check and computes every L, then writes nothing and reports what
  would land or why not. It takes no locks, so it never delays a landing. It is the one function behind both Submit and the Submit button's
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
  stopping at objects already pinned, so the cost is proportional to what is new. A pin walk
  that finds an object missing fails the transaction instead of pinning a dangling graph; a
  landing whose splice wrote an object it found already stored, which a sweep then took, runs
  again and stores it anew.
- **Operation-log retention follows jj.** Every operation reachable from an operation log's heads
  is kept, with its view and `commit_predecessors`. That is jj's own semantics, where any past
  state can be restored, and it matches CitC's snapshot history. A user sheds history with
  `jj op abandon` followed by `jj util gc`.
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
  - The server runs it daily for every operation log with a 14-day cutoff, matching the
    default of `jj util gc`.
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
  - **Roots:** every commit referenced by a view or an operation of any of the repository's
    operation logs (a view's heads, working-copy commits, and ref targets; an operation's
    `commit_predecessors`), and every pending candidate.
  - **Mark:** traverse the unpinned objects reachable from the roots, stopping at pinned ones and
    at ones touched within the cutoff (their subgraphs are no staler, so hold no candidate).
    Commits are walked in one recursive query over `commit_parent`; trees, files, and chunks
    breadth first.
  - **Sweep:** delete the unmarked candidates — commits, then trees and the other objects, then
    files, then chunks, so a row a reader finds rarely points at one already gone — in batches.
    Each `DELETE` re-checks `pinned` and `touched_at` under the row's lock; files and chunks are
    dropped from the content index under its lock, re-checked there.
  - Reading every retained view and operation is proportional to the repositories' retained
    history, like jj's own `jj util gc`; the object walk is proportional to the stale, unpinned
    objects still reachable.
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
  chunks no file on `main` holds follow, by ID. One pass reads at most 1 GiB of source packs in a
  repository; the rest waits for the next.
  1. The new pack is written to the object store.
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

Every `ok scm` replica runs the GC node; the one holding its GC lock (a row in PostgreSQL)
does the work, stopping between repositories if it loses the lock. Passes are safe to overlap — every deletion re-checks under row locks, or the content index's — so the
lock only keeps replicas from doing the same work twice. Every ten minutes the holder:

- deletes the retired packs past their hour, in every repository;
- runs a pass over each repository due one: once a day, from `scm.gc_hour` (UTC, default
  9). A pass collects the operation logs, marks and sweeps, compacts, and deletes the
  repository's retired and orphan packs.

Each pass is recorded in `gc_pass` — what it collected, swept, compacted, and deleted, or the
error that ended it; a failed pass is tried again an hour later. `ok scm gc [-dry-run]
REPOSITORY` runs a pass by hand, and `-dry-run` reports what one would do without changing
anything.

## Authorization

Who may call each service is declared in the protocol (`ok.v1.callers`) and enforced by ok's
serving runtime before the service sees a request. The SCM checks no caller against a list; it
identifies each caller by principal, to act as it. Repositories are public, and any person may
create or rename one; the SCM records each repository's creator. No one may delete a repository:
the protocol has no way to (see "Decisions"). "Other people can't write to your CL" is enforced by
`Upload`, because a CL's author is fixed at creation.

| Service | Callers | What they do |
|---|---|---|
| `Repositories`, `Store`, `ChangeLists`, `Browse`, `Workspaces` | people, ok control, ok automation, ok's workers, ok's deployer | read every repository; create repositories for themselves (becoming their creator) and rename any; publish to their own workspaces' operation logs (a request names the workspace; the principal is the caller's identity, never named in a request); `Upload` CLs (becoming their author), and patchsets, `ready`, and `wip` to their own CLs only |
| `Review` | ok control | for a person it authorized (`actor`): `CreateRepository` (who becomes its creator), `Transition`, and `Rollback` |
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

The SCM keeps its tables in the installation's PostgreSQL and its packs in an S3 bucket. Its
schema is ordered SQL migrations embedded in `ok` (`internal/scm/store/migrations`), applied when
the SCM starts, under an advisory lock, so replicas starting together apply each once; a database
newer than the binary is refused. The tables:

- **`repository`** (name, creator, `head` and `head_seq` for main) and **`head_log`**, every move
  of main, one row per landed commit.
- **Content, per repository:** `object` (trees, symlinks, and copy histories as canonical
  bodies), `commit` (body, change ID, generation, conflict flag) with its `commit_parent` edges,
  `copy_parent` (the reverse index `GetRelatedCopies` walks), `commit_path` (the paths each
  commit on main changed, and the directories above them, for `Log` and `ReadTree`), and
  `tree_sizes` (each tree's sizes record once read, *Tree sizes*; it goes with its tree).
- **File contents:** `pack` (an S3 pack's size, retirement, the dictionary compaction considered
  it with, and its number), `dictionary` (each repository's zstd dictionaries, *Dictionaries*),
  and the content index — `content_index`, `index_block`, and `content_log` (*The content
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

