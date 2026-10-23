# Hosted ok — one service, a million accounts

This is the plan for running ok as a hosted, multi-tenant service — a GitHub competitor — sized for
**a million accounts, each with a 1 TB repository**, without giving up the single-installation ok
that runs today. It says what breaks at that size and why, the shape that replaces it, the
interfaces every backend sits behind (data, object storage, events, work queues), and the order the
work lands in. Nothing here is built yet; each phase below is a series of CLs.

Related: [scm.md](scm.md) (the storage and protocol this reshapes), [identity.md](identity.md)
(principals, groups, credentials), [automation.md](automation.md) (pre, post, the landing queue,
workers), [deploy.md](deploy.md), [configuration.md](configuration.md).

## The target

| | |
|---|---|
| Accounts | 1,000,000, each with a repository of 1 TB |
| Logical bytes | 1 EB (10^18) |
| Stored bytes | 330–670 PB (source compresses about fivefold, *Storage*; a 1 TB repository is mostly binaries, so 1.5–3× is assumed) |
| Chunks | 1.5 × 10^13 (FastCDC, 64 KiB average); 1.5 × 10^7 per repository |
| Content index | ~0.9 PB at ~60 bytes an entry; ~0.9 GB per repository |
| Tree versions, commits | ~8 × 10^12 and ~10^12 (a repository of 10^7 files and 10^6 commits, ~8 new trees a commit) |
| Packs | ~2.6 × 10^9 objects at 256 MiB each |
| Publishes | ~2,300 a second on average, ~12,000 at peak (200 jj transactions a day per account) |
| okfs checkpoints | ~20,000 a second at peak (a tenth of accounts active, one checkpoint every five seconds) |
| Ingest | 1 PB a day, ~93 Gbit/s (1 GB a day per account) |
| CI reads | ~250 PB a day, ~2.9 TB/s (50 runs a day per repository, 5 GB read each) |
| CI runs | ~580 a second on average |

What follows from the numbers:

1. **Writes are not the problem; bytes are.** Split over a hundred cells (*Cells*), a cell sees ~23
   publishes a second and ~1 Gbit/s of ingest. Storage and reads — CI's above all — dominate.
2. **Immutable data cannot live in database rows.** 10^13 tree versions, 10^12 commits, and a
   petabyte of content index would cost ~85 Spanner nodes for the index alone at full utilization
   (10 TiB a node), and no PostgreSQL holds them. Content-addressed data moves to object storage;
   the database keeps what changes. What remains is ~50 MB a repository: ~50 TB in all, ~500 GB a
   cell.
3. **Reads must not pass through ok's servers in bulk.** 2.9 TB/s comes from caches near workers and
   from object storage directly, on URLs ok signs; ok's servers decide, they do not carry.
4. **Nothing may be global but the directory.** Every ordering, lock, leader, and queue in ok today
   is one per installation; each becomes one per repository, or disappears.

## What breaks today

ok was built as one installation for one organization: one PostgreSQL, one bucket, one domain of
names, one trust domain. These are the points that do not survive the target, each with where it is
and what replaces it.

| Choke point | Where | Why it breaks | Replaced by |
|---|---|---|---|
| One global event counter | `scm.event_counter`, whose row lock every repository- or CL-changing transaction takes (scm.md, "Events") | Every write in the installation serializes on one row | A sequence per repository ([Events](#events-and-the-bus)) |
| One cursor per consumer | `feed_cursor` in control, automation, and deploy, each following the whole feed in order | One process applies every repository's events, one at a time | Cursors per repository, applied with compare-and-set ([Consumers](#consumers)) |
| Leader-elected singletons | `LockFn` leaders: automation `feed`, `buildcache`, `queue`; control `feed`; deploy `leader`, `feed`, `observer`; scm `gc`; identity `sync` | One replica does each kind of work for the whole installation | Work leased per repository, per organization, or per shard ([Work without leaders](#work-without-leaders)) |
| Immutable data in rows | `object`, `tree_sizes`, `commit`, `commit_parent`, `copy_parent`, `commit_path`, `index_block`, `operation`, `view`, `event` | Trillions of rows; the content index alone is a petabyte | Segments in object storage, a tail in the database ([Segments](#segments-an-lsm-per-repository)) |
| One database | `internal/pgdb`: every role's schema in the installation's PostgreSQL | One machine's I/O and storage | A database per cell, behind interfaces, PostgreSQL or Spanner ([The data layer](#the-data-layer)) |
| `LISTEN`/`NOTIFY` | 16 sites, automation's queue and every feed | PostgreSQL only; one connection per channel per replica | The stream and queue interfaces ([Events and the bus](#events-and-the-bus)) |
| One run queue | `run_queued`: `FOR UPDATE SKIP LOCKED` over every queued run, one priority order | No fairness between tenants; one index every worker contends on | Admission per organization, dispatch queues per worker pool ([Scheduling](#scheduling-and-fairness)) |
| Logs in the database | `log_chunk`, moved to the bucket when a run finishes | A live run's every line is a row | Logs written to object storage as they stream ([Logs](#logs-secrets-and-caches)) |
| Finder by `LIKE` | `ListRepositories`: `name LIKE '%k%'` with `OFFSET` over every repository | A scan of a million rows per keystroke | A search service ([ok control](#ok-control-at-scale)) |
| Cross-repository indexes | `changelist_author`, `reviewer_principal` | "My CLs" spans every repository, so every shard | An inbox per principal ([ok control](#ok-control-at-scale)) |
| One namespace, all public, no deletion | `repositoryName` (`internal/scm/store/repository.go:30`); scm.md, "Authorization" | A hosted service needs owners, privacy, roles, and deletion | [Tenancy](#tenancy) |
| One domain of names | `<handle>.human.<domain>` (identity.md) | People belong to many organizations; signing up is self-service | Accounts and organizations ([Tenancy](#tenancy)) |
| One trust domain for workers | bubblewrap on shared workers (automation.md, "Execution") | Strangers' code beside each other's checkouts and caches | A microVM per run, caches per trust domain ([Automation](#automation-at-hosted-scale)) |
| One cluster for deploys | the deployer drives the installation's own Kubernetes (deploy.md, "Where it runs") | Customers deploy to their own clusters | Agents in customers' clusters ([Deploy](#deploy-at-hosted-scale)) |
| Sequential keys | `GENERATED ALWAYS AS IDENTITY` on `run`, `plan`, `comment`, `deployment`, `checkpoint`, `pack.num`, … | Spanner puts every new row of a monotonic key on one server | Random 63-bit IDs ([Shared primitives](#shared-primitives)) |
| Random routing to caches | the SCM's three in-memory caches, any replica serving any repository | Hit rates fall as replicas grow | Affinity by repository ([Reading](#reading-at-3-tbs)) |

## The shape

```
                     people, jj, okfs, workers, agents
                                   │
                    ┌──────────────▼───────────────┐
                    │ edge (stateless, every region)│  TLS, authentication, rate limits,
                    │  owner/repo ─▶ cell           │  routing by repository
                    └───────┬──────────────┬────────┘
                            │              │
          ┌─────────────────▼──┐        ┌──▼───────────────────────┐
          │ global directory    │        │ cell (one region, ~10^4   │ × ~100
          │ accounts, orgs,     │        │  repositories)            │
          │ names ▸ (repo, cell)│◀──────▶│ ok scm · control ·        │
          │ grants, tokens,     │ stream │ automation · deploy       │
          │ sessions, inboxes,  │        │ data store ▸ PG or Spanner│
          │ search              │        │ object store ▸ S3 or GCS  │
          └─────────────────────┘        │ stream + queue ▸ PG, NATS,│
                                         │   Pub/Sub, SQS+SNS        │
                                         │ workers (microVMs), caches│
                                         └───────────────────────────┘
```

Principles:

1. **The repository is the unit of everything** — placement, ordering, leases, garbage collection,
   landing. Nothing ok does needs two repositories at once, except a fork network's storage, which
   is placed whole.
2. **The database holds what changes; object storage holds what does not.** Content-addressed data
   is written once, as segments and packs; the database names the live ones and holds the recent
   tail.
3. **Every backend sits behind an interface with one conformance suite**, run against every
   implementation: the data store, the object store, the stream, the queue.
4. **The bus delivers; the database decides.** Every event is first a row in the repository's log,
   in the transaction that caused it. The bus is how consumers hear of it, at least once, in any
   order; a consumer applies an event exactly once by comparing against its cursor.
5. **No global singletons.** Every loop that runs on one replica today runs per repository, per
   organization, or per shard of a cell, under a lease taken on demand.
6. **One code path.** A single installation is one cell whose directory lives in the cell's database
   — today's ok, on today's backends.

## Tenancy

### Accounts, organizations, repositories

- **Accounts** are people and organizations, in one namespace of handles: 1–39 characters of
  lowercase letters, digits, and inner hyphens, unique across both, retired for good when released
  (identity.md's rule that a name never names two principals, kept). People sign up themselves, or
  are provisioned by their organization (*Enterprise organizations*).
- **Repositories** are `owner/name`, owned by a person or an organization, with a random 63-bit ID
  that never changes. Names are unique per owner; renaming or transferring leaves a redirect from
  the old name, held until that name is taken again.
- **Visibility** is per repository: **public** (anyone, signed in or not, reads), **internal**
  (every member of the owning enterprise reads), or **private** (only those granted).
- **Forks** are repositories of their own, in their parent's **fork network**, which shares storage
  (*Packs and fork networks*). A fork of a public repository may be private.

### Permissions

- **Roles on a repository:** read, triage (labels, reviewers, closing CLs), write (upload to
  branches it may, rerun checks), maintain (settings but not visibility or deletion), admin.
- **Organizations** have owners and members, a base role members get on every repository, and
  **teams** (nested), each granted roles on repositories. Outside collaborators are granted roles on
  single repositories.
- **Proposing a CL** needs read: on a public repository anyone may upload a CL, which is theirs, as
  today ("Other people can't write to your CL", scm.md); a repository may require write instead.
  **Landing** still needs what OWNERS say (owners.md) plus write for the person who submits.
- **Deciding.** One function, `Authorize(principal, repository, action)`, decides every request,
  from the repository's visibility, the owner's base role, team grants, and direct grants. Each cell
  keeps a projection of the grants on its repositories, fed by the directory's stream, so a check
  needs no call out of the cell; the edge caches the answer for a few seconds.

### Credentials and sign-in

- People sign in in a browser — with a password and passkey, with a social identity provider, or
  through their organization's single sign-on — and jj and okfs sign in as today (`jj auth login`, a
  device-certified key).
- **Tokens**: fine-grained personal tokens (named repositories, named permissions, an expiry), and
  installation tokens for apps, short-lived and scoped to one installation's grants.
- **Robots** belong to an organization: CI identities, deploy agents, apps. A robot signs in with a
  key ok certified, or federates an OIDC token from an issuer the organization trusts (identity.md's
  `identity.federation`, per organization).
- **Sessions, device logins, ceremonies** move to the directory, which every region reads.

### Enterprise organizations

An enterprise brings its own identity provider: identity.md's systems of record — OpenID Connect,
LDAP, SCIM — scoped to the organization instead of the installation, its people provisioned and
deprovisioned by it, and its groups synced as teams. The **groups-and-ACLs tree** becomes the
organization's: declared in a repository it names, approved by the ACLs that own each set, as today.

### Names

A principal has one stable ID and two presentations: hosted, `@ada`, `@acme/eng` (a team), and
`acme/ci` (a robot); single-installation, today's `ada.human.example.com`. OWNERS files, workflow
files, and every API accept the form their installation uses, so existing repositories keep working.

### Deleting

Repositories can be deleted. Deletion is soft for a retention period (30 days, restorable by an
admin), then hard: the repository's rows, segments, and presence sets go, and its packs are freed by
its network's garbage collection once nothing else in the network holds their chunks. Accounts are
deleted the same way, their names retired.

### Moving an existing installation in

An installation becomes one organization: its domain's people become its members (their accounts
linked to their identity provider's subjects), its groups its teams, its repositories its
repositories (`<org>/<name>`), its robots its robots. Content moves as a cell move does
(*Placement*).

## Cells and the global directory

### A cell

A cell is the unit of capacity and of failure: one region; one database (a PostgreSQL cluster with
replicas, or a Spanner database per role); a few buckets; a stream and queue namespace; every role's
replicas; worker pools; caches. It holds on the order of 10^4 repositories — sized by its database's
writes and its buckets' request rates, not by bytes, which object storage absorbs. A cell knows only
its own repositories; it calls the directory for nothing on a request's path.

### The global directory

The directory holds what is global, and only that: accounts, organizations, memberships, teams,
grants, tokens and credentials, sessions, names and redirects, each repository's ID and **cell**,
fork networks, inboxes (*ok control*), and the search index of repositories and people. It is small
— a few rows per account — but every region reads it and names must be unique everywhere, so it is a
multi-region Spanner database in the hosted service (a PostgreSQL database for one region, and the
cell's own database in a single installation). Its changes go out on its stream; cells and edges
keep what they need of it.

### The edge

Stateless replicas in every region terminate TLS, authenticate (a certificate or token checked
locally; sessions from the directory, cached), apply rate limits per principal and per address, and
route. Every ok RPC that names a repository carries it in a header, `ok-repository: owner/name`, set
by ok's clients, so the edge routes without parsing protobuf; pages route by path. The edge resolves
`owner/name` to (ID, cell) from a cache the directory's stream keeps current, and forwards to the
cell. Inside a cell, requests for a repository go to the same SCM replicas (rendezvous hashing of
the ID), so their caches stay hot (*Reading*).

### Placement and moving

A new repository is placed in a cell in its owner's region with room; a fork joins its network's
cell. Moving a repository (or a network) between cells — to rebalance, to change region, to bring an
installation in — copies its segments and packs (server-side copies within a provider, streamed
across), replays its tail, freezes writes for seconds while the last of the tail moves, flips the
directory entry, and lets the old cell redirect until edges have seen the flip.

### Regions and recovery

Cells are regional; the directory is multi-region. A cell's database has point-in-time recovery (a
PostgreSQL's WAL archive, Spanner's PITR) and its buckets replicate to another region, so a lost
region's cells are rebuilt elsewhere from what replicated. A repository's region can be chosen, for
data residency.

## The data layer

### Interfaces

Each role's storage becomes a Go interface in the role's terms — the SCM's (`PutTail`,
`SetOpHeads`, `AppendEvents`, `LandCandidate`, `CommitManifest`, …), automation's (`AdmitRuns`,
`ClaimRun`, `FinishRun`, …), control's, deploy's, identity's — with two implementations,
`pg` and `spanner`, and one conformance suite per interface that both pass. Domain logic —
verification, the splice, landing's rules, approval, the queue's decisions — stays above the
interface, written once.

Not a generic SQL or key-value layer: the two backends differ where it matters — Spanner's
transactions buffer mutations and abort to retry, it has no `SKIP LOCKED`, no advisory locks, no
`LISTEN`, and it wants interleaved tables; PostgreSQL wants none of that — and the hot paths want
each backend's own shape. A domain interface lets each be idiomatic and keeps the two honest with
one suite.

The SCM's store mixes its rules with its SQL today (`internal/scm/store`: 23 files, 11,600 lines,
156 query sites). Separating them is the first and largest piece of work; automation (102 sites),
identity (79), deploy (49), and control (34) are smaller.

### Shared primitives

`internal/data` holds what every store needs on either backend:

- **Transactions** — `data.Txn(ctx, fn)` runs `fn` again on a serialization failure (PostgreSQL
  `40001`, `40P01`) or an abort (Spanner `ABORTED`); `fn` holds no side effects outside the
  transaction.
- **IDs** — every surrogate key is random 63-bit (`crypto/rand`), never a sequence: repositories,
  runs, plans, comments, deployments, checkpoints, packs. Numbers people see in order — a
  repository's CL numbers — stay counters per repository, under the repository's key.
- **Time** — a transaction's commit timestamp, the database's clock, as today's leases already use.
- **Leases** — per-repository work leases and per-shard leases (*Work without leaders*), as rows
  read `FOR UPDATE` on both backends.
- **Migrations** — PostgreSQL's ordered SQL under an advisory lock, as today; Spanner's DDL batches
  through the admin API, each version recorded, guarded by a lock row.
- **Big writes** — immutable data first, in as many transactions as it takes (it is invisible until
  referenced), then one small transaction that makes it reachable. This is required on Spanner,
  whose commits hold at most 80,000 mutations and 100 MiB, and keeps PostgreSQL's transactions short
  too.

### PostgreSQL

What ok runs on today, kept for single installations and for cells that choose it, with what leaves
the database (*Segments*) and the stream and queue moved behind their interfaces. A cell's
PostgreSQL holds ~500 GB.

### Spanner

The GoogleSQL dialect through `cloud.google.com/go/spanner`, which has everything below: mutations,
read-write transactions with retries, partitioned DML, batch writes, and change streams.

- **Interleaving.** Every per-repository table is interleaved in `Repositories` (`INTERLEAVE IN
  PARENT … ON DELETE CASCADE`), so a repository's rows sit together and a transaction on one
  repository touches one split: CLs, with their patchsets and reviews interleaved in them; the event
  log; the tail; workspaces and checkpoints; operation logs, with their heads interleaved in them;
  runs and plans in automation's database. Spanner allows interleaving seven deep.
- **Keys.** Repository IDs are random, which spreads repositories over splits. Keys under a
  repository may be sequential (CL numbers, event sequence numbers). A table not under a repository
  is keyed by a random ID or, where order matters within one, a bit-reversed sequence.
- **Locking.** Reads in read-write transactions lock what they read; `SELECT … FOR UPDATE` takes
  exclusive cell-level locks for the read-then-write patterns PostgreSQL's `FOR UPDATE` serves
  today.
- **No queue in the database.** What `SKIP LOCKED` does today moves to the queue interface; what
  `LISTEN` does to the stream; advisory locks to lease rows.
- **The relay.** A change stream on the event log feeds the stream (*The per-repository log*).
  Change streams keep seven days by default; a relay that falls further behind resumes from the log.
- **Ancestry.** Walks that use recursive queries in PostgreSQL today use the commit graph segments
  instead (*Segments*), on both backends.
- **Databases.** One database per role per cell; an instance holds up to 100, so twenty cells per
  instance.
- **Tests.** The conformance suites run against the Spanner emulator
  (`gcr.io/cloud-spanner-emulator/emulator`) on every change, and against a real instance nightly.

Moving a cell from PostgreSQL to Spanner is a cell move (*Placement*): repositories move one at a
time into a Spanner cell.

## Repository storage at exabyte scale

### Segments: an LSM per repository

Everything content-addressed — trees, symlinks, copy histories, commits and their graph, the paths
each commit changed, the content index, operations and views, the event log's history — is written
once and never changed. Each repository keeps it as a log-structured merge tree:

- **The tail** is the database's: what was published recently, in the same tables as today
  (`object`, `commit`, `content_log`, `operation`, `view`, `event`), bounded.
- **Segments** are object storage's: immutable sorted files, each of one kind, with a fence index
  and a Bloom filter, under `repos/<id>/segments/<kind>/<segment id>`.
- **The manifest** is the database's: the repository's live segments by kind and level, under a
  generation number.
- **Compaction** — a job on the queue, per repository, when its tail passes a size or an age —
  writes a segment from the tail, then in one transaction adds it to the manifest (if the generation
  is still the one it read) and deletes the tail rows it folded in. Levels merge the same way.
- **Kinds.** Objects (trees, symlinks, copy histories, and trees' sizes records, scm.md "Tree
  sizes"); the **commit graph** (each commit's parents, generation, change ID, and conflict flag,
  and the reverse edges — what `commit`, `commit_parent`, and `commit_path`'s ancestry walks read,
  as Git's commit-graph does); **path history** (`commit_path`); the **content index** (which is
  already "blocks of one generation" in `index_block`: the blocks move to segments, `content_log`
  stays as the tail); **operation logs**; **events**.
- **Reads** check each kind's Bloom filters and fence indexes, kept in replica memory and on its
  disk, read the segment's block by range, and query the tail only for what no segment holds — so a
  read of anything not written in the last minutes asks the database nothing.
- **Large publishes** — an import, a vendored tree of millions of files — bypass the tail: the SCM
  writes the verified objects as segments, and the publish's transaction adds them to the manifest.

A repository's database rows are then its mutable state — `main` and its log, CLs and reviews, op
heads, workspaces and checkpoints, runs, the manifest — and its recent tail: ~50 MB.

### Packs and fork networks

- **Physical storage is per network; what a repository may read is per repository.** Packs and
  segments of chunks live under the fork network (`nets/<network id>/…`), so a fork's copy of its
  parent's content costs nothing. Each repository has a **presence set** — the files and objects it
  holds — and a read is answered only for what is in the requesting repository's set. A fork's set
  is copy-on-write: it starts as a reference to its parent's at the fork (O(1)), and each side then
  adds only to its own layer — what the fork publishes or syncs, what the parent lands. So a
  private fork's commits are never readable through the public parent, nor the parent's later
  commits through the fork until it syncs them.
- **A pack holds one repository's chunks**, as today. A fork reads its parent's packs by reference,
  for what the parent held when the fork synced.
- **Garbage collection is per network**: each repository's marking yields what it still reaches; a
  chunk goes when no repository in the network reaches it; packs are rewritten as today.

### The object store

`internal/blob`'s interface grows, with three implementations:

```go
type Store interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, o PutOptions) error // o.IfAbsent, o.Class
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	Download(ctx context.Context, key string, w io.Writer) (int64, error)
	Stat(ctx context.Context, key string) (Object, error)
	Delete(ctx context.Context, keys ...string) error
	List(ctx context.Context, prefix, after string, limit int) ([]Object, string, error) // paged
	Copy(ctx context.Context, src, dst string) error                                    // server-side
	SignGet(ctx context.Context, key string, ttl time.Duration) (string, error)         // range-capable URL
}
```

- **S3** — minio-go, as today: AWS S3, MinIO, Ceph, Cloudflare R2. Conditional writes are S3's
  `If-None-Match`; uploads past 64 MiB are multipart.
- **GCS** — native, through `cloud.google.com/go/storage`: credentials from the workload's identity
  (no HMAC keys), `ifGenerationMatch` preconditions, signed URLs, composed uploads. Today's GCS
  support goes through its S3-compatible XML API (`internal/blob/blob.go`'s note), which has none of
  these.
- **Filesystem** — a directory, for development and tests.

**Keys spread load.** S3 scales to at least 3,500 writes and 5,500 reads a second per partitioned
prefix, with no limit on prefixes; GCS starts a bucket at about 1,000 writes and 5,000 reads a
second and grows it, asking that load rise no faster than doubling every 20 minutes. So keys begin
with a short hash of the repository or network (`ab/repos/<id>/…`), a cell spreads its repositories
over a few buckets, and new cells ramp. ~2.9 TB/s of CI reads at 1 MiB a request is ~500 prefixes'
worth of S3 reads, which keys spread this way supply.

**Storage classes.** Packs no read has touched for months move to infrequent-access storage
(`PutOptions.Class` on rewrite, and lifecycle rules).

### Reading at 3 TB/s

- **Tiers.** A client's own cache (jj's, okfs's — content-addressed, scm.md "Cache, outbox, and
  prefetch"); a worker host's disk cache shared by its runs; a cache tier in each zone, holding hot
  chunks and segment blocks by ID; object storage. Every tier keys by content ID, so none is ever
  stale.
- **Direct reads.** For bulk reads — a clone, a CI checkout, a large file — the SCM returns chunk
  locations and URLs it signed (minutes long, one pack each), and the client reads the ranges from
  object storage or the zone's cache itself, verifying each chunk against its ID as it does now. The
  SCM decides who may read what; it does not carry the bytes. A URL is signed only for a pack all of
  whose chunks the reader's repository may read (*Packs and fork networks*).
- **Public repositories** are served through a CDN in front of the buckets, on the same signed URLs.
- **Affinity.** Requests for a repository go to the same few SCM replicas, so the in-memory caches
  (objects, chunks, diffs; scm.md "Reads") hold one repository's working set instead of every
  repository's.
- **CI beside the data.** Worker pools run in their cell's region and zones, so CI's reads cross no
  region and pay no egress.

### Writing

Publishing keeps its contract (scm.md, "Publish"): two round trips, the server verifying everything,
the pack written before the transaction that makes it visible. What changes: the transaction writes
the tail instead of permanent rows, locks the op log as today, and stays small; a publish too large
for one transaction becomes segments first (*Segments*). Uploads
stream through SCM replicas, which scale out — at ~1 Gbit/s of ingest a cell needs a handful —
because verification stays on the server.

### Garbage collection

Per repository (per network for chunks), as a job on the queue: a cell's scheduler — leased per
shard — finds repositories whose `gc_due` time has passed, ordered by how much they wrote since, and
enqueues them. Marking reads segments, not rows; sweeping writes new segments without what died, and
rewrites packs as today. No leader; a cell's GC throughput is its workers'.

## Events and the bus

### The per-repository log

Every transaction that changes a repository or a CL appends its events to that repository's log:
rows keyed `(repository, seq)`, the sequence the repository row's own counter, taken as the
transaction's last lock exactly as the global counter is today — but per repository, so two
repositories' writes never wait on each other. A repository's busiest writer, a monorepo landing
~500 commits a minute (automation.md, "The landing queue"), is about ten events a second.

The log is the source of truth; the bus only carries it:

- **The relay** publishes every event at least once. On PostgreSQL, the transaction also inserts the
  event's key into a pending table, which every replica's relay publishes from and clears: each
  takes pending rows `FOR UPDATE SKIP LOCKED`, publishes them, and deletes them in the transaction
  that took them, so relays never wait on one another, and one that dies first leaves its rows for
  the next — no shards or leases to keep. On Spanner, a change stream on the log is the relay's
  input.
- **History** compacts into event segments, so the log is still never pruned (scm.md, "Events"): a
  consumer that needs old events reads them from the SCM, which reads segments.

### The stream

```go
type Event struct {
	Topic string // "scm.events", "directory.events", …
	Key   int64  // the repository (or account)
	Seq   int64  // its sequence in the key's log
	Body  []byte
}

type Stream interface {
	Publish(ctx context.Context, events []Event) error
	// Subscribe delivers every event of topic to one subscriber of each group: groups fan out,
	// subscribers within a group share the load. At least once; order is not promised.
	Subscribe(ctx context.Context, topic, group string, h func(context.Context, Delivery) error) error
}
```

### Consumers

A consumer applies an event in its own transaction with its cursor for that key: `UPDATE cursor SET
seq = s WHERE key = k AND seq = s - 1`. One row updated: applied, exactly once. None, and the cursor
is at or past `s`: a duplicate, acknowledged. None, and the cursor is behind: a gap, filled by
reading `(cursor, s)` from the SCM's log (`Events.Read(repository, after)`) and applying it in order
first. So delivery may duplicate and reorder, and consumers never apply twice or skip — the same
contract today's feed has ("re-reads every 30 seconds in case a notification was lost"), per
repository instead of per installation. A new consumer bootstraps from snapshots (the SCM's current
state of each repository) rather than replaying a decade of every repository's history.

### The queue

```go
type Job struct {
	ID        string    // a duplicate ID within the backend's window is dropped
	Queue     string    // "automation.dispatch.linux-x86", "scm.compact", "scm.gc", "webhooks", …
	Key       int64     // the repository or organization, for leases and metrics
	NotBefore time.Time // delayed delivery
	Body      []byte
}

type Queue interface {
	Enqueue(ctx context.Context, jobs []Job) error
	// Lease hands out up to max jobs, each invisible to others for visibility.
	Lease(ctx context.Context, queue string, max int, visibility time.Duration) ([]Leased, error)
}

type Leased interface {
	Job() Job
	Attempt() int
	Extend(ctx context.Context, visibility time.Duration) error
	Ack(ctx context.Context) error
	Nack(ctx context.Context, delay time.Duration) error // past the queue's attempts: dead letters
}
```

Queues carry work, not truth: a job names what to do, and the worker takes the work's lease in the
database (a run's claim, a repository's work lease) before doing it, so a lost or duplicated job
costs a retry, never a double execution. Priorities are separate queues, read in order.

### Backends

| | Stream | Queue |
|---|---|---|
| **PostgreSQL** | The log's pending table and `LISTEN`/`NOTIFY`, as today's feed | A table leased with `FOR UPDATE SKIP LOCKED`, as today's run queue |
| **NATS JetStream** | A stream per topic, subjects per shard of keys, a durable consumer per group, replicated (R3) | A stream with work-queue retention and pull consumers, `AckWait` as the visibility, the duplicate window for IDs |
| **Google Pub/Sub** | A topic, ordering keys by key (1 MBps per ordering key is far above any repository's rate), a subscription per group, retention up to 31 days | A topic and pull subscription, acknowledgement deadlines extended while working, dead-letter topics |
| **AWS** | SNS topic fanning out to an SQS queue per group | SQS standard queues: nearly unlimited throughput, visibility up to 12 hours, retention up to 14 days, messages up to 1 MiB |
| **Kafka** (MSK, Confluent, Redpanda) | A topic, partitioned by key, a consumer group per group | — (Kafka has no per-message acknowledgement or visibility; an installation on Kafka takes its queue from another backend) |

The stream and the queue are configured separately (`bus.stream`, `bus.queue`), so an installation
runs on whatever its environment has: all on PostgreSQL, Kafka with PostgreSQL's queue, NATS for
both, Pub/Sub for both, or SNS and SQS.

**On SQS.** SQS is the right AWS service for the queue: it does exactly what the interface asks, at
any rate. On its own it is not a stream — each message goes to one consumer — which is why the
stream is SNS fanning out to an SQS queue per consumer group. Because consumers order and
de-duplicate by sequence themselves (*Consumers*), the stream needs neither SQS FIFO's ordering nor
Kinesis's replay; Kinesis (1 MB/s and 1,000 records a second written per shard, up to 365 days kept)
would work too, as Kafka does.

## Work without leaders

Every singleton becomes keyed work, leased on demand:

- **Per repository.** A `work(kind, key)` row (`internal/work`): the transaction that changes what
  the work acts on marks it dirty and wakes the replicas — on PostgreSQL by a notification at
  commit; where the database has none (Spanner), by a wake-up job on the bus's queue, deduplicated
  by ID where the backend can — and whichever replica takes the row's lease first clears the mark,
  does the work, and loops until it stays clean: a mark made while it works is seen as it lets go,
  under the row's lock, so none is lost. A sweep each replica runs marks what time decides, and
  work whose holder's lease lapsed is taken again. Many replicas drive many repositories; one
  drives each.
- **Per shard.** Sweeps over a cell — repositories due for GC, runs whose leases lapsed, relays —
  split the cell's repositories into 4,096 shards by ID, each shard leased by one replica,
  rebalanced as replicas come and go.

| Today | Becomes |
|---|---|
| automation `queue` (the landing queue, one replica) | Per repository: each repository's queue driven under its own lease |
| automation `feed`, control `feed`, deploy `feed` | Stream consumer groups with per-repository cursors |
| automation `buildcache` | Per organization (its cache namespace, *Logs, secrets, and caches*) |
| deploy `leader` | Per repository (its deployments) |
| deploy `observer` | Per deploy target (its agent, *Deploy*) |
| scm `gc` | Per repository and network, scheduled by shard |
| identity `sync` | Per organization (its identity provider) |

The landing queue and the feeds go first (phase 2), and the SCM's garbage collection with them.
Identity's sync and automation's build cache sweep have one key each until organizations exist
(phase 4). The deployer's leader stays one until its deploy agents (phase 10): it decides which
deployments may start from what every running one occupies, across repositories, and a split by
repository would let two replicas start two that claim one target.

## Automation at hosted scale

### Isolation

Every run gets a Firecracker microVM — on hosts with KVM, bare metal or nested virtualization — with
its own kernel, an ephemeral root, and no network beyond what its phase allows, on hosts that run
nothing else. Bazel's incrementality is kept within a trust domain only: a slot's output base and
checkout survive between runs of one repository (and, for post, only of `main`'s line); never
between repositories, never from a CL's pre into post.

### Scheduling and fairness

- **Admission.** Runs are recorded as today, queued in the cell's automation database per
  organization. A dispatcher — per shard — admits runs within each organization's quota (concurrent
  runs, minutes) by weighted fair share across organizations, post before pre before manual runs,
  older before newer, and enqueues each admitted run on the dispatch queue of the worker pool its
  job needs (Linux x86 or Arm, macOS, larger machines).
- **Execution.** A worker leases a dispatch job, claims the run in the database (compare-and-set on
  its attempt, as `Lease` does today), runs it, and extends both while it runs. A worker that dies
  lets its claim lapse; the dispatcher admits the run again.
- **Pools** scale on their dispatch queues' depth and age.
- **The landing queue** runs per repository (*Work without leaders*), so a million repositories'
  queues run side by side, each as automation.md describes.

### Logs, secrets, and caches

- **Logs** stream from the worker to object storage in segments of a few seconds, under the run; a
  live view tails the newest segment and the worker's last lines through the cell. `log_chunk` goes.
- **Secrets** — per repository and per organization — are encrypted under a key in the cloud's KMS
  (envelope encryption), and delivered only to runs whose phase may have them: post on `main` and
  deploys, never pre of a CL whose author lacks write.
- **Caches.** ok serves Bazel's remote cache (the Remote Execution API's content store and action
  cache), backed by object storage and the zone's cache tier, namespaced per organization; a CL's
  pre from outside the organization reads it but writes only to a namespace of its own, so no one
  poisons another's results.

## ok control at scale

- **Per cell, as now.** Its projections — repositories, CLs, reviewers, grants, comments, events —
  are keyed by repository and live in the cell, fed by the cell's stream.
- **Inboxes.** "My CLs", "Needs my review", and notifications span repositories and so cells: an
  inbox per principal in the directory — a row per (principal, repository, CL, relation) — kept by
  consumers of every cell's stream. Pages read the inbox, then each CL from its cell.
- **Search.** Repositories, people, and organizations are found through a search interface:
  Spanner's full-text search in the hosted directory, PostgreSQL trigram indexes in a single
  installation. Code search is a trigram index per repository, built on landing by a job, kept as
  segments, and served by search replicas with affinity, as the SCM's are.
- **Webhooks and notifications** are queue work: per-endpoint retries with backoff, signatures, and
  dead letters.

## Deploy at hosted scale

A customer's cluster runs ok's **deploy agent**, a robot of the customer's organization, which
connects out to its cell — no inbound access, no credentials to the cluster held by ok — and applies
what the deployer sends: push, migrations, roll, watch (deploy.md, "A deployment"), as the deployer
does in-cluster today. Deploy targets are configured per organization; a single installation keeps
deploying to its own cluster without an agent.

## Operating it

- **Quotas and abuse.** Storage, CI minutes, and request rates are metered per account and limited
  at the edge (per principal and address) and in cells (per organization, for expensive work).
- **Metering.** Usage — bytes stored per repository from manifests, CI minutes, egress — goes out as
  events, for billing outside ok.
- **Observability.** Every metric is labeled by cell; the expensive ones also by organization,
  within a cardinality budget.
- **Backups.** Point-in-time recovery for every database; object storage versioned for the window in
  which GC could err, and replicated across regions.

## A single installation

One cell, its directory in its own database: PostgreSQL, S3 (or MinIO, or GCS, or a directory), and
the PostgreSQL stream and queue. Helm and compose keep their shape (install.md); `ok serve -roles
all` still runs everything in one process. An installation can later move into the hosted service as
an organization (*Moving an existing installation in*), or run its own cells on Spanner, NATS, or
Pub/Sub.

## The plan

Each phase lands as CLs that keep every test passing and a single installation working. Each ends
with a test that proves it.

1. **Interfaces, on today's backends.** The object-store interface with S3, native GCS, and
   filesystem implementations; the stream and queue interfaces with PostgreSQL implementations; each
   role's store split into domain logic over a data interface with a PostgreSQL implementation;
   `internal/data`'s primitives; a conformance suite for every interface. Nothing changes for a
   user. *Done when:* every conformance suite passes against every implementation, and the full
   suites and e2e pass on GCS as on S3.
2. **Per-repository order, no leaders.** The per-repository event log, the relay, consumers with
   per-repository cursors; work leases per repository and per shard replacing every `LockFn` leader;
   the landing queue per repository; run logs streamed to object storage; random IDs in place of
   sequences.
   *Done when:* a test drives thousands of repositories at once through upload, review, landing, and
   CI across several replicas of each role, with replicas killed at random, and every event is
   applied exactly once and every repository's `main` stays green.
3. **Segments.** The LSM: tails, manifests, compaction, and segments for objects, the commit graph,
   path history, the content index, op logs, and events; reads through Bloom filters and fence
   indexes; large publishes as segments; fork networks with presence sets; garbage collection per
   repository and network.
   *Done when:* a 1 TB synthetic repository (10^7 files, 10^6 commits) imports, reads, and collects
   with its database under 100 MB, and fsck verifies it.
4. **Tenancy.** Accounts, organizations, teams, `owner/name`, visibility, roles, `Authorize`,
   tokens, robots per organization, enterprise identity providers per organization, deletion,
   renames and transfers; the two presentations of names; moving an installation in as an
   organization. *Done when:* the permission matrix — every role, visibility, and action — is a
   test, and this installation moves in as an organization with its history intact.
5. **Bus backends.** NATS JetStream, Google Pub/Sub, AWS SNS and SQS, and Kafka, each passing the
   stream and queue suites it implements, against servers in the test process (an embedded NATS
   server, Pub/Sub's `pstest`, Kafka's `kfake`, and a fake of SNS and SQS). *Done when:* phase 2's
   test passes on each.
6. **Spanner.** A Spanner implementation of every role's data interface and of the directory; the
   emulator in CI and a real instance nightly; cell moves from PostgreSQL to Spanner.
   *Done when:* every conformance suite, the full suites, and the e2e pass on Spanner, and a cell
   moves from PostgreSQL to Spanner under load without a failed request.
7. **Cells.** The global directory, the edge (routing header, authentication, rate limits),
   placement, repository and network moves, affinity within a cell, multi-region.
   *Done when:* three cells in two regions serve one namespace, and repositories move between them
   under load.
8. **Hosted automation.** microVM isolation, admission and fair share, dispatch queues per pool,
   autoscaling, the remote cache per organization, secrets.
   *Done when:* an organization flooding CI cannot delay another's runs beyond its fair share, and a
   run cannot read another tenant's checkout, cache, or secrets.
9. **ok control at scale.** Inboxes, search (repositories, people, code), webhooks, notifications.
10. **Deploy agents.**
11. **Proof at scale.** A load generator that simulates the target — a million repositories, their
    publishes, checkpoints, CLs, landings, and CI reads, at the rates in *The target* — against a
    staging deployment, with chaos (replicas, zones, the bus); and a cost model from what it
    measures.

## Testing

- **Conformance suites** for the data interfaces, the object store, the stream, and the queue, each
  run against every implementation on every change: PostgreSQL, the Spanner emulator; MinIO, a GCS
  fake, a directory; PostgreSQL, a NATS server, Pub/Sub's fake, Kafka's fake, a fake of SNS and SQS.
- **Equivalence**: ok's suites and e2e run on each supported combination, as today's suites run a
  scenario in a real and a virtual workspace (okfs.md, "Testing").
- **Scale**: phase 2's many-repository test, phase 3's 1 TB repository, phase 11's load generator.

## Decisions

1. **The repository is the unit of placement, order, and work**; a fork network is placed whole.
2. **Domain-level data interfaces, PostgreSQL and Spanner behind each**, one conformance suite per
   interface; no generic SQL or key-value layer.
3. **Immutable data in object storage as segments**, the database holding mutable state, the
   manifest, and a bounded tail.
4. **The log is the truth; the bus delivers** at least once, in any order; consumers apply exactly
   once by cursor.
5. **Stream backends:** PostgreSQL, NATS JetStream, Pub/Sub, SNS with SQS, Kafka; **queue
   backends:** PostgreSQL, NATS JetStream, Pub/Sub, SQS; each configured on its own.
6. **Object stores:** S3-compatible, native GCS, filesystem.
7. **Cells and one global directory**, Spanner multi-region in the hosted service.
8. **No leaders**: work leased per repository, organization, or shard.
9. **A Firecracker microVM per hosted run**, caches per trust domain.
10. **A single installation is one cell**, on today's backends.
11. **Every cloud, by configuration.** No backend is the default for the hosted service: each
    deployment configures the database, object store, stream, and queue its environment has (on
    AWS, the cells' databases are PostgreSQL — RDS or Aurora — since Spanner is Google's alone).
12. **Hosted CI stays Bazel-only**, as automation.md's principles make it.
13. **Fork networks share storage copy-on-write**; no repository reads what its own presence set
    does not hold.
