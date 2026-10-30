# Hosted ok — one service, a million accounts

This is the plan for running ok as a hosted, multi-tenant service — a GitHub competitor — sized for
**a million accounts, each with a 1 TB repository**, without giving up the single-installation ok
that runs today. It says what breaks at that size and why, the shape that replaces it, the
interfaces every backend sits behind (data, object storage, events, work queues), and the order the
work lands in. Each phase below is a series of CLs: phases 1 through 5 are built.

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
| One run queue | `run_queued`: `FOR UPDATE SKIP LOCKED` over every queued run, one priority order | One index every worker contends on; no tenant held to a quota | Quotas per organization, dispatch queues per worker pool on the bus ([Scheduling](#scheduling-and-fairness)) |
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
5. **No global singletons, and no leaders.** Every loop that runs on one replica today runs per
   repository, per organization, or per shard of a cell, under a lease taken on demand and fenced in
   the transactions that write under it; where replicas must agree — which deployments may start —
   one transaction decides, not one replica.
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
  (*Packs and fork networks*). Who may fork, a fork's visibility (its parent's), and what becomes of
  forks when a repository is deleted or changes visibility follow GitHub (*Fork networks, as
  built*, "GitHub's rules"); unlike GitHub, no repository reads another's content through the
  shared storage.

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

### Tenancy, as built

The sections above are the shape; this is the design phase 4 builds, settled against ok's code.

**Two modes, one code path.** `tenancy.mode` is `installation` (the default) or `hosted`. Both run
the model below; they differ in what is presented and who may make accounts:

| | `installation` | `hosted` |
|---|---|---|
| Organizations | One, the installation's (`tenancy.organization`, by default the domain's first label), made at start | Any number, made by people |
| People | The system of record's, each a member of the one organization | Sign up themselves, or are provisioned by an enterprise organization |
| A person | `ada.human.example.com` | `@ada` |
| A robot | `ci.robot.example.com` | `acme/ci` |
| A team | `eng.group.example.com` (a synced or declared group) | `@acme/eng` |
| A repository | `app` (the organization's), `ada/app` (a person's) | `acme/app` |

A parser accepts either form of a name in either mode, so a repository that moves in keeps working;
what ok writes and shows is the mode's. URLs are `/{owner}/{repo}` in both modes; an installation
also answers its organization's repositories at `/{repo}`, redirecting. As built, ok control finds
the repository a path names through the SCM, and so the directory: its bare name (`/app/cls`), or
a name it had, redirects a page to where it is now, the rest of the path and the query with it,
for as long as the old name is free; a form posted to an old path is refused, to be reloaded. Its
"new repository" form offers the installation's organization and the person's own handle.

**Principals have stable IDs, and every role stores them.** A principal's ID (`prn_…`, as identity
mints today) never changes; its name does — a person renamed in the system of record, a handle
changed, an installation moved in. So every role's store records principals by ID — a CL's author,
reviewers, approvals, uploaders, submitters, a run's requester, a deployment's actor, a workspace's
owner — and every API presents names in the mode's form, the role translating at its boundary
through the directory (`names.Resolver`, a cache the directory's stream keeps current). History
follows a rename, and moving an installation in rewrites no row outside the directory. The
caller's identity in a request (`authn.Caller`) carries both. OWNERS files, workflow files, and
groups-and-ACLs trees name principals in either form and are resolved to IDs when checked.
Stored text never names a principal: a run or a submission canceled records who canceled it
(`canceled_by`) beside a reason that names no one, a deployment who asked it to halt (`halt_by`)
apart from why ok halted it itself (`halt_reason`), and what is shown — "canceled by ada" — is
composed as it is shown. Commits are the exception: the SCM signs those it writes with the name
the principal has then, as text in a commit is history.

**The directory** is ok identity's store (it calls its schema that already), extended:

| | What |
|---|---|
| `account` | People and organizations, one namespace of handles (1–39 lowercase letters, digits, inner hyphens; reserved words — `assets`, `id`, `api`, `settings`, `login`, `new`, `orgs`, … — refused); each a principal with its ID. Robots and teams are principals of an organization, named under it. |
| `organization` | An organization's settings: its base role (none, read, triage, write, maintain, or admin; `write` for an installation's, as today's every-person-may-upload), who may make repositories, whether private repositories may be forked, its enterprise (an ID several organizations share; `internal` repositories are read by every member of every organization in it). |
| `membership` | An account's role in an organization: owner or member. |
| `team`, `team_member` | Teams, nested by a parent; a team's members are its own and its descendants'. A grant to a team reaches its descendants' members, as GitHub's does. |
| `repository_entry` | Each repository's ID, owner, name, visibility, fork parent, network, cell (one, in phase 4), and state (active, or deleted since a time). Unique per owner. |
| `redirect` | A name a repository had — renamed, transferred, or its owner renamed — and its ID, held until the name is taken again. |
| `grant` | A role on one repository for a team or an account (an outside collaborator's is an account's). |
| `token` | Fine-grained personal tokens, and apps' installation tokens (below). |

The SCM keeps each repository's ID and content and drops its name: a repository is made by asking
the directory for an entry (owner, name, visibility; the directory chooses the ID and the cell),
then the cell's SCM makes its storage under that ID, and the entry becomes active; an entry still
`creating` after an hour is removed with whatever its cell made. A rename, a transfer, or a
visibility change is the directory's alone. The SCM finds a repository by its full name through the
directory, and shows the full name it has now (kept half a minute); ok control and the deployer
keep each repository's full name as the directory's events say (`internal/id/dirfeed`), an owner's
rename renaming each of its repositories there. A creation a stopped replica left half done
resumes when its maker asks again: the directory answers their entry still being made, and the
SCM takes storage made already under its ID as made. The SCM needs ok identity, as every role does: in development, without a workload credential, it
names itself to a development identity, as `ok serve -dev`'s roles do.

**Roles and actions.** `Authorize(caller, repository, action)` decides every request about a
repository (`internal/authz`), the matrix below being its test:

| Action | read | triage | write | maintain | admin |
|---|---|---|---|---|---|
| Read code, CLs, runs, logs, deployments; clone; fork (where forking is allowed) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Propose a CL (upload; review, comment) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Triage: labels, reviewers, close and reopen CLs | | ✓ | ✓ | ✓ | ✓ |
| Write: submit a CL to land, rerun and cancel checks, act on the deployments of one's own CLs | | | ✓ | ✓ | ✓ |
| Maintain: the repository's settings but visibility, its name, and its grants; act on any deployment | | | | ✓ | ✓ |
| Admin: visibility, rename, transfer, delete, grants | | | | | ✓ |

A caller's role is the highest of: the repository's owner, if a person (admin); an owner of the
owning organization (admin); a member of it (its base role); each team they are in, through the
team's grants; their own grant; and the repository's visibility — `public` gives anyone read,
signed in or not, and `internal` gives read to every member of the owning organization's
enterprise (or organization, with none). A repository may require write to propose
(`propose: write`). A suspended, deleted, or unknown caller has none. A soft-deleted repository is
seen by its admins alone, to restore it. Landing still needs OWNERS' approvals (owners.md); the
submitter needs write. ok's own roles (`roles`) are not people: they act on a person's behalf only
after the role in front checked that person (ok control before it asks the SCM to land, say), and
the SCM admits them as before.

A token narrows its holder: a personal token names repositories (or every one its holder may read)
and, per action class, at most a role; an app's installation token holds the grants its
installation has, for an hour. A caller's role through a token is the lesser of its holder's and
the token's.

**Where it is decided.** `authz.Decide(facts, action)` is a pure function of the facts: the
repository's entry and its owner's settings, and the caller's memberships, teams, and grants on
it. Each role asks the directory for the facts of a (caller, repository) once and keeps them a few
seconds (`authz.Checker`); the directory's stream (the bus's topic `directory.repositories`,
`internal/id/dirfeed`) carries each change — a grant, a membership, a visibility — and drops what it
touches at once. Phase 7 moves the facts into a projection each cell keeps.

As built, Authorize is enforced where people reach ok. The SCM decides each request a person makes
(its `Repositories`, `Store`, `ChangeLists`, `Browse`, and `Workspaces` are the services people
call): every such request names its repository, and each method a person may call does one action
to it; a method given none is refused to people. ok control calls the SCM as itself, so it decides
for each person before it acts — its pages, and the ChangeLists service `jj cl` calls: a
repository the person cannot see answers as though there were none, and no redirect from a name it
had or its bare name says otherwise; the finder and the services pages list only what the person
may read. A CL's lifecycle is its author's, while they may propose, or a triager's; submitting is
its author's or an admin's, who may land; reruns need write; allowing a migration's hazards, admin.
Anyone reads a public repository's pages, signed in or not; a form posted needs a person, and jj
needs one (a workspace is a person's). ok automation and the deployer serve ok's roles alone. ok
control drops a repository's kept facts as the directory's events reach it; the SCM keeps them
five seconds.

**Robots, as built.** Each request records which of ok's roles made it, if any
(`authn.Caller.Role`, from `roles`); the SCM checks every other caller with Authorize — a person,
an installation's robot, or an organization's — and admits robots to the services people call. An
organization's robots (`organization_robots`, `<name>.<org>.robot.<domain>`) are its owners' to
make, certify, and delete in ok identity's Tenancy; a member of nothing, one is granted the
organization's repositories alone. An organization trusts its own issuers (`organization_issuers`)
and binds its robots to their subjects (`robot_bindings.organization_id`): a token for it carries its
ID as the audience, which the verifier reads to choose between the configuration's issuers and an
organization's, discovering and keeping an organization's (`internal/id/federation`) through a
client that reaches public addresses over HTTPS alone.

**Apps, as built.** An app (`apps`) is an organization's robot with a public key; an installation
(`app_installations`) is an organization's grant to it — a role, on named repositories or every one
— and identity's facts give the app's robot that role where its installation covers the repository.
An installation token (`tokens.installation_id`) is the robot's, narrowed by `authz.Token.Owner` to
the installing organization; authn resolves `oki_…` as the app's robot through
`Verify.GetToken`, as it does `okp_…` as a person. An app signs in with a JWT it signs, carried in
the request to `AppTokens`, so no role's authentication changes for it.

**Accounts, as built.** A hosted service's people make their own accounts (identity.md, "Accounts,
in a hosted service"): email and password — argon2id (`internal/id/password`, 64 MiB and three
passes, four at once per process), mailed links (`email_tokens`, by their SHA-256, taken once),
failed sign-ins counted in the directory so every replica sees them (`sign_in_failures`) — or a
social provider, linked by subject (`login_links`).

**Enterprise organizations, as built.** An organization's identity provider
(`organization_identity`) keeps its settings as given and its secrets sealed (`internal/id/seal`,
AES-256-GCM, each sealing bound to its organization); `internal/id/enterprise` builds its source and
runs its sync, `directory.Sync` over the organization's own state (`store.OrganizationSync`: its
people, linked by external ID in `organization_people`, and its synced teams, every other name
held), naming people `<handle>-<org>` and groups as teams through the sync's naming hooks. SCIM is
scoped per organization (`scim_users.scope`), at `/id/scim/v2/orgs/<org>` with the organization's
token. The groups-and-ACLs tree is still the installation's: an organization's own, declared in a
repository it names, comes with the hosted presentation of names.

**Purging, as built.** The SCM removes expired repositories — deleted more than 30 days ago, or
still being made after an hour — on a work lease per repository (`server.Purge`, store.PurgeWork),
its job booked for when the repository may expire: by the transaction that makes the repository's
storage, an hour on, and, as the SCM follows the directory's events (`directory.repositories`,
group `scm`), by a repository's deletion, for its retention's end. The job asks ok identity by its
clock (`GetExpiry`, which says when one deleted or being made will expire, and the purge books
itself for then); one expired, it deletes the repository's storage, then has the directory remove
the entry (`PurgeRepository`), a deleted repository's purge logged on `directory.repositories`. Nothing brings an expired repository back: identity refuses a restore
past the retention period, and an expired creation's resumption or activation (a creation refused so
deletes what it made). Each role follows the purge and forgets the repository whole: ok control its
CLs and favorites, ok automation its runs' logs, runs, plans, and submissions (taking the
repository's claim, so it never runs beside the SCM's events), the deployer its plans, services,
and deployments — what it deployed runs on, managed no more. The log outlives the entry, so a role
that was away purges as it catches up. The entry goes before the roles' rows, unlike the order
above: IDs are never reused, so nothing a role still holds can be mistaken for a repository made
under the freed name.

**Forks, as built.** The SCM serves forking (`Repositories.ForkRepository`, and
`Review.ForkRepository` for ok control): it resolves the upstream as the person sees it, decides
Authorize's fork on it (a personal token narrowing them), and the directory reserves the fork under
it — deciding again, and that the person may make repositories of the fork's owner — before the
storage forks by reference and the directory activates it, as a creation is made and resumed.
`SyncFork` needs write on the fork and the upstream readable, and syncs from the upstream the
directory names now — so GitHub's rules may give a fork a new one — which storage takes only where
the new upstream's main came to the fork's base at the place the fork follows up to (deleting a
repository now keeps its forks' places for this); a fork whose new upstream diverged earlier is
refused. Each repository's answer names its upstream (`Repository.parent`). ok control's landing
offers Fork where the person may, and Sync on a fork they may write to; jj has `jj fork` and `jj
sync --upstream`.

**GitHub's rules, as built.** The directory keeps them in the fork tree's parent links, each
change in its fork's log (`KIND_UPSTREAM`): a fork takes its upstream's visibility at its making,
and changes it only once it stands alone; a fork of an internal repository is not forked; an
upstream's visibility changing makes each of its forks the root of a tree of its own; a private or
internal repository deleted deletes its forks and theirs, a public one makes its oldest active fork
the others' upstream. Authorize reads a fork that is not public as its tree's root: the root's
teams' grants (not its people's own), read for the root's owners, and for an internal fork the
root's internal readers. A change that may take access away — a grant revoked, a member or a
team's member removed, a team deleted, a base role changed, a transfer — checks the people's forks
in the networks it touches against their roots, and deletes each whose owner reads its root no
more; an organization's forks stay. The installation's groups and ACLs, which the system of record
and the tree change, do not check forks as they change: an installation's forks are its people's,
and its repositories internal.

**Deleting accounts and organizations, as built.** A hosted service's person deletes their own
account from its page, typing their handle (`Tenancy.DeleteAccount`); an organization's owners
delete it (`DeleteOrganization`). Either is suspended at once — sessions ended, its repositories
deleted as any deletion is, GitHub's rules and all (`deletions`) — and restorable for 30 days, an
account by an administrator (`Admin.RestoreAccount`), an organization by an owner, with the
repositories its deletion deleted. Then identity's housekeeping finishes it, one replica each: a
person tombstoned — handle retired, credentials and tokens revoked, sign-ins, memberships, and
teams gone; an organization's teams, robots, apps and their installations, identity provider,
issuers, the people its provider made, and memberships gone, its handle retired too, which no person
may then take. Some organization's only owner is refused, as is a person a system of record or an
organization's identity provider made: theirs to delete. The repositories go on their own time, as
the SCM purges them.

**Names, as built.** A name in the directory is one string, DNS-like, in either mode;
`identity.Names.Present` shows it as the mode has it — an installation's people by handle and the
rest whole, a hosted service's `@ada`, `@acme/eng`, `acme/ci` (ok's own robots `ok/scm`), `acme` —
and `ParsePrincipal` and `ParseSet` read what people write in either form, so OWNERS files, reviewers,
and ok control's forms written for an installation keep working once it moves in. OWNERS take
`user: @ada` and a team as `acl: @acme/eng`, which the SCM checks and ok control resolves through
ok identity as it does an ACL; ok control shows names as the mode has them.

**Moving an installation in, as built.** `ok identity move-in`, run with the installation's
configuration and ok's roles stopped, converts the directory in one transaction
(`store.MoveIn`): the organization takes the handle given (its old handle an alias), its people its
members and its administrators (`admins.acl`, or `bootstrap_admins`) its owners; its synced groups
its teams, a group of groups flattened to its people, their grants moved with them; its own robots
the organization's (`ci.acme.robot.<domain>`), its roles' and ServiceAccounts' kept ok's; its system
of record, if not a static file, the organization's identity provider, its secrets sealed for the
organization and its people linked by external ID, keeping their handles (`plain_handles`, where an
organization's people are otherwise `<handle>-<org>`), a SCIM provider's data and token moving to
the organization's endpoint; each repository's rename logged. Every name given up is an alias
(`name_aliases`) of the one that took its place — `ada`'s own never change — so the tree, OWNERS
files, old URLs, and lookups by the old names (`Directory.GetPrincipal`, `GetSet`, an owner's old
handle) keep naming what they named, and an installation's bare repository names resolve to the
organization moved in. Identity records the mode it serves (`tenancy_state`) and refuses to start
in the other; the command says what of the configuration to change — `tenancy.mode: hosted`, no
`identity.system_of_record`, `identity.tree_repository` as `<owner>/<name>`, which a hosted
configuration requires.

**The permission matrix, as built.** One table (`internal/authz/authztest`) writes the matrix out
apart from authz's rules: every kind of caller of an organization's repository — anonymous, a
stranger, a suspended owner, a member, a team's member, an outside collaborator, the enterprise's,
an owner, an owner's personal tokens, an app's installation tokens — the role each has at each
visibility, and what each action needs. authz's tests hold `Decide` to it; the SCM's gate is swept
over it for every method people and robots call, at every visibility (let through exactly when it
allows, refused as though there were nothing there when the caller cannot see the repository, and
saying why when they can), with every such method listed with its action; ok control's operations —
a CL's lifecycle, submitting, its checks, deployments, administering — are swept over it likewise.
ok's own roles are let through by each API before Authorize decides anything; anonymous callers
reach the SCM through ok control alone (`ok.v1.callers`), whose pages read public repositories for
them. ok automation and the deployer serve ok's roles alone.

**Personal tokens, as built.** ok identity's `Tokens` service, and the account page, make a
person's tokens (`okp_` and 32 random bytes, kept as their SHA-256): the repositories they reach by
full name (none: every one), the most they allow, a year at most; listed and revoked by their
holder. `authn` resolves a bearer `okp_…` through the directory (`Verify.GetToken`) to its holder,
the caller carrying what it allows (`authn.Caller.Token`), which the SCM, ok control, and
identity's Tenancy pass to Authorize. A token reaches repositories alone: identity takes one only
on the methods it names (reading the directory, and Tenancy's repository calls), its pages sign no
token holder in, and the SCM applies a caller's token to a name it resolves, which the directory,
seeing the SCM's request, cannot.

**The directory's log.** Each change to a repository's entry that stands — made, renamed,
transferred, its visibility, who proposes, or its grants changed, deleted, restored, purged — is an
event in that repository's log, written in the change's transaction and carrying the entry after
it, its owner by ID; an entry being made is in none until it is made. ok identity's relay publishes
the log on `directory.repositories`, and its `DirectoryEvents` service serves each repository's log
and how far each reaches, so a follower (`internal/bus/follow`) catches up what it missed, as the
SCM's followers do.

**The Tenancy service** (ok identity's `Tenancy`; `Authorization.Facts` serves the roles' facts)
changes the directory for a person — its caller, or the one ok control or the SCM names — and
Authorize decides what that person may do to each repository; an organization is its owners' to
manage. An organization is a principal, `acme.org.<domain>`, that owns repositories and acts as no
one: no credential, session, token, or trusted header makes it a caller. A team is a set,
`eng.acme.team.<domain>`, of the organization's members by ID, which identity expands as it does
groups — so a groups-and-ACLs tree may name one, and membership through a team counts its teams
below. An installation's organization stores no memberships: its members are its active people and
robots, its owners ok's administrators (`admins.acl.<domain>`), and its teams its groups, so the
system of record and the tree stay the one place each is said. The SCM reserves an entry
(`creating`), makes the storage, and activates it; whoever makes an organization's repository is
granted admin of it, as GitHub does, and a fork is no more visible than the repository it forked.

**Names and the edge.** `owner/name` resolves to an ID through the directory (a redirect answering
for an old name); every request names the repository by ID, after the client resolved it once
(`jj clone acme/app` resolves, and the clone keeps the ID). Phase 7's edge routes on the
`ok-repository` header.

**Robots and tokens per organization.** A robot belongs to an organization, which binds it to OIDC
issuers it trusts (`organization_federation`: the organization's own `identity.federation`) or has
ok certify a key it holds; its roles are its grants. An **app** is a robot with a public key: an
organization installs it with repositories and a role, and the app trades a token it signs for an
installation token, good for an hour. Personal tokens are `okp_`, installation tokens `oki_`, each
a random secret stored as its SHA-256 (they are 32 random bytes, not passwords), shown once.

**Enterprise organizations.** An organization may name its own systems of record and sign-in —
OIDC, LDAP, SCIM — stored in the directory, their secrets sealed with the installation's key
(`identity.sealing_key_file`, AES-GCM); its people are provisioned and deprovisioned by them as
the installation's are today, linked to accounts by (organization, subject), and its groups synced
as teams. Its groups-and-ACLs tree is declared in a repository it names, as an installation's is
today; OWNERS name its ACLs as `@acme/eng`. An installation's systems of record are its one
organization's.

**Signing up and in, hosted.** People make accounts with a verified email and a password (argon2id)
or a social OIDC provider the hosted service configures, then add passkeys; an enterprise
organization's people sign in through its single sign-on. jj and okfs sign in as today.

**Deleting, renaming, transferring.** Deleting a repository marks its entry deleted; its name
stays reserved, its content readable to its admins only, and restoring it within 30 days undoes
it; afterwards each role deletes what it holds of the repository (the SCM's storage half, phase
3.7; CLs, runs, deployments) and the entry goes, its name freed. A rename or a transfer leaves a
redirect; a transfer drops the old owner's teams' grants and keeps outside collaborators'; forks
follow *GitHub's rules* (*Fork networks, as built*). An account deleted is soft-deleted the same
way, then its repositories are deleted, its memberships dropped, and its handle retired.

**Moving an installation in.** `ok identity move-in -organization acme` switches an installation
to `hosted` in place: its organization keeps its ID and takes the handle; each person keeps their
ID and handle; groups become teams; robots become the organization's; repositories become
`acme/<name>`, their old bare names redirecting. No role's rows change: they hold IDs. It runs
against identity's database with ok's roles stopped, not as a call to a running identity: the move
changes the mode every role serves, so every role starts again in the new one. Bringing an installation into another deployment's directory, with its content, is a cell
move (phase 7).

**Done when** `internal/authz`'s matrix test covers every role, visibility, action, and kind of
caller (anonymous, a stranger, a member, a team's member, an outside collaborator, an owner, a
suspended person, a personal token, an installation token, ok's own role), each role's API refuses
what the matrix refuses and allows what it allows, and an installation with people, groups,
robots, repositories, CLs, runs, and deployments moves in, after which every history shows the
same principals under their `@handles` and every old name redirects.

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

### Cells, as built

Phase 7's design, settled against ok's code before it is built. Phase 4 left each repository's
entry naming a cell, which nothing sets; nothing else knows of cells.

**A cell in configuration.** Every role but identity serves one cell: `cell: {name, region}` (a
single installation's is `default`, in no region, as an empty configuration says). The directory
keeps the cells — `cell`: name, region, whether new repositories are placed there, and where its
SCM, ok control, ok automation, and the deployer are reached by the edge and by other cells —
from identity's configuration (`identity.cells`; with none, the one cell `default`, whose roles are
`services`'). A role refuses a request about a repository another cell holds: `FailedPrecondition`
with an `ok.v1.Moved` detail naming the cell, which the edge acts on; a page answers `421`. The
directory stays identity's, global, a multi-region Spanner database in the hosted service; a cell's
roles reach it as today (`services.identity`). What each role caches of it (`authz.Checker`,
`names.Resolver`) stays as it is: moving those facts into a projection each cell keeps is for the
optimization after the phases are built.

**Placement.** `ReserveRepository` chooses the cell when the request names none: a fork's
network's; otherwise, of the cells open to placement in the owner's region — an account's
`region`, which an owner may set, or the directory's default — the one holding the fewest
repositories, which the directory counts. An SCM asked to make a repository the directory placed in
another cell forwards the request to that cell's SCM, as itself and for the person, which makes the
storage and activates the entry, as a creation resumed does today. *As built:* an owner names its region with
identity's `Tenancy.SetRegion` — a person their own, an organization's owners its, one a cell is in;
`GetRegion` says it and where new repositories go — kept in `owner_regions`; the directory's default
is `identity.default_region`; with no open cell in the region, any open cell's.

**The edge** (`internal/edge`, `ok edge serve`, alone in its process): stateless replicas in every
region at ok's public address, gRPC, REST, and pages on one listener, running as their robot
(`roles.edge`, `CALLER_EDGE`). It authenticates each request as the roles do (`authn.Resolver`),
refusing a credential that fails, limits each principal's requests and each address's (token
buckets at each replica, `edge.limits`; the address a proxy in front writes, `edge.address_header`,
or the connection's), and forwards it with its credentials untouched — a device credential, a
token, a cookie are all metadata — to a cell, whose role authenticates it again from its cache: a
cell trusts nothing new. It routes as an installation's one public address does
(docs/configuration.md, "One public address"):

- `/id/…` and identity's services to the directory.
- The SCM's, ok automation's, and the deployer's services to that role of the cell of the
  repository their `ok-repository` header names — an ID, or `owner/name` — which ok's clients set on
  every request that names one (jj's store by ID; `jj clone`, `jj fork`, and `jj cl` by name); one
  that names none (a creation, a listing) to the edge's home cell — an open cell of its region
  (`edge.region`), else any open one.
- Everything else to a cell's ok control, a page by its path: `/{owner}/{repo}/…`, or an
  installation's `/{repo}/…`, to the repository's cell; the rest (home, the finder, settings) to
  the home cell, as every cell's ok control knows every repository's name from the directory's
  events.
- Where a repository is comes from identity's `Cells.Route` (its ID and cell, by ID, full name, or a
  name it had), kept 30 seconds — two for one there is none of — rather than a map of every entry:
  what is kept follows the requests, and a stale route costs one more hop. A cell refuses a
  request about a repository another holds — a gRPC call with `ok.v1.Moved`, REST and a page with
  `421` and `Ok-Cell` — and the edge sends it again, whole, to the cell named, and goes there first
  from then on. It keeps up to 16 MiB of a request's body until the role's answer starts, for that:
  a call's status comes in its trailers, so it reads the answer to its first message or its end
  first. A request that sent more before the answer gets `UNAVAILABLE` (a page, the `421`, which a
  browser asks again on a new connection); jj publishes in batches of about 3 MiB.
- Within a cell, a repository's requests go to one of a role's replicas by rendezvous hashing of
  its ID over the addresses the role's name resolves to (looked up every 10 seconds), so the SCM's
  caches stay hot; requests about no known repository go round them.

**Moving** a repository between cells moves its fork network — every repository in it, which
share their storage — and is the directory's to run (`internal/id/mover`). An administrator asks
it (`ok admin move REPOSITORY CELL`, identity's `Admin.MoveNetwork`; `ok admin moves` lists them,
`ok admin cancel-move NETWORK` abandons one): the directory records the move (`network_moves`: the
network, its source and destination, its phase, who asked, when it is next tried, and why its last
step failed), refusing it while another of the network runs, while a repository of it is still
being made, or into a cell closed to new repositories. Each write of an unfinished move books its
next step (keyed work `move`, keyed by the network, for its `next_at`), whose job brings it to one
identity replica, which drives it under its network's lock (`move:<network>`) as far as it goes;
a step that fails is tried again 30 seconds later, and each is safe to repeat, so a replica that
stops short leaves the move to the next. The
directory reaches each cell's roles where `identity.cells` says, as its own robot (`CALLER_IDENTITY`),
and each role serves a `Moves` service (`ok.v1.Moves`) to it and to the same role of another cell,
which copies from it as itself. A move's phases:

1. **Copying.** Each destination role copies what is immutable, as it is now, while the network is
   written: the SCM the network's objects (`nets/<network>/…`: packs and segments), ok automation
   its runs' logs (`runs/<id>`, and a lease's segments, `live/<id>/…`) — each object it lacks,
   streamed from the source's role (`ListObjects`, `ReadObject`). Then each source role freezes
   the network (`Freeze`), the deployer first: it refuses while a deployment of the network runs
   — a running one acts on its cell's cluster, and finishes there — which costs nothing frozen
   yet. A role that refuses has those that froze thaw, so none waits frozen on another; the move
   asks again later.
2. **Frozen.** From a freeze's commit, every write transaction about the network is refused with
   `UNAVAILABLE` and an `ok.v1.Frozen` detail, a refusal made before anything was done, which ok's
   clients ask again for up to five minutes (jj's publishes, uploads, and `jj cl`'s actions; okfs
   retries on its own). It holds because it is checked in the transaction, not the request: each
   role's write transactions about a repository read its gate in share mode — the SCM its
   network's row (`network.move_state`); ok control, ok automation, and the deployer a
   `move_gate` row per repository, made if it is not there, as PostgreSQL locks no row that is
   not — and freezing writes the gate, so it waits for every write under way and none starts
   after. A transaction names its repository in its context (`move.WithRepository`: a request's
   repository, a feed's event's, keyed work's key), or reads the gate of the repository of a row it
   locks (ok automation's locks of runs, plans, submissions, and attempts; the deployer's of
   deployments); a transaction about many repositories leaves the moving ones be (ok automation's
   claims and sweeps, the deployer's starts and observations). ok automation gives the runs its
   workers hold back to the queue, unspent: they run again, from the start, where the network goes.
   Each destination role then copies everything else, whole, under the same IDs (random since
   phase 6, so unique in every cell) — what it held of the network already dropped first, a failed
   import's or a residence before — each table's rows about the network (`Export`, a page by
   primary key, as rows of `ok.v1.Row` named by PostgreSQL's columns, which a Spanner backend maps;
   up to 1,000 rows and 8 MiB a page), parents first, a row referencing its own table's after them
   all; then the objects written since copying began; then it moves what numbers rows — PostgreSQL's
   sequences, Spanner's counters — past those brought. The network stays frozen there. A move
   frozen over an hour, its rows not yet copied, is abandoned.
3. **Imported**, then **named**: one directory transaction names the destination in every entry of
   the network, logging each (`KIND_MOVED`), and records the move named. Edges and roles see it;
   an SCM about to refuse a request as another cell's decides on fresh facts, so none refuses one
   moved to it on facts kept from before.
4. **Done.** Each destination role opens the network (`Open`): its writes pass, and the role brings
   up to date what it keeps of it outside what moved — ok control its repository's head, from its
   SCM; ok automation its landing queues; the deployer its plans and its services' observations;
   the SCM its compaction and collection. Then each source role records the move (`Finish`): every
   write about the network refused with `Moved`, naming the destination, its keyed work done there
   now, and its rows and objects deleted a day later.

What moves is what a cell holds of the network; what every cell keeps of every repository stays:
ok control's entry of each repository, which the directory's events name, and anyone's favorites;
the deployer's repository's name (its head and what is planned move); every role's place in the
directory's events. The directory's followers write through no gate.

A move canceled, or abandoned, before it is named has each destination role drop what it copied
(`Drop`) and each source role thaw (`Thaw`); once named, it only goes on. So a move freezes a
network for as long as copying its rows takes, its bytes copied before: seconds for most. Copying
only what changed since copying began waits for the optimization after the phases are built. Moving
a cell from PostgreSQL to Spanner is moving its networks, one at a time, to a Spanner cell. A cell
names an address for every role another cell runs, or no network moves into or out of it.

**Regions.** A cell's region is its configuration's; the edge serves every region and forwards to
whichever a repository's cell is in; the directory's Spanner is multi-region by its instance's
configuration, which ok does not choose.

**Done when**, as the plan says: three cells in two regions — each with its own databases,
buckets, and roles — one directory, and an edge in each region serve one namespace; repositories
are placed by their owners' regions; jj clones, publishes, uploads, and lands through the edges; and
repositories and fork networks move between the cells under a load of publishes, uploads, and
landings with no request failing but those retried during a freeze, and every history intact. *As built*
(`TestCellsUnderLoad`): three cells in two regions — two on PostgreSQL, one on Spanner — one
directory, and an edge in each region; repositories made through either edge in their owners'
regions' cells, a creation asked of the other region's forwarded; a network moving from a PostgreSQL
cell to the Spanner cell, and one from it to a PostgreSQL cell, while publishes stream through the
edges to them and to one that stays — none failing but those asked again while frozen, about a
second each, and every commit where its repository landed. Uploads and landings move with their
roles' rows (each role's Moves test, and `TestMoveBetweenCells` for the SCM and ok control through
the directory's runner); a run of jj and the landing queue through the edges during a move is for
the hosted service's own load tests.

**The order.** (1) This design. (2) Cells in configuration and the directory, placement, the SCM
forwarding a creation, and roles refusing another cell's repositories. (3) The `ok-repository`
header from ok's clients, and the edge: routing, routes from the directory, `Moved`, rate limits,
affinity. (4) Moves: the directory's `move`, each role's gate, `Moves` service, export and import, and
the source's purge; jj retrying a frozen network's refusals. (5) Regions in placement, and the end-to-end test
of three cells in two regions with moves under load — among them a PostgreSQL cell's repositories
moving to a Spanner cell, which ends phase 6.

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
- **IDs** — every surrogate key that names something across cells is random 63-bit
  (`data.NewID`), never a sequence: repositories first (phase 2), whose ID every role keys by and
  which move between cells; then runs, plans, comments, deployments, and checkpoints (phase 6),
  once their pages order by time and page by a cursor of time and ID — today ~45 queries take a
  larger ID to mean a later one (the queue's oldest run, a repository's latest plan, `before`
  paging). Numbers people see in order — a repository's CL numbers — and numbers encoded compactly
  stay counters per repository, under the repository's key: a pack's number in the content index,
  a zstd dictionary's ID (32 bits in every frame), the content log's order.
  *As built:* ok automation's runs, plans, submissions, and attempts; ok control's comments and a
  CL's log lines; the deployer's plans, services, deployments, and their log lines; the SCM's
  checkpoints and operation logs; and identity's audit lines are made under `data.NewID()`. What
  needs their order reads it from their times — a run's queue time, a row's creation — the ID only
  breaking ties: the run queue and the landing queue go oldest first, "latest" is the newest, and a
  page's cursor is a row whose time and ID the next page starts below. A transaction that makes
  several rows takes each one's time from the clock (`clock_timestamp()`), not from its own start,
  so they keep the order it made them in; a CL's log lines, whose time is the event's, are ordered
  among one time by when each was written. Rows made before keep their IDs, which order them as
  before.
- **Time** — a transaction's commit timestamp, the database's clock, as today's leases already use.
- **Leases** — per-repository work leases and per-shard leases (*Work without leaders*), as rows
  read `FOR UPDATE` on both backends, and **fenced**: a transaction that writes under a lease reads
  the lease's row and commits only if its holder still holds it. A lease's row lives in the same
  database as what it guards, so the check and the write are one transaction, and a holder that
  stalled past its lease — a long pause, a partition — never commits: at worst it wastes its work.
- **Claims** — keyed work held outside a job (`work.Claim`): a sync asked for, a fold that must
  follow what its caller did, a full compaction, a deployment's steps. One that waits for its holder
  (`wait.Claims`) registers an inbox on the claim, tries again, and is told as the holder lets go —
  through the claim's release, or a runner's `LetGo` — or wakes as the holder's lease would lapse,
  should it have died holding it; no waiter asks again on a timer.
- **Locks** — `internal/lock`: a row in the role's database, held one of two ways. As a lease, by
  a locker that refreshes it, judged by the database's clock and fenced as leases are, for what spans
  transactions — a migration alone, whose waiter starts before its schema exists, with neither
  registrations nor keyed work to wait by: it asks again each second, as it starts. (The deployer
  takes a deployment's lock beside its claim for one release, so a replica of the release before,
  which takes only the lock, is kept out.) A fence holds only what a takeover writes, the lease's holder (its
  refresh ID: on Spanner that cell, which Spanner locks alone; on PostgreSQL `FOR KEY SHARE`,
  which a takeover's `FOR UPDATE` waits for), never what its holder's own refreshes write: a fenced
  transaction longer than the lease would otherwise keep the refresh out and lose its lock. Within a transaction (`lock.InTx`), by locking its row until the
  transaction ends, for what one transaction does — allocating names, planning a CL's patchset. The
  two exclude each other. **No advisory locks**: they are invisible to every tool
  but PostgreSQL's, held by a connection rather than a holder, and Spanner has none.
- **Migrations** — PostgreSQL's ordered SQL, Spanner's DDL batches through the admin API, each
  version recorded, under the lock's `migrate` lock (its table made first, on its own, being what
  the lock needs). A schema is made `IF NOT EXISTS`, and made again if two replicas race.
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
- **No queue in the database.** What `SKIP LOCKED` does today moves to the queue interface, or to
  conditional updates — a lease taken by `UPDATE … WHERE` its holder is none or lapsed, replicas
  starting their scans at random places so they rarely meet; what `LISTEN` did to registrations and
  inboxes (state.md, *Waiting*); advisory locks to the lock (*Shared primitives*).
- **Sequencing and the relay.** A repository's CL numbers and its log's places are given just
  after the transactions that make them commit, by its sequencing pass, not taken from counters
  inside them (scm.md, "Sequencing"); the pass marks the events it places pending, which a relay
  reads without a lock and takes by key (*The per-repository log*).
- **Ancestry.** Walks that use recursive queries in PostgreSQL today use the commit graph segments
  instead (*Segments*), on both backends.
- **Databases.** One database per role per cell; an instance holds up to 100, so twenty cells per
  instance.
- **Tests.** The conformance suites run against Spanner Omni — the engine Spanner runs, as one
  server on the test's machine (its no-charge edition, from Google's release tarball) — and against
  a real instance, the smallest one, 100 processing units in one region; each run is in databases of
  its own, dropped after (`internal/testdb`; building.md, "Tests"). The emulator serves too, but
  allows one read-write transaction or schema change at a time and aborts any other, which the
  client retries: a suite's concurrent cases run there one after another, where on Omni and the
  real instance they meet.

**Spanner's primitives, as built.** `internal/lock` keeps its locks in a `lock.DB`, PostgreSQL's or
Spanner's (`lock/spannerlock`): taking, refreshing, and giving up a lease are each a read-write
transaction judged by Spanner's clock; a transaction's lock (`InTx`) writes its row, so Spanner runs
two that hold it one after the other; a fence's check reads its row, so a takeover waits for the
transaction. `internal/data/spannerdata` holds what every role's backend does alike: `Txn`, a
read-write transaction that checks its context's fence first, retried by the client when Spanner
aborts it; keyed work and follow cursors, each reading its row and then writing it; and `Migrate`,
files of DDL each one schema change, its version recorded, under the database's migrate lock — the
change's operation named for its version, so a replica that died mid-change waits for it rather
than making it twice; a database that has the locks and migrations tables is not asked for them
again, as a schema change costs seconds even when it changes nothing. One locker's suite
(`locktest`) passes on both databases. A backend's tests make each case's database with its schema
at once (`spannerdata/spannertest`): Migrate's schema changes on a new database take tens of
seconds each on Spanner Omni — the first on a database the longest — where a database made with its
schema takes under a second.

**The deployer on Spanner, as built** (`deploy/store/spannerstore`; `database.type: spanner`,
configuration.md). Its own database, `<prefix>-deploy`, made as it first starts (`internal/spannerdb`).
What is reached through its repository — main's history, the plans — is interleaved in the
repository; deployments and services, reached by their random IDs, are tables of their own, what
each holds (images, migrations, log lines; a service's observation) interleaved in it. An
interleaved table's key begins with its parent's by name, so each table's own key column is named
for it (`repository_id`, `deployment_id`, `service_id`). Every write is DML, not a mutation:
Spanner's transactions read their own DML, as PostgreSQL's read their own writes, where buffered
mutations are seen only at commit. Creation times are `CURRENT_TIMESTAMP()`, which moves on between
a transaction's statements as `clock_timestamp()` does, so rows one transaction makes keep their
order. JSON is kept as Spanner normalizes it — keys sorted, no spaces — which callers compare by
value. `SELECT … FOR UPDATE` and DML's `THEN RETURN` serve as PostgreSQL's do. (`at` is a reserved
word in GoogleSQL: a log line's time is `event_at`.) The deployer's conformance suite passes on
Spanner Omni; the bus is then another backend's, and the other roles refuse `spanner` until theirs.

**ok control on Spanner, as built** (`control/db/spannerstore`), its database `<prefix>-control`. A
CL is recorded whether or not its repository is — the SCM's events and a request may each come first
— so CLs are a table of their own, keyed by repository and number, and what review keeps of a CL
(reviewers, label grants, comments, its log) is interleaved in it, going with it. A stack is walked
in one read-only transaction a level at a time, where PostgreSQL recurses; deleting a comment
deletes the replies under it a level at a time, where PostgreSQL's foreign key cascades. An inline
comment's anchor is a `CHECK` constraint, as on PostgreSQL. Its conformance suite passes on Spanner
Omni.

**ok automation on Spanner, as built** (`automation/store/spannerstore`), its database
`<prefix>-automation`. Plans, runs, submissions, and attempts are tables of their own, keyed by their
random IDs, what each holds interleaved in it (a run's pushed images, a submission's CLs, an
attempt's candidates). With no `SKIP LOCKED`, a queued run is claimed by reading the queue's head and
moving it to running in one transaction: two claims that read one head meet at its row, and Spanner
runs the second again, after the first, on the next head — the queue stays first in, first out. That
holds claims to one at a time: measured on Spanner Omni, one claimer takes 94.5 runs a second and
eight 1.9, which bounds the end-to-end scale test on Spanner (220 of its 300 repositories landed in
its seven minutes; on PostgreSQL, `SKIP LOCKED`, all of them in 36 seconds). Claims from random
places in the queue would not meet; that waits for the optimization after the phases are built. A
CL's pre runs are read through `run_cl`: unhinted, Spanner reads them by status, through
`run_leased`, locking every queued run, which every plan inserts one of. A patchset's
one pre plan is a `NULL_FILTERED` unique index on a generated column that is the patchset for a pre
plan and `NULL` for a post plan, where PostgreSQL has a partial index. A plan's JSON is kept as the
text the store wrote: nothing queries into it. A repository's rows are purged by partitioned DML,
each statement apart, as are the deployer's and ok control's: one transaction's mutations hold too
few, and a purged repository is written no more, so a statement run again deletes nothing more.

**ok identity on Spanner, as built** (`id/store/spannerstore`), its database `<prefix>-identity`.
PostgreSQL's tables, checks, and foreign keys, but for those Spanner has no form of: a fork's parent
set `NULL` as its parent goes, a repository's redirects deleted with it — which the backend does
itself — its grants interleaved in it. A key column is never updated, so a row whose key changes is
written anew: a synced group is keyed by its provider's ID, so a rename is an update; a renamed
synced team is written under its new name, its grants moved to it; a grant moved to another subject
is inserted and the old deleted. Case-insensitive uniqueness (SCIM's user and group names) is a
unique index on a generated `LOWER` column. Upserts are `INSERT OR UPDATE` and `INSERT OR IGNORE`,
or, where PostgreSQL's reads the row it updates (a provider's plain handles, kept once set; the
tenancy's moved-in organization; a sign-in failure's window), an update then an insert. The
directory's relay takes pending events on a lease — a holder and an expiry on each, in a
transaction of its own — publishes them outside any transaction, as Spanner aborts a transaction
left waiting and may run its function again, then deletes them, or lets them go if publishing
failed; a relay that dies holding them has them taken again once the lease lapses. With no `SKIP
LOCKED`, due deletions are read `FOR UPDATE`, two sweepers taking one after the other. Expired
sessions, ceremonies, device logins, and email links are swept by partitioned DML. The admin
commands open identity's store where the configuration says (`role.OpenBackend`).

**The SCM on Spanner, as built** (`scm/store/spannerstore`), its database `<prefix>-scm`, under
PostgreSQL's table and column names, so the store's suite reads and ages rows of either alike
(`testDB`; `OK_TEST_DATABASE=spanner` runs it on Spanner). A repository's rows are interleaved in it
`ON DELETE NO ACTION`: a row needs its repository, as PostgreSQL's foreign keys have it — so no
publish writes under a repository being deleted — and the backend deletes them before the
repository's row, where PostgreSQL's keys delete them with it. What hangs off one row goes with it,
as on PostgreSQL: an operation log's operations, views, heads, and tombstones (interleaved in it); a
network's packs and chunk releases (interleaved), and its dictionaries and deletion records; a CL's
patchsets, a commit's parent edges, and a workspace's checkpoints (by foreign keys); a checkpoint
whose parent goes has it set `NULL` by the backend. The store's transaction runs once — a
statement-based transaction, which the client does not run again; the store does when Spanner
aborts it (`Retryable`) — and its statements take turns, as a PostgreSQL connection's do. A
Spanner read-write transaction locks what it reads, where PostgreSQL's read committed does not: the
content index's generation, blocks, and log, which every writer appends to, are read outside it,
so writers do not conflict over them, and what holds a fold back meanwhile is the share lock on the
index's row, as on PostgreSQL. What PostgreSQL's identity columns and sequence count — a network's
packs and segments' ages, a content index's log rows, the dictionaries — are counters taken in a
transaction of their own, as a sequence hands out values, so no transaction holds one until it
commits. Times are kept to the microsecond, as on PostgreSQL, so a row reads the same from the tail
as from a segment; days are UTC dates, and a time compares with a day at its midnight, as
PostgreSQL compares them. GoogleSQL has neither recursive queries nor window functions: ancestry
walks go a generation of edges at a time, in one read, and thinning ranks checkpoints in the
backend. A composite key is matched as a `STRUCT` in `UNNEST`. The relay takes pending events on a
lease, as identity's does.

Spanner's limits, as measured on a real instance (Spanner Omni allows more in a statement than
Spanner does, so its suites cannot show them): a DML statement of at most 80,000 mutations — each
column a row writes, each index entry, each row a cascade deletes; a transaction of at most 100 MiB
of writes, however many statements; and about 100,000 rows deleted in one — 100,000 commit, 400,000
are refused. Every SCM statement over the rows the store hands it runs in chunks of up to 5,000,
and a delete by a condition a page of keys at a time, what a cascade would delete going first. A
compaction moves at most 4,096 records of each kind (`SegmentBatch`), all deleted in the
transaction that records its segments. A repository's tail, a deleted repository's content index,
and a network's rows go in transactions of up to 10,000 rows (`DeleteBatch`) before the one that
deletes their rows — nothing reads them meanwhile, as the directory deleted the repository before
the SCM purges it — and logs of no workspace, workspaces with no log, and thinned checkpoints, which
collection deletes outside a transaction, go in transactions of up to 10,000 too. A transaction's
content index updates go in log rows of at most 4 MiB (Spanner refuses a value over 10 MiB); on
Spanner, past its first 32 MiB of them — an upload pins 68 bytes of log a file, so one of 1.5
million files logs 100 MiB — the rest are staged in transactions of their own under a batch whose
marker the transaction commits, and read only once it has (`content_log_batch`). A fold deletes
the staged rows of a transaction that did not commit, those an hour older than its place with no
marker: staged as it is about to commit, a transaction's rows are never that old before it does —
one holding no lock on the index commits within the hour it read it, and one holding the lock
holds folds back. A fold writes what the log changed to runs in
the bucket, not rows: its transaction records them and the next generation, under a claim that
keeps folds of one index from overlapping (`docs/scm.md`, *Folds*). And
a landing that changed more than 20,000 paths leaves them to compaction, which writes them 10,000
to a transaction, its commit answered from its trees meanwhile (`docs/scm.md`, *Landing*).

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
  and a Bloom filter, under `nets/<network id>/segments/<kind>/<segment id>`.
- **The manifest** is the database's: the repository's live segments by kind and level, under a
  generation number.
- **Compaction** — a job on the queue, per repository, when its tail passes a size or an age —
  writes a segment from the tail, then in one transaction adds it to the manifest (if the generation
  is still the one it read) and deletes the tail rows it folded in. Levels merge the same way.
- **Kinds.** Objects (trees, symlinks, copy histories, and trees' sizes records, scm.md "Tree
  sizes"); the **commit graph** (each commit's parents, generation, change ID, and conflict flag,
  and the reverse edges — what `commit`, `commit_parent`, and `commit_path`'s ancestry walks read,
  as Git's commit-graph does); **path history** (`commit_path`); the **content index** (its log,
  `content_log`, is the tail; a fold writes what it changed to runs, a repository's pinned entries
  and its unpinned ones in kinds of their own — scm.md, "The content index"); **operation logs**;
  **events**.
- **Reads** check each kind's Bloom filters and fence indexes, kept in replica memory and on its
  disk, read the segment's block by range, and query the tail only for what no segment holds — so a
  read of anything not written in the last minutes asks the database nothing.
- **Large publishes** — an import, a vendored tree of millions of files — bypass the tail: the SCM
  writes the verified objects as segments, and the publish's transaction adds them to the manifest.

A repository's database rows are then its mutable state — `main` and its log, CLs and reviews, op
heads, workspaces and checkpoints, runs, the manifest — and its recent tail: ~50 MB.

### Segments, as built

The plan above is the shape; this is the design phase 3 builds, settled against the SCM's code.

**What moves, and what stays: segments hold what is pinned.** Everything reachable from `main` or
from any CL's patchset is pinned, and pinned is permanent (scm.md, "Garbage collection and
compaction"): no sweep ever takes it. So segments hold exactly the pinned objects, commits, and
index entries of a repository, and nothing else. What is not yet pinned — a person's work published
but not uploaded, kept fresh by touches and swept 15 days stale — stays in the database's tables
(the tail) with its `pinned` flag and freshness day, as today. When an upload or a landing pins it,
compaction later moves it into a segment and deletes its row. Consequences, each of which keeps a
proven part of the SCM unchanged:

- Garbage collection, the touch and refresh walks, and the pin walk work on rows exactly as now. A
  walk that reaches an object in a segment finds it pinned, and stops, as it stops at a pinned row.
  The mark stops there too, and no sweep candidate is ever in a segment.
- A segment is never rewritten to drop anything (until a repository is deleted, phase 4), so every
  replica caches its blocks forever.
- The tail is a repository's unpinned work plus what was pinned since the last compaction: bounded
  by its people's recent work, not by its history.
- Landed CLs go the same way. Nothing changes a CL once it lands, and main's log only grows, so
  compaction moves a landed CL — its row, its patchsets, and the move of main that landed it —
  into segments together; what stays is the CLs still in review or abandoned, which may change.
  Reads of CLs and of main's log look in the tail, then the segments, as every read does; a
  rollback, which locked the landed CLs it rolls back, locks the repository's row instead.

**Kinds.** Each kind is its own segments; a record is a key and a value, sorted by key.

| Kind | Key | Value | Today's rows |
|---|---|---|---|
| `object` | ID | kind, canonical body | `object` (trees, symlinks, copy histories) |
| `treesizes` | tree ID | sizes record | `tree_sizes` |
| `commitgraph` | ID | generation, parents | `commit`, `commit_parent`: what walks read, many records to a block |
| `commitbody` | ID | body | `commit`: the change ID, conflict flag, and the rest are in the body, and nothing reads the other columns |
| `copychild` | parent ID ‖ copy ID | — | `copy_parent`, read from the parent (the forward edge is in the copy's body) |
| `path` | path ‖ 0 ‖ generation ‖ commit ID | — | `commit_path`, main's path history: permanent as main is |
| `index` | ID | content index entry, or a tombstone | `content_log`, folded: a repository's pinned files and chunks, which forks inherit, and a network's every location |
| `indexunpinned` | ID | content index entry, or a tombstone | `content_log`, folded: a repository's unpinned files and chunks, a run a day, retired once garbage collection swept past its day |
| `event` | repository_seq | event | `event`: never pruned; moved once the relay has published it and it is ten minutes old |
| `op` | op log ‖ kind ‖ ID | last touched, an operation's view, body; or a tombstone | `operation`, `view`, once settled: touched before collection's window |
| `changelist` | `n` number; `c` change ID; `p` patchset commit ‖ number; `a` author ‖ 0 ‖ number; `s` parent ‖ number | the CL with its patchsets and landing; its number; — | `changelist`, `patchset`, once landed: no transition leaves landed, and an upload to it is refused |
| `mainlog` | `l` place; `h` commit ID | the move (old and new head, CL, patchset, who, when); its place | `head_log`, moved with the CL that landed it: main's log only grows |

Two need more than "pinned is permanent":

- **The content index.** A pinned chunk's location still changes when compaction repacks it. The
  move is logged in `content_log` as today, and kept, of a chunk the tail lacks, as its location
  alone; a lookup reads the tail, then the newest segment that has the key, the tail's move applied.
  The index's tail is folded — the GC pass's rebuild, and compaction's once enough would move — into
  blocks of the unpinned entries and an index segment of the pinned ones and the folded moves.
  Segments are ordered by age, not by when their rows were written: a merge's output is as new as
  its newest input, so a superseded record never wins.
- **Operation logs** are not pinned: `jj op abandon` and collection delete operations. An
  operation or view moves once settled — touched before collection's window, so a collection has
  decided whether anything reaches it. Collection deletes what it takes from the tail, and of what
  only a segment holds writes a tombstone (`op_tombstone`), which compaction moves into a segment
  too: a read takes the tail's row, then a tombstone in the tail, then the newest segment's record.
  Re-publishing one only a segment holds puts its row in the tail again, so collection's check of
  the last touch, under the log's lock, still keeps it. Merging drops the records of deleted logs,
  and, merging into the oldest level, the tombstones, what they bury dropped with them.

`commit.change_id`'s index and `commit_parent`'s reverse index have no reader, and no kind.

**The format.** One immutable file per segment, at
`nets/<network id>/segments/<kind>/<segment id>` — under its repository's network, whose every
manifest may name it (*Fork networks, as built*) — the ID the BLAKE3 of its bytes:

```
magic "OKSCMSEG", version u32 = 1, kind u8, nonce 16 random bytes   (a deleted key is never rewritten)
blocks    zstd frames of about 64 KiB of records: key length uvarint, key, value length uvarint, value
fence     per block: its first key, offset u64, stored length u32, raw length u32
bloom     a split-block Bloom filter over the keys — and, for kinds read by prefix (`copychild`,
          `path`), over their prefixes too — 10 bits an entry
meta      records u64; fence offset u64, length u32; bloom offset u64, length u32; first and last
          key; flags u8 (1: the filter holds prefixes)
tail      meta length u32, magic "OKSCMEND"
```

**The manifest.** `segment (repository_id, kind, id, level, records, size, created_at,
retired_at)`; a segment is live until retired. A request reads a repository's manifest from the
database once for its life (`store.WithRequestScope`), and again on a lookup that missed; a replica
keeps segments' readers and blocks, named by ID, which never change — never the manifest.

**Reads.** A batch of keys of a kind: the tail first, one query for the batch as today; then, for
the keys it lacks, each live segment of the kind, newest first, through its Bloom filter and fence
index — both cached in memory, loaded by one range read of the segment's end — reading the blocks
the batch needs with one range GET for each run of consecutive ones, several at once, cached
decompressed and keyed by segment and block, so never stale. A key
missing from both is looked for again after the manifest is refreshed: compaction may have moved
it since the tail was read. Ranged kinds (`path`, `copychild`, `event`, `op`) merge the tail's rows
with each segment's range, read after the tail with the manifest read anew, skipping a segment whose
filter holds no key with the range's prefix; a read of events whose page the tail holds without a
gap reads no segment. A walk over a range reads ahead — its first block as a lookup, then twice as
many consecutive blocks each GET as the last, up to 4 MiB, kept out of the cache — so a scan of a
segment (fsck's, garbage collection's, a merge's) costs a GET per 4 MiB, not per 64 KiB block. Within a transaction, the tail is read through the transaction, as every
read is.

**Compaction.** Keyed work (`segments`, per repository), marked when a pin commits and when the
tail passes a size; it, per kind:

1. reads up to 1 million of the tail's pinned rows (256 MiB at most), in key order;
2. writes them as a level-0 segment, and only then
3. in one transaction, adds the segment to the manifest and deletes those rows — each only if still
   pinned, which nothing undoes;
4. merges size-tiered: when a level holds four segments within a factor of four of each other's
   size, one segment of them replaces them at the next level, and they are retired; retired
   segments are deleted from the bucket after a day, as packs are after an hour, so a reader that
   located a key in one still reads it.

IDs are hashes, so every segment spans the whole key space; size tiers, unlike leveled ranges, never
rewrite more than the segments they merge, and a kind of n records has about log₄ n segments.

**The commit graph.** A walk over history (`Ancestry`) goes through the commit graph's segments in
memory, a generation of the walk at a time, and through the tail with one recursive query from
wherever it first needs the tail, as far as the tail goes: a query each time it enters the tail,
not each generation, so compacted history costs no query once its blocks are cached.

**Large publishes** still go through the tail, written with `COPY`: their objects are unpinned
until an upload or a landing pins them, and segments hold only what is pinned. An import pins at
once and compaction moves it right after, so its rows are transient.

**Garbage collection** never reads segments: its candidates are unpinned, so in the tail. Pack
compaction, which needs every chunk's location, scans the content index's segments with the tail's
log applied over them, a block at a time.

**Fork networks** come last in the phase (*Fork networks, as built*). A fork shares its parent's
packs and segments by reference, so chunk locations and the retirement of packs and segments become
the network's — a pack or segment goes only when no repository in the network refers to it — while
what a repository may read stays its own: its manifest (its parent's segments as they were at the
fork, by reference, then its own) and its content index.

### Packs and fork networks

- **Physical storage is per network; what a repository may read is per repository.** Packs and
  segments of chunks live under the fork network (`nets/<network id>/…`), so a fork's copy of its
  parent's content costs nothing. Each repository has a **presence set** — the files and objects it
  holds — and a read is answered only for what is in the requesting repository's set. A fork's set
  is copy-on-write: it starts as a reference to its parent's at the fork (O(1)), and each side then
  adds only to its own layer — what the fork publishes or syncs, what the parent lands. So a
  private fork's commits are never readable through the public parent, nor the parent's later
  commits through the fork until it syncs them.
- **A pack holds any of the network's chunks.** A reader is sent only ranges of the records its
  repository holds — URLs bound to their ranges — so a pack need not be one repository's, and
  repacking mixes a network's chunks freely (*Fork networks, as built*).
- **Garbage collection is per network**: each repository's marking yields what it still reaches; a
  chunk goes when no repository in the network reaches it; packs are rewritten as today.

### Fork networks, as built

The section above is the shape; this is the design the end of phase 3 builds, settled against the
SCM's code.

**What a network shares, and what a repository keeps.** A network shares bytes and the means of
reading them: packs, chunk locations, and dictionaries. Everything that says what a repository holds
stays the repository's, in its tail and in the segments its manifest names: its objects, commits,
trees' sizes, copy edges, its content index of files and chunks, `main` and its log, path history,
CLs, events, operation logs, workspaces. Those are its presence set. A read is answered from them
alone, so nothing another repository in the network holds is readable through it, even when the
bytes lie in the same pack.

| | Today | In a network |
|---|---|---|
| `pack`, `dictionary` | the repository's | the network's |
| chunk locations | in the content index's entries (`contentindex.Entry.Loc`) | the network's **chunk index**: chunk ID → location, with a tail, log, and segments of its own |
| content index | the repository's files and chunks, with locations | the files and chunks the repository holds, without locations |
| objects, commits, trees' sizes, copy edges, path history, main's log, CLs, events, op logs | the repository's tail and segments | the repository's tail and segments — its own, and those it inherited |
| segment objects | `repos/<repository>/segments/<kind>/<id>` | `nets/<network>/segments/<kind>/<id>`, which any manifest in the network may name |
| pack objects | `repos/<repository>/packs/<id>` | `nets/<network>/packs/<id>` |

A network has a random 63-bit ID of its own, as a repository has. A repository with no forks is a
network of one: its keys and tables move, locating a chunk takes one more lookup in memory (its
network's chunk index after its content index), and it collects garbage as today.

**Forking costs the parent's segment count, not its size: a fork inherits by reference.** To fork,
the SCM

1. reads the parent's `main` and its place in main's history;
2. compacts the parent's tail into segments, pass after pass, until a pass moves every pinned row
   and landed CL it found, so that everything pinned by then — `main` at that place with it, and
   main's log and path history up to it — is in segments;
3. in one transaction, makes the fork in the parent's network with that `main`, and adds to its
   manifest a reference to each of the parent's live segments of the kinds a fork inherits,
   marked inherited — the parent's own and those it inherited, so a chain of forks stays one layer
   deep.

A fork inherits objects, trees' sizes, copy edges, the commit graph and bodies, the content index,
path history, and main's log. Nothing else is copied: the parent's unpinned tail — its people's
work not uploaded or landed — is not inherited, and CLs, events, operation logs, workspaces, and
checkpoints start empty. The kinds read along main — `path` and `mainlog` — are read in an inherited
segment only up to the fork's place, since compaction may have moved later landings into the same
segments; every inherited reference keeps the bound it was inherited with, so a fork of a fork
reads its grandparent's history only as far as its parent did.

**Inherited segments are never merged.** Merging rewrites only segments the repository wrote, so a
fork shares its parent's segments for as long as it lives instead of copying them into its first
merges. A read searches its own and inherited segments alike, newest first: the parent's
log₄ n more at most.

**Copy on write: nothing a parent does changes its forks.** A fork names immutable segments, reads
chunks through the network's locations, and decodes with dictionaries that are never deleted; what
its parent does touches none of them in place:

- **Landing, uploading, publishing.** New tail rows and segments the fork does not name.
- **Compaction and merges.** A merge writes new segments and retires the parent's references to the
  old ones — only the parent's. A segment object leaves the bucket only when no manifest in the
  network names it, live or retired within its grace: deleting retired segments checks every
  manifest in the network, never one repository's.
- **Repacking** is the network's: it moves chunks and records where in the network's chunk index,
  which every repository reads through; a pack leaves only when no location names it.
- **Garbage collection.** A repository's sweep takes only its own unpinned tail; a chunk it sweeps
  is released, and the network drops its location only when no repository holds it — a fork's
  inherited segments hold it. Segments hold only what is pinned, so no sweep, the fork's or the
  parent's, ever takes an inherited record.
- **fsck.** A repository's fsck verifies what it holds — its tail, its own segments, and its
  inherited ones — and never deletes or rewrites any of it; its repair adds chunk locations to the
  network's index from the packs, as today's adds index entries. The network's fsck verifies packs
  against the chunk index, and counts an object in the bucket an orphan only when no manifest in the
  network names it, so a segment only a fork names is not one.
- **Deletion.** The parent's manifest references, tail, and rows go; the segments its forks name
  stay, as do the dictionaries; each fork's parent link is cleared, and it stands alone with
  everything it inherited until *GitHub's rules* give it a new upstream. Nothing is copied.
- **Visibility changes and renames** move forks in the fork tree (*GitHub's rules*, below); no byte
  of their storage moves.

**Syncing** — a fork taking its parent's newer work — compacts the parent's tail as forking does,
replaces the fork's inherited references with the parent's live segments as they are now (those of
`path` and `mainlog` keeping the fork's first bound), and records the sync. It too costs the
parent's segment count. The fork's `main` then moves by a landing like any other, which finds the
parent's commits pinned and stops, and records history of its own from there.

**Reads** keep their order — the repository's tail, then its manifest's segments, own and inherited
— and locate a chunk's bytes in the network's chunk index. A file is read only if the repository's
content index holds it.

**Writes, and no dedup oracle.** A publish asks for the chunks the *repository's* content index
lacks, as today (`BeginPublish`'s check of `tChunk`), never the network's chunk index: a client
learns nothing of what another repository in its network holds, so a private fork's content cannot
be confirmed by guessing it and offering its chunks. A chunk uploaded that the network already
stores is written as uploaded; the chunk index keeps its existing location, and repacking reclaims
the copy. A repository gains files without their bytes passing only from a repository its actor may
read, on the server: a fork, a sync, and a CL uploaded from a fork to its parent.

**Garbage collection** stays per repository — its own unpinned stale rows and index entries are the
candidates, its own roots mark, its sweep re-checks staleness in its transaction — with one change:
sweeping a chunk from a repository's content index **releases** it to the network (a
`chunk_release` row, in the sweep's transaction) instead of dropping its location. The network's
collection drops a released chunk's location only when no repository in the network holds it — in
its tail, or in a segment of its manifest through the segment's Bloom filter, each segment checked
once however many manifests name it — in a transaction holding the network's chunk-index lock,
which a publish takes before it records a location, so a publish that relied on a chunk finds it
gone and retries, as it does today when a sweep wins (`touchHeld`). A network of one repository
drops the location in the sweep's own transaction, as today. Packs are retired and deleted per
network, when no location in the chunk index names them, as they are per repository today
(`deleteRetiredPacks`).

**Pack compaction** is per network: the chunk index says what is live and records where it moves.
Chunks are ordered by the paths of `main` of the repository whose collection repacks them
(`mainPaths`); any order is correct, and each repository's pass serves its own reads.

**Direct reads: URLs bound to ranges.** A URL is signed for one byte range of a pack, its `Range`
header in the signature — S3's `X-Amz-SignedHeaders` (minio-go's `PresignHeader`), GCS's V4 signed
headers (`SignedURLOptions.Headers`) — so the store refuses it for any other range. The SCM signs
only runs of whole records the reader's repository holds: never the gaps a read through the SCM
merges across (`fetchChunks` reads up to 128 KiB between chunks), which may hold another
repository's. So a pack need not be one repository's. `blob.Store` gains `SignRange(ctx, key,
offset, length, ttl)`, and its conformance suite proves against MinIO, which checks signatures as S3
does, that a URL signed for one range reads no other range and nothing without its `Range`; GCS
enforces signed headers the same way (its emulator checks no signatures, so CI cannot show it).
Nothing in the SCM signs URLs yet (`SignGet` has no caller there), so no reader changes.

**Dictionaries.** A dictionary holds fragments of the files it was trained on (`trainDictionary`
samples the content index), and a reader of stored chunks fetches it to decode them. So:

- **One repository trains**: the network's first, and once it is deleted, the network's oldest
  remaining. It trains only from content it has pinned — landed or uploaded, never work published
  but not uploaded.
- **A repository may read** the dictionaries it trained, and those its source could read when it
  last forked or synced from it, trained no later than then: it records, for each trainer, the
  newest it may read. Each was trained only on content the repository inherited, so a fork of a
  fork, a sibling, or a repository whose tree a deletion re-rooted never reads a dictionary of
  content it never held.
- **Repacking compresses with the network's newest dictionary.** A reader asking for stored chunks
  compressed with one it may not read gets those chunks raw, as `StoredChunks` already sends a
  chunk compressed with a dictionary, and no URL for their range; `ReadDictionary` refuses it the
  dictionary.

A fork's own small files compress with the trainer's dictionaries, a little worse than with their
own: that is the price of no dictionary ever carrying one repository's content to another's
readers.

**GitHub's rules.** Who may fork, and what happens to forks, follows GitHub (docs.github.com,
"Forks", as of October 2026):

- A fork's visibility is its upstream's, and cannot change on its own.
- Private forks inherit the upstream's team grants, not its individual ones; the upstream's owners
  may read every fork.
- Removing someone's access to a private repository deletes their forks of it, except forks an
  organization holds.
- Deleting a private repository deletes its private forks. Deleting a public one makes an active
  public fork the new upstream, and the other forks its forks.
- Making a public repository private leaves its public forks public, in a fork tree of their own;
  making a private one public leaves each private fork private, a fork tree of its own.
- An internal repository allows one level of forking.

These are `Authorize`'s and deletion's, phase 4. To storage each is a change to the fork tree —
parent links and the root — and none moves a byte: a fork that leaves its tree keeps its storage in
the network, shared and safe, since presence alone keeps each repository's content its own. The
storage keeps a fork whatever happens to its parent; whether the fork is then deleted is the rules'.
**Unlike GitHub**, where "Git data from any repository in a network may be accessed from any
repository in the same network … even after a fork is deleted", no repository here reads what its
own presence does not hold (*Decisions*, 13): a fork's commits are never readable through its
upstream, nor a deleted fork's through anything.

**Deleting** — the storage half, which phase 4's deletion calls (`Store.DeleteRepository`): one
transaction deletes the repository's row and tail, clears its forks' parent links, hands the
network's dictionary training on, and records the deletion (`repository_deletion`). Then every chunk
the repository held is released, its segment references are retired — an object goes after its
grace only if no other manifest names it — and its content index goes; the network's collection
drops the locations no other repository holds, pinned ones too, a drop of a location folded into
the chunk index's segments becoming a tombstone there at the next fold. A deletion a replica stops
short of is finished by the repository's purge, which the transaction that deleted its row marked
(`FinishDeletion`). A network whose last
repository goes is deleted whole, `nets/<network>/` with it.

**Done when** `TestForkNetwork` forks a repository, works on both sides, and checks that forking
writes rows in proportion to the parent's segments, not its content; that the fork reads the
parent's `main`, files, log, path history, and annotations as of the fork; that neither reads by ID
an object, commit, or file only the other holds, though their chunks share packs; that the parent's
later work is unreadable through the fork until a sync, and readable after; that a publish into one
never skips a chunk only the other holds; that the parent's compaction, merges, repacking,
collection, fsck with repair, and deletion each leave the fork reading what it read before, fsck
clean; and that a chunk goes once no repository holds it. `TestLargeRepository` forks its
repository and checks the fork costs the same at any size.

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

**A request that fails for a moment is made again** — a 429, a 5xx, a dropped connection — writes
too: every key's writers write what any of them may (a key named by its content, or a cache entry
any of whose values is right), so a put made twice stores what one would. GCS's client makes again
only what it can tell is idempotent, which an unconditional write is not, so its buckets retry
always, up to six attempts; minio makes a request again only with a body it can rewind, so a put
from one that cannot is held in memory up to minio's single-request size (16 MiB), past which minio
sends held parts. The secret stores do likewise: Vault's requests, Secret Manager's writes (its
client retries reads alone), and AWS Secrets Manager's SDK, raised to six attempts. Staging's
publish failed on one GCS 503 on a pack, in 12,500.

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
  SCM decides who may read what; it does not carry the bytes. A URL is signed for a range of a pack,
  the range in its signature, and only for records the reader's repository holds (*Fork networks,
  as built*).
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

Where ok's state lives, and so what the bus is and is not, is [state.md](state.md): object storage,
the database, or a queue item, and nothing else. The bus is queues: work, the relays of logs to
their followers, and the replies to the live requests waiting on a change.

### The per-repository log

Every transaction that changes a repository or a CL appends its events to that repository's log:
rows keyed `(repository, seq)`, in commit order with no gap, per repository, so two repositories'
writes never wait on each other. On PostgreSQL the place comes from the repository's own counter,
taken as the transaction's last lock; on Spanner, where a row every request of a repository
writes aborts the requests that meet on it, the repository's sequencing pass gives it just after
the transaction commits (scm.md, "Sequencing"). A repository's busiest writer, a monorepo landing
~500 commits a minute (automation.md, "The landing queue"), is about ten events a second.

The log is the source of truth; the bus only carries it:

- **The relay** relays every event at least once. The transaction that places events — on
  PostgreSQL the writer's own, on Spanner the repository's sequencing pass — inserts their keys
  into a pending table, and enqueues, as it is about to commit, a relay job naming the repository
  and the places it gave (`work.Pend`). Whichever replica leases the job relays the repository's
  pending events and clears them, and acknowledges the job only once its own are gone from pending:
  relayed by it, or by another relay, which holds them meanwhile. To relay a page of events is to
  enqueue, on each follower's queue, a job naming each repository whose log it holds, and the last
  place of it (*Followers*). On PostgreSQL a relay takes
  pending rows `FOR UPDATE SKIP LOCKED` and deletes them in the transaction that took them, so one
  that dies first leaves its rows, and its job, for the next; on Spanner it reads pending events
  without a lock, takes those no relay holds for a lease by key, publishes them, and deletes them by
  key (scm.md, "Events"). A job that comes before its transaction committed — the job's places past
  the repository's counter, which the transaction holds until it commits — looks again shortly,
  until it is a minute old. No relay looks for pending events; the directory's relay is the same.
- **History** compacts into event segments, so the log is still never pruned (scm.md, "Events"): a
  consumer that needs old events reads them from the SCM, which reads segments.

### Followers

ok control, ok automation, and the deployer follow the SCM's log of each repository; they and the
SCM follow the directory's (`internal/bus/follow`). A follower registers with the log's source as
it starts (`Events.Follow`, `DirectoryEvents.Follow`: the caller's role, a row of the source's
`follower` table), and from then on the source's relay enqueues, on the follower's queue
(`<role>.relay.<topic>`), a job for each repository whose log grew, its ID the repository and the
last place relayed, so a backend that drops duplicate IDs drops a second relay of one page. Any
worker of the follower takes the job and marks the repository's keyed work (`follow.<topic>`);
whichever worker takes that applies the repository's events from its cursor to the log's end,
read from the source a page at a time (`Events.Read`), and goes round again if it was marked
meanwhile, so nothing relayed while it applied waits. Work that fails is done again a second later.
The first time a follower follows a log — a new role, or one coming back from a release that
followed a stream — it marks, once for its database (`work.Once`), each repository whose log reaches
past its cursor (`Events.Heads`): what was relayed before it registered.

### Consumers

A consumer applies an event in its own transaction with its cursor for that key: `UPDATE cursor SET
seq = s WHERE key = k AND seq = s - 1`. One row updated: applied, exactly once. None, and the cursor
is at or past `s`: a duplicate, acknowledged. None, and the cursor is behind: a gap, filled by
reading `(cursor, s)` from the SCM's log (`Events.Read(repository, after)`) and applying it in order
first. So a relay may come twice or late, and consumers never apply twice or skip. A new consumer
bootstraps from snapshots (the SCM's current state of each repository) rather than replaying a
decade of every repository's history.

A repository's events are applied by one worker at a time, the one holding its `follow.<topic>`
work; a consumer that also holds the repository for other reasons claims it as it applies
(automation's `feed`, control's `control-feed`, which a purge takes too), and finding it held,
answers `follow.ErrBusy`, and the work is done again a moment later. A consumer that applies a run of
events at once (`follow.BatchApplier`; ok control's) is handed each page of the log it reads: ok control applies a page of a
repository's events in one transaction with its cursor, reading each CL they name once beforehand;
ok automation applies a page's CLs' events beside each other, each CL's in order — main's moves,
which name no CL and whose recording reads the landings recorded before them, each in its place,
after everything before it and before everything after — and moves the cursor past those applied,
to the first that failed. Its events' effects are their CL's and its
stack's, each a transaction under the CL's lock acting on what the SCM says now, so different CLs'
events commute, and a page applied again applies each CL's events in order again: one at a time,
a repository's events applied at about five a second, and a herd of 3,000 uploads waited eleven
minutes for pre.
How far each consumer runs behind is a metric (`ok_control_feed_lag_seconds`,
`ok_automation_feed_lag_seconds`, by event kind).

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
	// Lease hands out up to max jobs, each invisible to others for visibility: as soon as one is
	// ready, waiting up to a time for the first.
	Lease(ctx context.Context, queue string, max int, visibility time.Duration) ([]Leased, error)
	// Inbox opens a live request's inbox: a queue whose address any process enqueues replies on,
	// until it closes (state.md, "Waiting").
	Inbox(ctx context.Context) (Inbox, error)
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

A process's inboxes share one queue of the process's own (`inbox.<random>`), which it leases while
any is open, handing each reply to the inbox it names; a reply to an inbox that closed is dropped.
A live request that waits — a watch of a CL, a page's live regions, a log followed, an upload
waiting for its CL's number, an action waiting for the feed — registers its inbox, in the database
of each service whose writes change what it waits on, under the subjects it waits on
(`internal/wait`), and reads what it waits for after. A transaction that changes a subject pends a
step naming it before it commits; the worker that takes the step replies to every inbox registered
under the subject. Another role's request registers through the service's `Waits` RPCs (ok
automation's and the deployer's, to ok control). A registration lapses unless its request renews
it, every third of two minutes, and a step due as it lapses removes it.

### Backends

| | Queue |
|---|---|
| **NATS JetStream** (`internal/bus/natsbus`) | One stream with work-queue retention, a subject and a pull consumer per queue, `MaxDeliver` its attempts; the ack wait the floor of a lease, which renews itself until its visibility passes; message schedules for jobs not due; the duplicate window for IDs; dead letters a stream of their own; a process's inboxes a consumer of its own, which the server removes an hour after it was last leased |
| **Google Pub/Sub** (`internal/bus/pubsubbus`) | A topic and a subscription read by synchronous pulls; the ack deadline the lease, renewed until its visibility (at least ten seconds) passes; a dead-letter policy for Pub/Sub's delivery counts, the last attempt's Nack dead-lettered by the client (Pub/Sub's own needs five); a delay topic per queue for jobs not due, moved on by `Lease`; no duplicate IDs dropped; a process's inboxes its subscription, filtered to its own, to one topic of replies, which Pub/Sub removes a day after it was last leased |
| **AWS** (`internal/bus/awsbus`) | SQS standard queues: nearly unlimited throughput, the receive's visibility the lease (up to 12 hours), retention up to 14 days, a redrive policy and a dead-letter queue; delays past SQS's 15 minutes sent again carrying their attempts; no duplicate IDs dropped. Bodies travel in a binary message attribute, SQS's being text. A process's inboxes a queue of its own, tagged as it is leased, which any process making its own removes an hour after |

There is no bus in PostgreSQL — its notifications are lost while no one listens — nor on Kafka,
which acknowledges no single job; and no streams or signals: what reached other services as a
stream's events is a relay's job on each follower's queue (*Followers*), and what woke a replica's
waiters or refreshed its caches is a registered inbox (state.md, *Waiting*), or a read of the
database each request.

## Work without leaders

Every singleton becomes keyed work, leased on demand:

- **Per key.** A `work(kind, key)` row (`internal/work`) and a job on the bus: the transaction
  that changes what the work acts on marks the row dirty and enqueues a job naming the work on its
  kind's queue (`work.<role>.<kind>`) before it commits, so a mark is never without its job. A
  replica's lease takes the job, then the row's lease — which waits for a mark's transaction still
  open: the mark holds the cells a take writes (a row lock on PostgreSQL, the mark's read on
  Spanner) — clears the mark, does the work, and loops until it stays clean: a mark made while it
  works is seen as it lets go, under the row's lock, so none is lost. A job that finds the work held
  under another job leaves the mark to that holder; one that finds it claimed (`work.Claim`, which
  holds no job) looks again once the claim's lease may have ended; one whose work is clean — done,
  or its mark's transaction never committed — costs a look. The holder keeps its job leased as it
  renews the row's lease, past it: should the holder die, its job comes back once its lease has
  lapsed, and the replica that takes it takes the work over. What time decides is a mark due then
  (`MarkAt`), its job delayed to then; a set that falls due by a schedule — the SCM's daily
  collection, ok secrets', a purge at a retention's end — is a chain: each run books the next from
  the work's own state, as the repository's making booked the first, so a booking folded into a
  sooner one is booked again when that one runs. Nothing looks for work: no replica reads the work
  table but for the key a job names. Many replicas drive many keys; one drives each.
- **Steps.** What a transaction leaves to do once it commits, which no row's lease guards — publish
  the events it placed, run a sequencing pass should its writer stop, re-derive the approvals its
  events changed — is a step: a job it enqueues as it is about to commit (`work.Pend`), whose
  consumer (`work.Consume`) finds what the transaction wrote, or, its commit not seen yet, looks
  again shortly until the job is a minute old. A step that fails is a fresh job a while later; a job
  put off any number of times is a copy of its own (`bus.PutOff`, `base~n`), so no backend's dedupe
  drops it, and none spends the attempts toward a dead letter.
- **Upgrades.** A release before this one marked work with no job and looked for it, so what it
  marked as it stopped would be brought by nothing. Each kind's runner books its adoption, once for
  the life of the database, fifteen minutes after the first replica of this release starts
  (`work.UpgradeAfter`), by when the release before has stopped: a job for every piece of work
  marked, or held by a holder that died (`work.Store.Marked`). What a release before booked by
  sweeps — repositories' collections and purges, identity providers' syncs, moves' steps, moved
  repositories' purges, events pending, queued runs with no job, finished runs' logs, the
  deployer's plans, turns, steps, and retirements — is booked by the same delay, once
  (`work.Once`).

| Today | Becomes |
|---|---|
| automation `queue` (the landing queue, one replica) | Per repository: each repository's queue driven under its own lease |
| automation `feed`, control `feed`, deploy `feed` | Stream consumer groups with per-repository cursors |
| automation `buildcache` | Nothing: the lock was taken by no code. A cache's sweep, once caches are per organization (phase 8), is keyed work per organization |
| deploy `leader` | Planning per repository; starting decided by the database; stepping per deployment (below) |
| deploy `observer` | Per service: its workload read under its own lease, due ten seconds after the last read |
| scm `gc` | Per repository and network: each pass books the next, as its retired packs' grace ends or its daily slot comes |
| identity `sync` | The installation's, and each organization's, keyed work booked for its interval and soon after a SCIM push; a sync asked for waits for the one under way to let go of its claim (*Shared primitives*); its plan applied fenced |

The landing queue and the feeds went first (phase 2), and the SCM's garbage collection with them.
The rest go in phase 6, so that no role keeps a leader: a leader is one replica's throughput for
the whole role, a failover that waits out its lease, a stalled holder that keeps acting after its
lease has passed, and a singleton a cell cannot split.

- **The deployer.** Its leader planned every repository and decided which waiting deployments
  start, from what every running one occupies — its deploy, its workload, and, for one that runs
  alone, everything. Planning becomes per-repository work, marked when main moves. Starting becomes
  a transaction: it reads what is running, and either records the deployment's claims and starts it
  or leaves it waiting, so the database, not one replica's memory, says that two deployments never
  hold one target; any replica that finds a waiting deployment tries. Stepping a deployment is per
  deployment, as its lock already makes it, and a deployment of the deployer itself is stepped by the
  copy that started it, as today. Deploy agents (phase 10) change where steps run, not this.
- **Fencing.** Work whose late write could do harm writes fenced (*Shared primitives*): a
  deployment's steps, the landing queue's landings, garbage collection's deletions, compaction's
  commits, a sync's plan. Work whose steps are safe to repeat need not.
- **Locks are not leaders.** The lock remains for exclusion that is not keyed work — a migration,
  allocating names, planning a CL's patchset — each held for that one act, never as a role.

**No leaders, as built.** A fence (`data.Fence`) names a lease — a lock's, by its refresh ID, or
keyed work's, by its holder — and rides the context its holder works with: `lock.LockFn` and
`work.Runner` and `work.Claim` put theirs there, and the deployer's deployment lock returns its
own. Every role's `Txn` on PostgreSQL checks the fence its context carries first, reading the
lease's row `FOR SHARE` — held by the lease's holder, within its lease — so that row cannot be taken
over until the transaction ends, and a transaction under a lease its holder no longer holds fails
with `data.ErrFenced` and commits nothing. On Spanner, which locks cells, a fence on keyed work reads
the holder's cell alone, which a takeover writes, and a mark the mark's cells alone: neither reads
the lease's end, which the holder's renewals write, so neither holds a renewal back — a
compaction's every fenced batch did, beside every upload's mark. The deployer writes its steps in a transaction when
fenced, which is one statement each otherwise. No deployer replica looks for work; each piece is
keyed work its own transactions mark. It plans each repository as its planning (`plan`) comes —
marked as the feed records main's move, as the directory names the repository, and as its owner's
deploy agent connects — and looks again once it holds it; starts deployments in `StartTurns`, one
transaction at a time across replicas (`lock.InTx`), as the cell's turns (`turns`) come — marked as
a deployment is queued or ends, or a repository's gate opens — which reads what each running
deployment holds, claims what each it starts holds, and marks each one's steps; steps each running
deployment under its lock as its steps (`step`) come — marked as it starts, as a person halts it or
allows its hazards, as its migration ends, and booked by each step that waits on something outside
the database (its pushes, its roll's pods; `engine.Look`, five seconds) for its next look — a
deployment of the deployer itself on `step-<color>`, stepped by the copy that started it alone;
retires the old copy a deployment of the deployer replaced as `retire` comes, marked as it
succeeds; and reads each service under its `observe` work, which each read marks due ten seconds
after it ends — first marked by the plan that declares the service, and brought back by its job
should its reader die reading. Automation's build-cache leader, and ok control's lock, had no
callers, and are gone.

## Automation at hosted scale

### Isolation

Every run gets a Firecracker microVM — on hosts with KVM, bare metal or nested virtualization — with
its own kernel, an ephemeral root, and no network beyond what its phase allows, on hosts that run
nothing else. Bazel's incrementality is kept within a trust domain only: a slot's output base and
checkout survive between runs of one repository (and, for post, only of `main`'s line); never
between repositories, never from a CL's pre into post.

### Scheduling and fairness

- **Dispatch.** Runs are recorded as today, queued in the cell's automation database, and each is
  enqueued, as it is queued, on the dispatch queue of the worker pool its job needs (Linux x86 or
  Arm, macOS, larger machines) — post's and deploys' runs on a first queue the workers look at
  before their pool's.
- **Execution.** A worker's lease takes the next job and claims its run in the database
  (compare-and-set on its status), runs it, and extends its lease while it runs. A worker that dies
  lets its claim lapse, and the run is queued and dispatched again. An account at its quota
  (concurrent runs, minutes) has its run set aside until a run of its stops: quotas, not the order
  of the queue, keep one organization's flood from holding another's runs.
- **Pools** scale on their dispatch queues' depth and age.
- **The landing queue** runs per repository (*Work without leaders*), so a million repositories'
  queues run side by side, each as automation.md describes.

### Logs, secrets, and caches

- **Logs** stream from the worker to object storage in segments of a few seconds, under the run; a
  live view tails the newest segment and the worker's last lines through the cell. `log_chunk` goes
  (phase 2 writes segments, about one a second per run, gathered into one object when the run
  ends; the live view reads the segments).
- **Secrets** ([secrets.md](secrets.md)) are held by a role of their own, `ok secrets`, each value
  sealed under a key of its own, wrapped by its repository's key, wrapped by a root in the
  cloud's KMS (or an organization's own); people unseal each read, and deploys of `main` and post
  runs of submitted changes read what a folder grants them — pre never.
- **Caches.** ok serves Bazel's remote cache (the Remote Execution API's content store and action
  cache), backed by object storage and the zone's cache tier, namespaced per organization; a CL's
  pre from outside the organization reads it but writes only to a namespace of its own, so no one
  poisons another's results.

### Hosted automation, as built

Phase 8's design, settled against ok automation's code before it is built. Today every queued run
is one queue: a worker's lease takes the highest-priority, oldest run of any repository; every
worker takes every repository's runs in slots it keeps from run to run, isolated by a mount
namespace and bubblewrap; and a remote cache is whatever `worker.bazel_flags` names. Secrets have
a design of their own ([secrets.md](secrets.md)).

**Accounts.** A run's account is its repository's owner — an organization, or a person — which
ok automation keeps from the directory's events (`repository_owner`, every repository's, as every
cell's ok control keeps every repository's name), and which the claim charges. A repository whose
events have not arrived yet counts under no account.

**Quotas.** ok identity keeps an account's quota (`account_quotas`): its concurrent runs, its CI
minutes in a calendar month (UTC) — `identity.default_quota` for an account with none (zero is
unlimited). Administrators set them (`Admin.SetQuota`,
`ok admin quota ACCOUNT`); ok automation reads them (`Quotas.GetQuotas`), kept 30 seconds. Each
cell's ok automation counts each account's run seconds a month (`account_usage`), charged by the
database's clock as each run that ran finishes or is given back. An account's repositories may be
in several cells, so a quota holds across them: every replica of each cell's ok automation reports
the cell's use this month — each account's seconds and running runs — to the directory every 15
seconds (`Quotas.ReportUsage`, idempotent: every replica reports the same totals), and a quota
read from the directory carries the account's use in the other cells (running runs only from
reports of the last two minutes, so a cell that stopped holds no one back). A cell admits a run
while its own count and the others' reports together stay within the quota: exact within a cell,
and across cells within a report and a cache, so at worst an account runs over its concurrent
quota by what the other cells admitted in the last 45 seconds.

**Pools.** A job runs in a pool of workers: the one its workflow job names (`pool:`), else its
repository's (`.ok/repository.yaml` `pool:`, read from main as the workflow files are, so a change
does not choose where it runs), else `default`; a planning run in `automation.plan_pool`. A worker
serves one pool (`worker.pool`). The run records its pool, and a worker's lease names it.

**Dispatch, and admission at the claim.** Every queued run is a job on its pool's dispatch queue
on the bus (`automation.dispatch.<pool>`), enqueued before the transaction that queued it — started,
planned, rerun, given back — commits: a run is never queued without its job. The run names the one
job that dispatches it now (`run.dispatch_job`). A worker's lease takes the next job — the bus
hands each to one consumer — and claims the run it names in a transaction that locks only that
run, only while the run is queued and names that job. Workers added to a pool add as many
consumers, and nothing they all contend on: no claim reads a queue's head, and none races another
for a run. A job is a pointer, the run's row the truth: a delivery of any job but the run's own — a
copy of an attempt that did not commit, one a release replaced — is acknowledged and dropped where
it is leased, as is the job of a run no longer queued; one whose run is not there yet, its
transaction still committing, is returned, until it is a minute old. Runs are taken in the order
they were queued; there is no fair share among accounts — an account's quota is what bounds it. An
account with a concurrent quota runs each run in a **slot** of its own (`account_slot`), at most its
quota — less what the other cells report — of them: the claim takes a free slot with the run, an
insert another claim's refuses, and the run lets it go as it stops running. A run the claim finds
its account at its quota, or past its minutes, for **waits**: its job is enqueued again, due five
seconds on (a minute, past its minutes), and held up behind it is nothing — the next job is another
account's run. The queue's own schedule brings it back; nothing scans for it.
An account's month's seconds are kept in sixteen rows, a run charged to the one its ID picks, so
runs ending at once write apart. The claim gates the run's repository: a run of a repository a move
holds is dropped there, and dispatched again where the repository arrives, or here once the move is
called off. A repository the directory's events have not named counts under no account, "".

**The run's lease is its job's.** The bus's lease on the job's delivery is the run's lease while it
runs: the run records the delivery's handle (`run.lease_handle`), each heartbeat — whichever
replica it reaches — extends it (`bus.Queue.Resume`, on every backend), and the run's outcome, or
its release, acknowledges it. A worker that stops heartbeating lets the delivery lapse, and the bus
delivers the job again: the lease that takes it takes the run over — its next attempt, with a fresh
log, or, on its last, an error; one its worker was asked to stop ends canceled. No sweep looks for
lapsed leases, nor for runs whose jobs were lost: a job is enqueued in its run's transaction, and
the bus redelivers what its consumer did not finish. A finished run's log is stored by a job of its
own (`automation.logs`), enqueued in the transaction that finished it and done as it finishes —
again by whichever replica leases it, should that fail. Once the release before this one has
stopped, one replica dispatches the queued runs it left with no job, and stores their logs
(*Work without leaders*, "Upgrades").

**Pools scale.** ok automation's scaler (keyed work `pools`, every 15 seconds) sets each pool's
worker Deployment's replicas through Kubernetes' scale subresource: its running and claimable runs
(queued, of accounts under their quotas) over each worker's slots, within the pool's bounds, down
only once the pool needed fewer for five minutes (`automation.pools`: name, namespace, deployment,
slots, min, max). Each pool's queued and running runs and its oldest queued run's age are metrics.

**Isolation.** Every run gets a microVM: Cloud Hypervisor on KVM. Firecracker, the plan's, has no
virtio-fs, so the worker's checkouts and output bases could reach a guest only as block devices
built and copied for each run; Cloud Hypervisor, of the same rust-vmm lineage and Kata Containers'
VMM, shares host directories through virtiofsd. The worker — the host — keeps what is trusted: its
credentials, the SCM, checkouts, logs. The guest has its own kernel (`worker.microvm.kernel`), an
ephemeral root (a tmpfs), the host's system directories read-only (`/usr`, and
`worker.microvm.share`), and no network device. virtiofsd shares, each enforced read-only on the
host where it is: the run's trust domain's checkout (read-only), its output base (read-write), and
the run's scratch (read-write). Commands reach the guest over vsock: every Bazel command already
runs under the worker's wrapper, which becomes `ok worker vm-exec`, and the guest's init is ok's
own static binary (`ok worker guest`). Its only way out is a proxy on the host, over vsock
(`HTTPS_PROXY`), which allows the hosts `worker.microvm.egress` names — Bazel's downloads — and,
for a push run, `worker.microvm.push_egress`; and the remote cache, through `--remote_proxy`. A
microVM lives for its run: Bazel's server starts with it, its output base what makes a build
incremental.

**Trust domains.** A worker keeps a checkout and an output base for each trust domain: a
repository's pre (pre's runs, manual runs, and their plans) or its post (post's runs, pushes, and
their plans) — never across repositories, never from pre into post. A run takes an idle copy of
its domain's (the most recently used, whose output base is warmest), or a new one; copies go least
recently used first while the worker keeps more than `worker.microvm.domains` or its filesystem has
less than `worker.microvm.reserve_gib` free — bounds a worker checks after each run without walking
output bases of hundreds of thousands of files. The build graphs a worker keeps are kept by domain
too, and so is Bazel's home: its install base, repository cache, and bazelisk's downloads, unless
an installation shares a cache of Bazel's releases read-only (`worker.microvm.share`,
`BAZELISK_HOME`, `USE_BAZEL_VERSION`).

**The remote cache.** ok automation serves Bazel's remote cache — the Remote Execution API's
`Capabilities`, `ContentAddressableStorage`, and `ActionCache`, and `ByteStream` — to workers alone,
over its logs' bucket (`cache/<namespace>/cas/<sha256>`, `…/ac/<sha256>`) and a memory tier in each
replica: a blob written goes to its key as it arrives, its last byte held back until every byte
hashes to its digest — a bucket stores nothing it has not read whole, so a blob that is not its
digest is never stored, and nothing touches the replica's disk — and an action's result is
answered only when every output it names is there. A run's Bazel reaches it through its worker: a gRPC
pass-through on a socket of the run's own (`--remote_cache=unix:…`; in a microVM, the guest's
`/run/ok/cache.sock`, over vsock), which names the run on every call (`x-ok-run`). ok automation
answers a call only while that run runs, in the namespaces the run's own facts give it — so
nothing a run is handed, no token, decides what it reaches: its account's namespace for post,
deploys, and the pre and manual runs of someone who may land on the repository; a pre whose author
may not writes `<account>/u/<author>`, and reads that, then the account's, so no one poisons
another's results. Entries go `automation.cache.ttl` (30 days) after they were written, swept by
keyed work (`cache`) every hour.

**Done when**, as built. *Dispatch and quotas* — ok automation's store conformance suite, on
PostgreSQL and Spanner: `Quotas` holds an account to its concurrent quota with the runs it has
elsewhere, its further runs waiting while another account's, queued behind them, are leased at
once, one starting as one of its runs finishes, and as many more as a larger quota admits as their
jobs come back; `ConcurrentClaims` has 24 workers claim at once, each run of an unlimited account
leased exactly once and an account of quota 3 given exactly 3; `DispatchWithTheRun` refuses to queue
a run whose job the bus refuses, and drops a stale copy of a run's job and an orphan's where they
are leased; `LapsedLeases` keeps a run its worker heartbeats, its job leased past three heartbeats'
span, and takes over one whose worker stopped — its next attempt, then, on its last, an error, its
log stored by its log's job; `ReleaseDispatches` hands a released run to the next worker at once;
`DispatchUndispatched` dispatches a run an older release queued as a replica starts; `Usage` holds
back an account past its month's seconds. *Keyed work* — `internal/work`'s tests (runners driven by
jobs alone, none looking for work; a holder that dies taken over once its lease lapses, not
before; marks while held and while claimed; a job come early) and the work stores' suite
(`worktest`) on PostgreSQL and Spanner, which holds a take of work a transaction marks, open, until
it commits. *Isolation* —
`TestMicroVM` boots real microVMs and checks what a guest is given: read-only shares the host
refuses writes to, the host's other paths absent, loopback its only interface, egress only through
the proxy to the hosts it allows, commands as the worker's user. `TestMicroVMs` runs pre and post
with real Bazel through a microVM worker: each repository's runs in its own trust domains, the
worker keeping exactly those, and the remote cache reaching the guests. `TestNamespaces` keeps one
account's cache entries from another's, and a pre whose author may not land from writing its
account's; the worker's pass-through names the run it serves whatever the run sends
(`TestCachePassThrough`). Secrets are [secrets.md](secrets.md)'s: post runs of submitted changes read
what a folder grants their job, and pre none.

**The order.** (1) This design. (2) Accounts, quotas, pools, and dispatch over the bus. (3)
microVMs and trust domains. (4) The remote cache. (5) Pools scaling.

*As built, (5):* `internal/automation/scale`, through a two-method view of Deployments (the scale
subresource, which client-go's fake does not serve; `scale.Kube` is the cluster's). Before a pool
shrinks, the scaler sets each of its pods' `controller.kubernetes.io/pod-deletion-cost` to the runs
its worker holds, so the ReplicaSet deletes idle workers first. The chart's `worker.autoscale`
makes the worker Deployment the pool, omits its replicas, and grants ok automation `get` and
`update` on the Deployment and its scale, and `list` and `patch` on the namespace's pods.

*As built, (3):* `internal/automation/microvm`. Each guest boots from a read-only virtio-fs root the
worker makes under its state — ok itself as `/ok`, its init (`ok worker guest`, so ok must be built
static), `/etc`'s few files for the worker's user, mount points — rather than an initramfs, which
would hold ok's 120 MiB in every guest's memory; it mounts the shares the host tells it over vsock
as it boots, each at its host path. Commands run as the worker's user, in their own process group;
the host's `ok worker vm-exec` forwards its directory, environment, and signals, so a command runs
in the guest as it would have on the host. A domain's checkouts are shared through their parent
directory, so a checkout presented after the guest booted (a plan's base) is seen. A virtual
checkout's presentation leaves with its copy (okfs `Unpresent`). The microVM boots in under a
second (747 ms in the test VM with seven shares).

*As built, (4):* `internal/automation/cache` serves the cache, over the logs' bucket (a write is
checked against its digest as it arrives; a ByteStream write of a blob the namespace holds ends at
once), with each replica's memory of small blobs; ByteStream is registered from a descriptor of
ok's own, the generated one taking only a `*grpc.Server`. Workers alone may call it (authn's
services outside ok's protocols). `service.Namespaces` resolves a call's run, kept 30 seconds — so
a call within 30 seconds of its run's end is still answered — its account from `repository_owner`
(`_` for none), and whether its author may land (`authz.Land`, from identity's facts). The
worker's pass-through (`worker/cache.go`) forwards the cache's four services raw, as the worker,
replacing whatever names the run with the run it serves; it listens in the run's slot's scratch,
which no other slot's runs see, or, for a microVM, on the host alone, which the guest's socket
reaches over vsock. A lease says whether there is a cache (`LeaseResponse.cache`).

*As built, (2):* the claim is the store's (`internal/automation/store`, `claim.go`), over each
backend's `QueuedHeads`, `RunningByAccount`, `MonthSeconds`, `LockAccount`, `RunningOf`, and
`ClaimHead`; the quotas are `internal/automation/quota`'s cache of the directory's, and its reporter
of the cell's use. A rerun keeps its phase's priority, which before it lost.

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

### ok control at scale, as built

Phase 9's design, settled against ok control's code before it is built. Today ok control lists a
repository's CLs (`ListChangeLists`, its finder's tabs) and repositories by a literal match on its
own cell's projection; nothing spans repositories, nothing is mailed, nothing calls out, and code is
browsed, not searched.

**One outbox.** Every change to a CL that ok control records — the SCM's feed's uploads,
transitions, and landings, and its own reviewers, grants, replies, and comments — is an event in
the CL's history (`RecordEvent`). The same write puts the event in an outbox (`outbox`: the
repository, the event, when) and marks the repository's keyed work (`outbox`, in a `work` table ok
control gains, as ok automation and the deployer have). The repository's outbox is drained in order
of when, each row deleted once what it causes is done: so no event is lost however transactions
interleave, and none is done twice by a replica that stops short (each consumer is idempotent). The
outbox moves with its repository (`moveTables`). Each event feeds three consumers:

- **Inboxes.** A CL's inbox entries are recomputed from ok control's projection — its author's
  (`authored`) and each reviewer's (`reviewing`, with whether the reviewer owes the current
  patchset a reply: no grant on it, no comment since it) — with what a list shows (the repository's
  full name, number, title, status, when it changed), and set in the directory (identity's
  `Delivery.SetInbox`, every entry of the CL at once, at a version, so an older write never
  overwrites a newer). The directory keeps them (`inbox`: principal, repository, CL, relation), so
  a person's "My CLs" and "Needs my review" span every cell: the home page lists them
  (`Inbox.ListInbox`), and a CL's page is its cell's. A repository moved keeps its entries — they
  name the repository, which the edge routes — and a purge removes them.
- **Notifications.** An event that asks something of someone notifies them, never the one who did
  it: a review asked of them, a label granted on their CL, a comment on their CL or in a thread
  they were in before it, their CL landed or rolled back, a CL they review abandoned
  (`Delivery.Notify`, to the directory: principal, kind, the CL, who, when, a line of text, a
  link; once by recipient and source). ok control's notifications page shows them, read or not
  (`Notifications.ListNotifications`, `MarkRead`), and its home their unread count. Each person
  chooses which kinds are mailed (`SetNotificationPreferences`; by default reviews asked of them
  and their CLs rolled back); the directory mails them through `identity.mail.smtp`, as queue
  work: a `mail_outbox` in its database sent by keyed work (`mail`, a key per message) with
  retries and backoff, a message that fails a day's tries kept as dead.
- **Webhooks.** A repository's administrators add webhooks (a URL — `https` to a public address,
  unless the installation allows private networks — and the events it wants, `cl` and `push`; ok
  makes the secret, seals it, and shows it once). Each event a webhook wants is a delivery (`webhook_delivery`: the payload, JSON; attempts;
  the next try; delivered, failing, or dead), sent by keyed work (`webhooks`, by repository): a POST
  with `X-Ok-Event`, `X-Ok-Delivery`, and `X-Ok-Signature-256` (HMAC-SHA256 of the body with the
  webhook's secret, `sha256=<hex>`), a 2xx delivered, anything else tried again after 30 s,
  doubling to an hour, dead after a day; the last 100 deliveries kept and shown, and a dead one
  redelivered on request. Webhooks and their deliveries move with their repository.

**Search.** The directory searches repositories, people, and organizations by substring of their
names, as people type them (`Directory.Search`): PostgreSQL's trigram indexes (`pg_trgm`) in a
single installation, Spanner's substring search indexes (`TOKENIZE_SUBSTRING`) in the hosted
directory — every repository in every cell, and only what the person searching may see. ok control's
home searches through it.

**Code search.** The SCM keeps a trigram index of each repository's main: built when main moves, by
keyed work (`codesearch`, at most every 30 seconds a repository), as one object in its bucket
(`search/<repository>`, the latest): the files' paths and content IDs, then each trigram's sorted
posting list of files, delta- and varint-coded — text files alone, each up to 1 MiB, and the index
of the latest main kept. A query (`Search.Code`: a literal or a regular expression, and a path's
glob) becomes the trigrams it requires (a regular expression's, by its syntax tree, as an AND of
ORs), whose postings intersect to the candidate files, which are read and matched a line at a time,
up to a limit. SCM replicas keep indexes they served in memory (within `scm.cache.code_indexes`), so a
repository's searches go where its reads go: the edge's affinity.
ok control's repository pages search its code (`/{owner}/{repo}/search`).

**Done when:** a person's inbox lists CLs from repositories in two cells, and a review requested in
either notifies them and mails them; a webhook receives a CL's events signed, retried through a
failing endpoint, and dead after its tries; the directory finds a repository by part of its name on
PostgreSQL and on Spanner; and a repository's code is found by a literal and by a regular
expression, read only from the files its trigrams name.

**The order.** (1) This design. (2) Directory search. (3) ok control's outbox, and inboxes. (4)
Notifications and mail. (5) Webhooks. (6) Code search.

*As built, (6):* `internal/scm/codesearch` is the index and its queries; the store builds and keeps
them (`store/codesearch.go`), `Search.Code` serves them, authorized as a read. Trigrams are of bytes
with ASCII letters lowercased, so a case-sensitive query's candidates are a superset, matched in
full; a case-insensitive literal holding a letter beyond ASCII with other cases requires nothing of
the index. A regular expression's plan joins literal runs across concatenation (`foo(bar|baz)`,
which the parser factors to `foo(ba[rz])`, requires `foo` alone). `appendEvents` marks the build in
the transaction of every `HeadMoved`. A search answers from the latest index it has, saying when
main has moved since (`stale`), and asks for the next; a repository never indexed is `UNAVAILABLE`
until it is. A deletion drops the index; a move leaves it behind, the destination indexing on its
first search. ok control's `/{owner}/{repo}/search` serves it, linked from the repository's page.

**Done when, as built.** `TestInboxAcrossCells` lists one person's CLs from two cells, each drained
by its own ok control. `TestNotifications` (ok control's end-to-end suite) notifies and mails a
review, with a mail server of the test's own. `TestWebhooks` delivers a CL's events signed, a
failing answer retried, and `TestDelivery` keeps a delivery dead after a day. `Search` in identity's
conformance suite finds a repository by part of its name on PostgreSQL and on Spanner. And
`TestCodeSearch` finds code by a literal and by a regular expression, while `TestNoMissedMatch`
shows the trigrams never drop a file a match is in.

*As built, (5):* `internal/control/webhooks`, on ok control's repository pages
(`/{owner}/{repo}/webhooks`, linked from the repository's page for its administrators). A secret is
32 random bytes in hex, sealed under `control.sealing_key_file` — shared by an installation's
cells, so a moved repository's webhooks open — and bound to its repository and webhook; an
installation with no key keeps no webhooks. Delivery dials only public addresses, checked as each
connection is made so a name that resolves anew reaches no private one, follows no redirect, and
needs https; `control.webhooks.allow_private_networks` lifts all three, for an installation whose
endpoints are its own, and tests. A delivery's ID is its outbox entry's and its webhook's, so a
drain that stops short and queues again queues nothing twice; main's moves wait in the outbox as
entries of no CL. A webhook added is pinged. Payloads name people by their stable IDs.

*As built, (4):* ok control's notifier (`outbox.Notifier`) is a consumer of the drain. A review
asked is one notification per reviewer per patchset (its source names both), however it was
asked: a reviewer added to an open CL, or a draft's reviewers when its author asks for review.
Notifications of repositories the person may no longer see are not listed; a purge removes them.
A notification is recorded and its mail queued — its sending's work marked, its job enqueued — in
one transaction, so a retried `Notify` that finds it recorded never loses its mail. Mail is sent at least once — a send that succeeded but whose
record failed is sent again. Without `identity.mail`, notifications are recorded and none is
queued. The failure of a CL in the landing queue does not notify: ok control records no event of
it.

*As built, (3):* the outbox is written where the event or comment is — `RecordEvent` and
`CreateComment`, on PostgreSQL one statement of writable CTEs (event, outbox entry, work mark),
on Spanner one transaction — so no call site can forget it; it moves with its repository. ok
control's `work` table and `pgdata.Work`, PostgreSQL's keyed work store beside `spannerdata.Work`.
The drainer (`internal/control/outbox`) drains each outbox as its mark's job comes — a move's
arrival marks the outboxes that came with it — and sets each touched CL's entries from the
projection, at the drain's clock in nanoseconds — drains of a repository are one at a time, its keyed work's —
then its consumers (notifications, webhooks), then removes the entries. A repository the
directory's events have not named in the projection yet is named by the directory
(`GetRepositoryEntry`). The inbox carries no checks verdict: ok control is not told when pre
finishes, so a verdict there would go stale. `ListInbox` drops entries of repositories the person
may no longer see (`readerRole`), so losing access hides a CL's title. The home page shows the
signed-in person's ten latest, those waiting on them first.

*As built, (2):* `Tenancy.Search`, through the store's `SearchRepositories` and `SearchPrincipals`:
repositories by their names (owner/name narrowing to owners whose handles start with owner), people
and organizations by their handles or display names, the closest first — equal, then starting with
it, then the shorter. Repository names are lowercase; queries are lowercased. On PostgreSQL the
migration makes `pg_trgm` and its indexes where the role may (a server without the extension
scans); on Spanner, hidden `TOKENIZE_SUBSTRING` columns and search indexes. A repository is listed
as `readerRole` lets the caller see it, active ones alone; ok control searches for the signed-in
person only — a robot naming no person sees every repository. ok control's home searches through
the directory when it is given a query: a row of a repository its cell holds is the row it always
showed, another cell's what the directory says of it (its visibility and creator), one page of 50.

## Deploy at hosted scale

A customer's cluster runs ok's **deploy agent**, a robot of the customer's organization, which
connects out to its cell — no inbound access, no credentials to the cluster held by ok — and applies
what the deployer sends: push, migrations, roll, watch (deploy.md, "A deployment"), as the deployer
does in-cluster today. Deploy targets are configured per organization; a single installation keeps
deploying to its own cluster without an agent.

### Deploy agents, as built

Phase 10's design, settled against the deployer's code before it is built. Today the deployer holds
one cluster (`kubernetes.Interface`, in-cluster or a kubeconfig): the engine reads live replicas as
it plans and rolls through it, the observer watches pods through it, and migrations run in the
deployer's checkout with the installation's environment (`deploy.migration_env`). A hosted service
must deploy into its customers' clusters without holding a credential to any of them.

**The agent.** An organization's owners designate one of its robots as its deploy agent
(identity's `DeployAgents.SetDeployAgent`; identity.md, "An organization's robots"). The agent runs in the
organization's cluster (`ok deploy agent`; the chart's `deployAgent`), under a ServiceAccount the
organization grants what rolling needs (deploy.md, "Grants"), and signs in as the robot — with a
token from its cluster's issuer, which the organization trusts and binds to the robot, or a key an
owner certified.

**The connection.** The agent asks the directory which cells' deployers it serves — those holding
its organization's repositories (`DeployAgents.DeployCells`) — and keeps a connection to each, out
from the cluster: `Agents.Connect`, a stream either way, through the edge, which sends a request
about no repository that names its cell (`Ok-Cell`) to that cell. A deployer takes it only from
the robot its organization designated, and records the session (`agent_session`: the account, the
replica holding the connection at the address it advertises, a lease renewed every 10 seconds); a
newer connection of the account replaces the older.

**What goes through it.** Two kinds of request, many at once, each with an ID:

- *The cluster's API.* Each Kubernetes request — method, path, headers, body — goes to the agent,
  which sends it to its cluster's API server with its own ServiceAccount's token, and streams the
  answer back, watches included. The deployer's client-go uses a transport that sends its requests
  so; nothing else in the deployer changes. ok holds no credential to the cluster: the agent's
  RBAC bounds what ok may do there, and the agent makes only what rolling and observing need —
  Deployments and StatefulSets read, created, and updated, ReplicaSets and pods read, and
  discovery — so no Secret is read, no command run in a pod, no proxy or port opened, no token
  minted, nothing deleted, whatever its RBAC would allow.
- *Migrations.* The agent runs a deployment's migrations itself: it checks the commit out from the
  SCM as its robot and runs the migrate target with Bazel, in its own environment
  (its process's), and reports what it applied. The customer's databases' credentials never leave
  the cluster. It refuses a repository that is not its organization's (`DeployAgents.AgentRepository`),
  and a commit not on that repository's main: whatever a deployer asks, only landed code of its
  organization's runs with those credentials.

**Every replica reaches the cluster.** A replica not holding an organization's session sends each
request to the one that does (`AgentRelay.Exchange`, deployer to deployer, at the session's
address); so planning, the engine, and the observer work on any replica, as today.

**Which cluster.** A repository's deployments go to its owner's agent, if its owner designated one.
A hosted service has no organization of its own, so every repository deploys through its owner's
agent — ok's own organization runs one beside its deployer — and an owner with none designated, or
none connected, has its plans and deployments wait, saying so. A single installation deploys to its
own cluster, as before, unless an owner designates an agent.

**Pushes** are unchanged: workers push, and the registries' credentials are secrets, which wait for
a design of their own.

**Done when:** against a real Kubernetes API server, the deployer's client-go creates, reads,
updates, lists, and watches through an agent connected to another replica; a migration runs in the
agent with its environment; a hosted deployer's repository with no agent waits; and the agent
refuses `exec` and Secrets. A real cluster's roll through an agent is phase 11's.

**The order.** (1) This design. (2) The protocol, the tunnel's transport, and the agent's forwarding.
(3) The deployer's sessions, relay, and cluster choice; identity's designation and cells. (4)
Migrations through the agent. (5) `ok deploy agent`, its chart, and docs.

*As built.* `internal/deploy/tunnel` is the deployer's side: `Hub` takes agents' connections and
records each as a session (`agent_session`, cell-local, a 30-second lease renewed every 10 —
another replica's claim ends this one's), serves other replicas' `AgentRelay.Exchange`, and gives
each account a `Link`, its own session or a relay to the replica holding it at the address that
replica advertises (`OK_DEPLOY_ADDRESS`, its pod's, through the downward API); `Transport` is
client-go's `http.RoundTripper` over a link, a request's frames carrying its method, path, headers,
and body (at most 16 MiB), its answer streamed back in 256 KiB frames, at most 64 MiB waiting
unread, a body closed early canceling it at the agent; `Migrate` asks for a migration and waits.
`internal/deploy/agent` is the agent: `Allowed`, its allowlist of calls, by method and path; the
headers it passes on (`Accept`, `Content-Type`, `User-Agent` — none that authenticates or
impersonates); `Migrations`, its checks, then its checkout and Bazel; `Runner`, a connection per
cell, kept with a backoff from a second to thirty. The engine takes `Clusters` (`AgentClusters`:
each repository's owner as the directory's events say it, now recorded by the deployer; whether it
designated an agent, asked of identity every 30 seconds): the roller, live replicas at planning,
the observer, and migrations each use a repository's own. A repository whose agent is away has its
plan wait, logged once; a deployment under way logs `waiting: …` once and goes on when it is back;
the Services page shows the observer's error. One agent per organization connects at a time — the
chart runs one, recreated rather than rolled — since a newer connection replaces the older.
`ok deploy agent` and the `ok-agent` chart (`deploy/helm/ok-agent`) run it; deploy.md, "Deploy
agents", is how.

**Done when, as built.** `TestThroughAnAgent` (internal/deploy/agent): against a real Kubernetes API
server with RBAC (`testdb.Kube`), the deployer's client-go on one replica creates, reads, updates,
lists, and watches Deployments through an agent connected to another; a namespace not granted is the
API server's refusal; Secrets, deletes, a pod made, exec, a pod's log, and a service's proxy are the
agent's, whatever RBAC grants; a robot no one designated is refused; a migration goes through; and
once the agent disconnects the replica reaches it no longer. `TestDeployThroughAnAgent`
(internal/deploy/role): a hosted deployer's deployment waits for acme's agent, saying so; once it
connects, its migration runs at the agent with real Bazel, in the agent's environment — the
deployer's never seen — and succeeds; the agent refuses a CL's unlanded commit and another owner's
repository. `TestThroughClusters` and `TestAgentClusters` (internal/deploy/engine) hold the waiting
and the choice of cluster; `DeployAgents` in identity's conformance suite and `TestDeployAgents` in
its service, the designations; `TestEdgeNamedCell`, the edge's routing.

### Proof at scale, as built

Phase 11's design. Phases 2 and 3 proved the shape in one process (`TestScale`: many repositories
through the whole flow while replicas stop; `TestLargeRepository`: a 1 TB repository); this phase
proves a deployed cell carries its share of *The target*, under chaos, and prices it.

**One cell's share.** Cells are independent — their own databases, bucket, and bus — so a cell's
load at the target is the target's over its cells. Of a hundred: 10,000 accounts and repositories,
~23 publishes a second (~120 at peak), ~200 checkpoints a second at peak (a tenth of accounts
active, one every five seconds), ~5.8 CI runs a second, ~1 Gbit/s of ingest. The directory's load —
sign-in, search, inboxes, notifications — is global, and measured beside. CI's bulk reads (~29 GB/s
a cell) do not pass through ok's servers by design (*What follows from the numbers*, 3): the
generator reads each run's files through the SCM as a cache's misses would, a fraction it is told.

**The generator, `ok load`.** It drives an installation through the services people's jj, ok
control, and workers call — nothing internal — as an organization's people and as workers:

- `ok load setup` makes organizations and their repositories, each seeded with a tree of files.
- `ok load run` offers each operation open-loop — Poisson arrivals at its rate, so a slow cell
  shows as latency and backlog, not as less load offered — at a fraction of a cell's share: publish
  (a commit changing a few files of a repository's main, sent as jj sends it); checkpoint;
  upload a CL, review it (LGTM and Approval, the author's own where no OWNERS apply), and submit it
  once pre passes; CI, by synthetic workers that lease runs — a plan of one job, a job that, with
  `-present-dir`, presents its commit through okfs as a worker does and reads files of it, and
  passes after a run's time. Rates are the target's per account and day:
  200 publishes, 50 CI runs; a CL rate, which the target does not name, is a flag (10 a day).
- It measures each operation — requests, errors by code, latency at p50, p90, p99, and p99.9 (a
  log-bucketed histogram), offered and achieved rates, and each by the minute of the run it began
  in — and how long a CL took from upload to landing, its wait for pre and for post apart, and
  writes a report (JSON).
- It checks what must hold however a run went: every CL it submitted landed or failed with a
  reason, none stuck; no operation lost silently.

**Chaos.** `scripts/chaos.sh`, against a staging cluster: every interval it deletes a random pod
of a random ok role; once a run it drains a zone's nodes; once it restarts the bus. A run under
chaos must keep the checks above, with errors only where a request was under way when its replica
stopped.

**The cost model, `ok load cost`.** From a run's report and what the installation used — cores of
each role, database and bucket bytes per repository, the bus's messages — scaled linearly to the
target, at list prices (a prices file, each price with its source), it gives the monthly cost of a
cell, of the target, and of an account. Linear scaling is the assumption it states; the run's
measured rates are its inputs, not the target's.

**Staging.** One cell on GKE, in a region's three zones: ok's chart, the directory and cell on
Spanner, a bucket, NATS; Terraform of its own (`deploy/staging`), applied and torn down.
It costs money while it runs, so it is stood up only with an explicit go-ahead, sized and priced
first.

**Done when:** `ok load` drives an installation through the whole flow at a stated fraction of a
cell's share, its checks holding, and reports; under chaos too; and `ok load cost` prices it. Run
locally first (`ok serve` in the VM, reduced), then on staging.

**The order.** (1) This design. (2) `ok load setup` and `run`, synthetic workers, the report, its
checks; a local run. (3) `ok load cost` and the prices. (4) `scripts/chaos.sh` and staging's
Terraform. (5) Staging, with a go-ahead: the run, under chaos; the cost; torn down.

*As built, (2)–(4).* `internal/load` and `ok load run` (setup is part of a run: it makes the
repositories it lacks) and `ok load cost`. A run's speed multiplies a repository's publishes and
CLs, but adds workspaces rather than speeding them: a tenth of the repositories times the speed
are active, each checkpointing every five seconds on average, as okfs does. A publish carries an
account's day of ingest over its 200 publishes (5 MB); a checkpoint, one changed file of 4 KiB; a
CL, 64 KiB under a directory of its own, so CLs never conflict. The prices are
`deploy/load/gcp-us-central1.yaml`, each with its SKU, from the Cloud Billing Catalog API, with
Spanner's published throughput and CPU ceiling. `deploy/staging` holds staging: its Terraform, the
chart's values (the chart now runs on Spanner, `database.spanner`, through workload identity), the
load job, and the runbook with what it costs (~$3.20 an hour at its defaults).

**Done when, so far.** Locally — `ok serve` in a 12-core VM, on PostgreSQL and a directory —
`ok load run` offered a cell's full share on average (100 repositories at 100×: 23.6 publishes, 199
checkpoints, and 1.15 CLs a second, with 21.8 GB written in three minutes, ~116 MB/s): every
operation succeeded, 207 CLs landed, none stuck, every check held; publishes took 79 ms at p50 and
5.3 s at p99, checkpoints 31 ms and 3.2 s, a CL 6.4 s from upload to landing at p50. The server used
1.1 cores and PostgreSQL 0.4; the database grew ~50 GB a day at that load, and read ~99,000 rows a
second against 2,000 written. `ok load cost` priced it: ~$100,800 a cell a month at the target,
~$10.08 an account — object storage $93,100 of it (at 2× compression, assumed), Spanner compute
7 nodes for the reads, $4,600. Those reads, and checkpoints' growth of the database, are the two
things the numbers say to look at first. Of the reads, the SCM's content-index log is about half:
a minute's run at a cell's share fetched 5.6 million of its rows in 108,000 index scans, ~52 a
scan — a replica catching a repository's index up from the log's tail on each publish and
checkpoint until a merge folds it. (On Spanner the SCM keeps its logs staged and folds them in
bounds, phase 6; a PostgreSQL row is priced here as a Spanner one all the same.) `TestLoad` (cmd/ok) runs the whole flow through the real
command; `TestChaos`, the chaos script against a stub.

**Staging, as run (2026-10-21).** One installation on GKE in us-central1's zones — three to six
server replicas on n2-standard-8 nodes, Spanner (Enterprise, three nodes), GCS, NATS — signed in
to through Dex, driven by `ok load run -people`: each of up to 2,000 people signed in through the
device flow and acting on a connection of their own, reviewing each other's CLs, from four pods
of an indexed Job whose reports `ok load merge` adds up (histograms merged, checks judged again).
What it found, each fixed and tested on both databases:

- *Claims.* Every claim wrote its account's usage row and counted its running runs inside its
  transaction (aborted 85–99% at a quarter of a cell's share; workers waited 204 s for a lease).
  Now a claim reads outside its transaction and locks only its run; a concurrent quota is slots;
  seconds are sharded (*Dispatch, and admission at the claim*). At the same load: every CL
  landed, leases 3 s at p50.
- *The lock's fence* read the cell its own refreshes wrote: a fenced transaction past the lease
  lost its lock. Identity's sync of 2,000 people did so, then exceeded a Spanner transaction's
  limits: the sync now applies its plan a chunk at a time, the snapshot last, a sync cut short
  completed by the next.
- *Checkpoints* scanned their repository's every checkpoint, the optimizer having no statistics
  yet, aborting each other's: a newest-first index each names (87% aborted, 114 s at p50, before;
  none and 0.4 s after).
- *The content index's log*: every refresh read the tail's every ID, folds came daily and
  committed by deleting what they folded beside writers that aborted them, so a busy repository's
  tail grew without end (22,778 rows in ten minutes). It is now read from a place, in commit
  order, folded once long, the fold's commit short (docs/scm.md, "The content index"). On
  PostgreSQL a place taken in a write transaction leaves the transaction out of what it read.
- *Pack numbers* were a counter every publish wrote: now blocks of 1,024 a replica reserves.
- *Device sign-in* swept stale logins by partitioned DML on every start, which Spanner refuses
  run twice at once; the housekeeper sweeps them. The load generator's sign-ins wait for a
  connection (gRPC WaitForReady, five minutes at most) rather than failing while servers roll; a
  role calling another still fails at once and serves what it can. A transient OWNERS read no
  longer fails a CL's review: its caller tries again. A publish is refused for a network that
  moved before its exchange, so the edge sends it on to the cell that holds it.

Results, ten minutes each, every check holding:

| Run | Publish p50 | Checkpoint p50 | CL upload p50 | CLs landed |
|---|---|---|---|---|
| 2,000 people, 100 repositories | 586 ms | 396 ms | 615 ms | 144 of 144 |
| 2,000 people, one repository | 678 ms | 506 ms | 747 ms | 165 of 165 |
| Under chaos: real CI, pods killed every 45 s, a zone drained, the bus restarted | | | | 40 of 40, 12 s mean |

At a cell's full share into one repository (2,000 people at 5×: 23 publishes, 200 checkpoints, and
1.35 CLs a second) the CL pipeline is the limit: publishes 17 s at p50, checkpoints shed, and
lock waits on per-repository rows — a CL's row range, the CL and event counters, compaction's
work row, main — and in ok control's review and automation's finish, ~18 attempts each. One
repository at Google's scale of CLs is that redesign's: batched landing and per-repository rows
written once a batch.

**A monorepo, as run (2026-10-24).** 3,000 people in one repository shaped like a monorepo (67,000
files), at three times their daily rate — a working day's peak. Before: half of all uploads aborted
on per-repository rows, the CL and event counters above all (8,843 and 2,488 seconds of lock waits
in one run), and a CL took ten minutes from submit to landing. Now a repository's CL numbers and its
log's places are given just after commit, by its sequencing pass (scm.md, "Sequencing"), and an
upload's pins are found before its transaction, which is 12 round trips, from 25. At the same peak
every upload succeeded (0–2 aborts a minute of up to 79 attempts), the counters' waits 113 and 96
seconds, the passes' own. The run named two more rows, fixed since: compaction's work row, which a
fenced batch's read of the lease held from its holder's renewals (*No leaders, as built*), and the
content log's commit-timestamp index, whose writers held its entry at the pending commit timestamp
— now sharded (scm.md, "The content index"). Spanner's CPU, 70–95% throughout, went to workspace
trees, a row each: 63% of it on the touch walk's and publishes' reads and writes of `object`.

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
   the landing queue per repository; run logs streamed to object storage; random repository IDs.
   *Done when:* a test drives thousands of repositories at once through upload, review, landing, and
   CI across several replicas of each role, with replicas killed at random, and every event is
   applied exactly once and every repository's `main` stays green. That test is
   `internal/control/e2e` `TestScale`: three replicas each of the SCM, ok automation, and ok control
   behind a proxy and three followers of the deployer, one of them crashed and replaced every 150 ms
   in turn, while repositories go from creation through upload, approval, pre, submission, post,
   and landing; then every follower's cursor must match every log's head, each upload, landing, and
   pre plan must appear once, and every `main` must be its CL's commit, moved by the landing queue.
   It runs when `OK_TEST_SCALE_REPOSITORIES` says how many repositories to drive, as ok's CI
   target does with 300, 32 at a time (`OK_TEST_SCALE_AT_ONCE`); locally it drove 2,000 in seven
   minutes, about five landings a second from the first to the last.
3. **Segments.** The LSM: tails, manifests, compaction, and segments for objects, the commit graph,
   path history, the content index, op logs, and events; reads through Bloom filters and fence
   indexes; large publishes as segments; fork networks with presence sets; garbage collection per
   repository and network.
   *Done when:* a 1 TB synthetic repository (10^7 files, 10^6 commits) imports, reads, and collects
   with its database under 100 MB, and fsck verifies it. That test is `internal/scm/store`
   `TestLargeRepository`: the files land as one CL, then the history as stacks of a hundred CLs,
   each changing three files; the relay publishes the events; a pass collects garbage and folds the
   content index; compaction moves the rest; then no immutable data is left in the tail, a replica
   with nothing cached reads trees with their last commits, a directory's history, an annotation,
   and files, and fsck verifies it all. CI runs 3,000 files and 400 commits;
   `OK_TEST_LARGE_FILES`, `OK_TEST_LARGE_COMMITS`, and `OK_TEST_LARGE_BYTES` size it up. With
   landed CLs and main's log in segments too, the repository's rows hold nothing that grows with its
   history: at 30,000 files and 2,000 commits they are 71 KB, and at 1,000,000 files and 10,000
   commits 98 KB — a trained dictionary's body (64 KB), pack rows, and counters. The test then
   forks the repository, which writes a reference to each of its live segments and no rows per
   object, and reads the fork's main. Run in the test VM (12 vCPUs, 36 GiB), the 1,000,000-file
   repository landed in 1m13s, collected in 44s, compacted in 0.2s, read back cold in 59 ms, and fsck
   took 11s; forking it took 7 ms, writing 7 segment references and 1.2 KB of rows; the test peaked
   at 4.0 GB. The plan's 10^7 files would hold some 40 GB in the test's own tree builder alone: the
   repository's rows do not grow with it.
4. **Tenancy.** Accounts, organizations, teams, `owner/name`, visibility, roles, `Authorize`,
   tokens, robots per organization, enterprise identity providers per organization, deletion,
   renames and transfers; the two presentations of names; moving an installation in as an
   organization (*Tenancy, as built*). *Done when:* the permission matrix — every role,
   visibility, and action — is a test, and this installation moves in as an organization with its
   history intact. The matrix is `internal/authz/authztest`: authz's tests hold `Decide` to it, and
   the SCM's gate (`internal/scm/service` `TestAuthorizeMatrix`, for every method people and robots
   call) and ok control's review operations (`internal/control/review` `TestAccessMatrix`) are swept
   over it, every kind of caller at every visibility. The move is `internal/control/e2e`
   `TestMoveIn`: an installation with a landed CL is stopped, moved in as `acme`, and started as a
   hosted service — its old URLs redirect, its history names people `@bob`, a clone made before the
   move syncs and lands another CL, and the bare name clones it still.
5. **Bus backends.** NATS JetStream, Google Pub/Sub, and AWS SQS, each passing the queue suite,
   against servers in the test process (an embedded NATS server, Pub/Sub's `pstest`, and a fake of
   SQS). *Done when:* phase 2's
   test passes on each.
6. **Spanner.** A Spanner implementation of every role's data interface and of the directory; Spanner
   Omni on every change and a real instance nightly; cell moves from PostgreSQL to Spanner; random IDs for
   runs, plans, comments, deployments, and checkpoints, their pages ordered by time. First what
   Spanner lacks leaves PostgreSQL's own mechanisms behind, on PostgreSQL: registrations and inboxes
   in place of `LISTEN`/`NOTIFY`; the lock in place of advisory locks; no leaders, and fencing; random IDs.
   Then `internal/data`'s Spanner primitives and Spanner Omni, and each role's Spanner
   implementation, smallest first — the deployer's, ok control's, ok automation's, identity's, the
   SCM's — each passing its conformance suite on Spanner Omni and on the real instance.
   *Done when:* every conformance suite, the full suites, and the e2e pass on Spanner, and a cell
   moves from PostgreSQL to Spanner under load without a failed request.
7. **Cells.** The global directory, the edge (routing header, authentication, rate limits),
   placement, repository and network moves, affinity within a cell, multi-region (*Cells, as
   built*, its order).
   *Done when:* three cells in two regions serve one namespace, and repositories move between them
   under load.
8. **Hosted automation.** microVM isolation, quotas, dispatch queues per pool, autoscaling, the
   remote cache per organization, secrets.
   *Done when:* an organization flooding CI holds no more workers than its quota, the rest of its
   runs set aside so none of another's waits behind them; workers added to a pool add consumers
   with nothing they contend on; and a
   run cannot read another tenant's checkout, cache, or secrets.
9. **ok control at scale.** Inboxes, search (repositories, people, code), webhooks, notifications.
10. **Deploy agents.**
11. **Proof at scale.** A load generator that simulates the target — a million repositories, their
    publishes, checkpoints, CLs, landings, and CI reads, at the rates in *The target* — against a
    staging deployment, with chaos (replicas, zones, the bus); and a cost model from what it
    measures.

## Testing

- **Conformance suites** for the data interfaces, the object store, and the queue, each run against
  every implementation on every change: PostgreSQL, Spanner Omni; MinIO, a GCS fake, a directory; a
  NATS server, Pub/Sub's fake, a fake of SQS.
- **Equivalence**: ok's suites and e2e run on each supported combination, as today's suites run a
  scenario in a real and a virtual workspace (okfs.md, "Testing"). The database is
  `OK_TEST_DATABASE`'s and the bus `OK_TEST_BUS`'s: the end-to-end suites on Spanner give each
  test's roles databases of their own, made with their schemas at once (`internal/testdb/roledb`),
  and a bus of another backend.
- **Scale**: phase 2's many-repository test, phase 3's 1 TB repository, phase 11's load generator.

## Decisions

1. **The repository is the unit of placement, order, and work**; a fork network is placed whole.
2. **Domain-level data interfaces, PostgreSQL and Spanner behind each**, one conformance suite per
   interface; no generic SQL or key-value layer.
3. **Immutable data in object storage as segments**, the database holding mutable state, the
   manifest, and a bounded tail.
4. **The log is the truth; the relay names what grew** at least once, in any order; consumers
   apply exactly once by cursor.
5. **Queue backends:** NATS JetStream, Pub/Sub, SQS.
6. **Object stores:** S3-compatible, native GCS, filesystem.
7. **Cells and one global directory**, Spanner multi-region in the hosted service, in its GoogleSQL
   dialect.
8. **No leaders**: work leased per repository, organization, or shard, fenced where a late write
   could harm; exclusion that is no work by `internal/lock`'s leases, never advisory locks; wake-ups
   by a registered inbox, never `LISTEN`/`NOTIFY`.
9. **A Firecracker microVM per hosted run**, caches per trust domain.
10. **A single installation is one cell**, on today's backends.
11. **Every cloud, by configuration.** No backend is the default for the hosted service: each
    deployment configures the database, object store, stream, and queue its environment has (on
    AWS, the cells' databases are PostgreSQL — RDS or Aurora — since Spanner is Google's alone).
12. **Hosted CI stays Bazel-only**, as automation.md's principles make it.
13. **Fork networks share storage copy-on-write**; no repository reads what its own presence set
    does not hold.
