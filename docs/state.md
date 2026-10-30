# Where state lives

Every piece of ok's state lives in one of three places: **object storage**, **the database**, or **a
queue item**. Nothing else holds state — no node, no process, no cache of anything that changes —
but a connection, which is held where it was made. A node holds state only while it serves a live
request, or while a worker does the queue item it holds; when either ends, so does the state.

So every node is dumb, and every worker is any worker. It never matters which worker carries which
payload: each queue item names work, the work is idempotent, and an item whose worker dies before it
acknowledges it is delivered to another, which does the work again from what the database, the
bucket, and the item say. Nothing waits on a node that might be gone.

This document is the rule, the bus it is built on, the patterns every role follows, and what ok
changes to follow them (*The change*).

## The rule

- **Three places.** Object storage holds content (packs, segments, logs, checkouts); the database
  holds what changes, and what says where content is; a queue item holds a piece of work, and what
  it needs that is not the database's to hold (a reply address, the place a long walk reached).
- **Connections are exempt.** A live request's connection is where it is: what is for that request
  — a reply to it, a stream of its updates — goes to its node. A deploy agent's tunnel is held by
  the replica it reached, and what is for that agent goes there.
- **Copies of what never changes are not state.** A cache of content named by its hash — a chunk, a
  tree, a segment's block, a commit's change list — can never disagree with storage, and losing it
  costs a read; replicas keep them, bounded by memory. A copy of anything that changes — an index's
  log, a manifest, a permission, a key, another replica's progress — is state, and is not kept: it
  is read from the database each request, which costs reads, and the database's capacity grows with
  them.
- **No polling.** Nothing asks again and again whether something changed. What changes says so: the
  transaction that changes it enqueues the work that follows, and a live request waiting for it is
  told (*Waiting*). External protocols that only poll are exempt: Spanner's schema operations, and
  an identity provider's device sign-in.
- **Timers are distributed.** A timer is state, so it lives where state lives: as a queue item due
  at its time, or as work marked due at its time in the database, whose job the queue holds until
  then. Any worker takes it when it is due. No process owns a schedule, and none ticks.
- **No PostgreSQL notifications.** LISTEN and NOTIFY lose what is sent while no one listens; ok uses
  neither. An installation runs a bus (*The bus*); PostgreSQL is a database, never a bus.

What a node may do: hold a connection; renew a lease on work it is doing, and heartbeat a run it is
running; read its own machine (a worker's free memory); and share one read among the live requests
that ask for it at once — nothing of it outlives them.

## The bus

The bus is queues, nothing else (`internal/bus`). Every backend passes bustest's suite; NATS with
JetStream is the first, and Pub/Sub and SQS implement it too.

```go
type Queue interface {
	// Enqueue adds jobs: each with an ID — a duplicate within the backend's window is dropped — a
	// queue, a key it is about, a body, and a time it is due (NotBefore; zero: now).
	Enqueue(ctx context.Context, jobs ...Job) error
	// Lease waits up to o.Wait for up to o.Max jobs of a queue that are due, each invisible to
	// every other worker until its visibility ends; a lease that is not acknowledged before then is
	// delivered again — up to the queue's attempts, after which it is a dead letter.
	Lease(ctx context.Context, queue string, o LeaseOptions) ([]Leased, error)
	// Resume is a lease from its handle, on any replica: a run's lease moves with its heartbeats.
	Resume(handle []byte) (Leased, error)
	// Inbox opens a queue of the caller's own, for a live request: its replies.
	Inbox(ctx context.Context) (Inbox, error)
}

type Inbox interface {
	// Address is the queue jobs for this inbox are enqueued on, by anyone, from anywhere.
	Address() string
	// Next waits for the next job enqueued on it, until ctx ends.
	Next(ctx context.Context) (Job, error)
	// Close ends the inbox: what is enqueued on it after is dropped.
	Close() error
}
```

A lease waits on the backend — NATS's pull, Pub/Sub's streaming pull, SQS's long poll of up to
twenty seconds, which is the protocol's wait and not ok asking again — and returns as a job is due.
A job due later (NotBefore) is held by the backend until then: NATS by its message schedule; SQS by
its delay, past fifteen minutes by enqueueing it again when it comes up early; Pub/Sub, which has
no delays, by hiding a job leased early until it is due — its ack deadline, up to ten minutes at a
time — so Pub/Sub delivers it again then.

An inbox is a reply address for one live request, and ends with it. Each backend carries inboxes as
it can: NATS gives each a subject of a stream of inboxes and a consumer of its own; Pub/Sub and SQS,
where making a queue takes long, carry every inbox of a process on one queue of the process's own,
made as it starts, and hand each job to the inbox it names. What is enqueued on an inbox whose
request ended is dropped: no one is waiting for it.

There are no streams — no topic every group of subscribers reads, no subscription with a position —
and no pub/sub signals. Where a service told others what happened, it enqueues work on each of their
queues (*Events between services*); where a node heard of a change to refresh what it kept, it keeps
nothing (*The rule*); where a request waited to hear, it has an inbox (*Waiting*).

## Patterns

### Work

A queue item names work: a repository's compaction, a run to dispatch, a landing to record. The
worker that leases it takes the work's claim in the database (keyed work, `internal/work`) and does
it; a second item for work held by another leaves its mark for the holder, which does it again
before it lets go. An item is enqueued in the transaction that makes the work due — before it
commits, so a committed change never lacks its work; one whose transaction did not commit finds
nothing to do, and is dropped. Work the item cannot finish now is put off: a copy of the item due
later.

What a long piece of work has reached — a window of a repository's tail, a page of a listing — goes
in the next item's body, not in the worker: the worker that takes it goes on from there.

### Events between services

What a service does that another acts on is first a row of its database: a repository's log of
events, the directory's. The relay that reads new rows enqueues, for each service that consumes
them, an item on that service's queue — keyed by the repository or the directory — and marks the
rows relayed once every consumer's item is enqueued. Any worker of the consuming service takes the
item, takes the key's claim, and applies every row after its cursor — its cursor is a row of its
own database — to the log's end, in order; an item for a key another worker holds marks it for that
worker. A service that starts consuming a log, or comes back from a release that did not, catches up
by work marked once (`work.Once`).

### Waiting

A live request that waits for something — a CL's status to change, its upload's CL number, the feed
to apply what an action did, a run's log to grow, a run to end — opens an inbox and registers: a row
of the database naming what it waits for, the inbox's address, and when the registration lapses.
The transaction that changes what it waits for enqueues an item naming the change, before it
commits. Any worker takes that item, reads the registrations for what changed, and enqueues a job on
each one's inbox. The request takes the job from its inbox, reads what it waits for from the
database, and answers or waits again; as it ends it deletes its registration. A registration whose
request died lapses: a job due when it lapses deletes it.

A request reads what it waits for after it registers, so a change between the two is read, not
missed. A change no one waits for costs one item and one read of no registrations.

What a request registers on is what can change its answer, and no more: every waiter woken by a
change wakes and reads, so a subject many wait on that changes often is a herd. A CL's status watch
registers on its CL and the stack below it, and of main's head on what governs every CL (settings,
OWNERS files, the groups-and-ACLs tree) and the paths the CL changes — a move of main is one item,
whose worker diffs it once and tells the watchers of what it changed; a CL too large to name its
paths is told of every move. A read never says it changed what it read: a write that changes
nothing — the projection of an unchanged CL a status read makes — enqueues nothing, or each read
would wake its own reader.

### Timers

Work due at a time is marked due then in the database (`work.MarkAt`), with its job due then on the
queue, or is a queue item due then. A daily collection, a lease's lapse, a candidate coming of age,
a directory's periodic sync: each is a job due at its time, and any worker takes it.

## The change

What ok held, polled, or listened to, and where it goes. Each phase is a change of its own, tested on
PostgreSQL and on Spanner, on NATS.

0. **Landings write in a few round trips.** A landing of a batch of CLs merged each CL onto the one
   before it inside its transaction, reading each tree a round trip at a time, and wrote each CL's
   rows a statement at a time: 352 round trips for 32 CLs on staging, 685 from a replica with
   nothing cached, all while main was held. The merges are computed before the transaction, from
   trees read a level at a time; the transaction checks main and the CLs, and writes the batch's
   rows together — split to stay inside a commit's limits (Spanner's 80,000 mutations and 100 MB).
   ok automation records a landing likewise.
1. **The bus is queues.** Inboxes and due times join the queue; Kafka, which only carried streams,
   is deleted; NATS, Pub/Sub, and SQS each carry inboxes.
2. **Events are queue items.** The SCM's events and the directory's reach ok automation, control,
   and deploy, and the directory's the SCM, as items on each one's queue, applied by keyed work.
   Streams, subscriptions, and the follower that restarted itself every two seconds go.
3. **Waits are registrations and inboxes.** A CL's status watch, which read everything every two
   seconds; the CL, run, and deploy pages' live regions, which asked every two or three — streamed
   over server-sent events now; an upload waiting for its CL's number; an action waiting for the
   feed; a run's log tail, and a wait for a run to end.
4. **The SCM keeps nothing that changes.** A content index's view, a repository's segment manifest,
   and other replicas' sequencing passes are read from the database each request — a manifest once
   for a request's life; what a replica keeps of an index is its generation's log to a place, which
   within the generation only grows — and segments' readers and blocks, named by ID, stay cached.
   Compaction's place in a repository's tail is a row of the database beside it, written in the
   transaction that moves what it found. Then signals, streams, the PostgreSQL bus, `pgdata.Listen`,
   and every NOTIFY are deleted, the bus's configuration is one backend (`bus: {type: …}`), and
   tests run on a NATS server of their own.
5. **No copies of permissions or keys.** Authorization's facts, resolved credentials, and secrets'
   keys are read each request.
6. **No owned timers, no polling.** The directory's periodic sync and identity's housekeeping are
   keyed work booked for when each is next due, a SCIM push booking a sync a moment on; the landing
   queue's candidate collection is booked by a pass that finds nothing left to do, and done holding
   the queue's claim; the lock package's waiters — a sync asked for while another runs, a fold that
   must follow what its caller did, a full compaction — are claims' waiters, told by their inbox as
   the holder lets go (`wait.Claims`). The lock package keeps one user, the migration that runs
   before a role's schema exists, and the deployer, which takes a deployment's lock beside its claim
   for one release. What renews a lease on a timer — a claim's holder, a tunnel's session — renews
   what it holds; a worker, the deploy agent, and the command line keep their own clocks, outside
   the service.
7. **Measured.** Staging's steady load and its herd, before and after (2026-10-29, GKE with 12
   n2d-standard-8 nodes and 9 server replicas, Spanner at 3,000 processing units, NATS; 3,000
   people in one monorepo; a run's numbers over its 10 minutes of load, a herd's over 30). Each
   build ran on databases of its own, its steady run then its herd:

   | | before | after |
   |---|---|---|
   | Steady: CLs landed, p50 / p99 | 32.0 s / 47.3 s | 33.6 s / 49.6 s |
   | Steady: publish, checkpoint p99 | 3.7 s, 3.1 s | 2.0 s, 1.5 s |
   | Steady: server cores | 10.4 | 10.8 |
   | Steady: Spanner CPU, mean (the SCM's, identity's) | 22.7% (16.0%, 2.3%) | 32.5% (20.0%, 5.6%) |
   | Herd: CLs landed | 3,622 of 3,622 | 3,673 of 3,673 |
   | Herd: the burst's CLs landed, p50 / p99 | 974 s / 1,370 s | 692 s / 841 s |
   | Herd: server core-seconds | 38,008 | 13,215 |
   | Herd: Spanner CPU, mean / max | 39.7% / 75% | 34.2% / 102% |

   What staging found on the way: a watch's status read woke the watch itself, every watch
   registered on main's head, each read began its transaction with a round trip, and calls between
   ok serve's roles were each checked against the directory (the changes after the state change).
   What it costs now: a person's request asks identity for the repository's facts — several reads,
   where a replica kept them — and the SCM reads a repository's segment manifest each request, 70%
   more reads of it.
