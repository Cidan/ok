# The deployer

After a commit lands on `main`, ok's deployer puts what it changed into the cluster: it runs the
migrations the commit affects, has ok automation's workers push the images it affects, and rolls
every long-lived service whose image changed — in watched stages, in waves, rolling back what
fails. In the repository that holds ok's groups-and-ACLs tree, it also applies the tree to ok
identity.

Related: [automation.md](automation.md) (pre, post, the landing queue, workflow files),
[scm.md](scm.md) (`main`, the change feed), [identity.md](identity.md) (the tree),
[configuration.md](configuration.md) (the `deploy:` section).

## What deploy is

- **In a Kubernetes cluster, images only, long-lived services only.** A deploy rolls a Deployment
  or a StatefulSet in a namespace to images Bazel builds and pushes. Jobs, CronJobs, and anything
  outside the cluster are not deploys.
- **Only from `main`.** A deploy runs for commits that have landed, never for a CL.
- **Everything a commit affects deploys.** A deploy is due when a landed commit affects any of its
  targets — through anything they are built from, so a change to a shared library or
  `MODULE.bazel` redeploys every service built on it. An affected image that comes out
  byte-identical has the digest already running, and nothing rolls.

### Vocabulary

- **Deploy** — a `deploy` stanza in a workflow file: migrations, images, and the workload they
  roll.
- **Deployment** — one execution of a deploy, for one commit on `main`.
- **Plan** — the deployer's reading of `main` at one head: every deploy declared there, and which
  of them the commits since the previous plan make due.
- **Wave** — the deployments of one plan run in three waves.
- **Stage** — one step of a roll (canary, then full), each watched.
- **Blue and green** — a Deployment's two copies: blue is the one the installation's manifests
  declare, green its copy the deployer makes. Each deployment rolls the idle one.

## The stanza

`deploy` is a phase of workflow files beside `pre` and `post` ([automation.md](automation.md),
"Workflow files"): read from `main`, changed only through OWNERS review, validated at upload and in
pre.

```yaml
# //services/api/.ok/workflow/api.yaml
deploy:
  api:                                    # its identity: //<file>:deploy:api
    migrate:                              # run in order, before any image goes out
      - //services/api/db:migrate         # a binary speaking ok's migrate protocol
    images:                               # container → the rules_oci oci_push that makes its image
      api: //services/api:push
    workload:
      namespace: api
      deployment: api                     # or statefulset: <name>, exactly one
    group: storefront                     # the Services pages' group (see Services)

# //services/billing/db/.ok/workflow/db.yaml — data with no service behind it
deploy:
  billing-db:
    migrate:
      - //services/billing/db:migrate
```

- **Keys.** A deploy's keys are `migrate`, `images`, `workload`, `group`, `alone`, and `secrets`
  — the secrets its migrations read, `//secret/<path>`, which its folder's `acl.yaml` must give the
  deploy (`deploys:`), each a file in `$OK_SECRETS_DIR` while a migration runs
  ([secrets.md](secrets.md), "Readers that are not people"). `images`
  and `workload` come together or not at all; a deploy has at least `migrate` or `images`.
  `workload` is `namespace` and exactly one of `deployment` and `statefulset`. `group` names the
  group its service is shown in, so only a deploy with a workload has one. `alone: true` makes its
  deployments run while no other does (*Waves*). Anything else is an error, as is a key given
  twice. Deploy and group names follow job names (lowercase letters, digits, `_.-`, at most 64).
- **Targets.** Every `images` value is a [rules_oci](https://github.com/bazel-contrib/rules_oci)
  `oci_push` target; every `migrate` entry a runnable target (`*_binary`) speaking ok's migrate
  protocol (*Migrations*). `images` names each container the deploy rolls; a workload's other
  containers keep their images. The containers must exist in the workload, which the deployer
  checks when it rolls.
- **One workload, one deploy.** Two deploys naming the same workload would fight: pre refuses a CL
  that makes them, and the deployer fails both.
- **The timings are the system's:** no deploy sets its own.
- **Where the file lives does not matter** to what triggers a deploy: its targets do.

What needs no build graph — the stanza's schema — is checked at upload; the target kinds and the
one-workload rule are checked in pre, on a worker with Bazel.

## When a deploy runs

The deployer follows `main` (the SCM's change feed). Each time `main` moves it plans at the newest
head, H — heads that moved past while it planned are covered by the next plan, never skipped:

1. **The changes since the last plan.** The files changed between the previous plan's head, P,
   and H. The first plan has no P: every deploy is due.
2. **Every deploy at H.** The deployer reads every workflow file in its checkout of H.
3. **Which are due.** What the changed files affect, from the Bazel graphs at P and H
   ([automation.md](automation.md), "Affected targets"): a deploy is due when any of its targets
   is affected, or its workflow file changed. The deployer keeps each head's graph, so P's is
   queried once, when it was H.

Each due deploy gets a **deployment** at H, covering the commits after P up to H — so every CL
landed between them shows it. A deploy has one deployment running at a time; a newer one waits,
and replaces any deployment of the same deploy still waiting (the newest commit wins).

**A replacement takes the replaced deployment's place.** `main` is one line: a later commit
carries every earlier one, so a deployment that replaces a waiting one carries whatever change made
the earlier plan's waves. It keeps the replaced deployment's **gates** — its wave in its plan, and
the places that one had taken in turn — and starts only once each of those plans' earlier waves
succeeded; a failure of one of those plans stops it too.

### The groups-and-ACLs tree

The repository the configuration names as the tree's (`identity.tree_repository`) has one deploy
more, built in: `//.ok/repository.yaml:deploy:identity`, declared by its settings, which name the
tree's folder. It is due on the first plan, and whenever a change touches the tree or the settings,
and its one step applies the tree at its commit to ok identity (`Tree.Apply`), ordered by the
commit's place on `main` so an older tree never replaces a newer one. ok identity refusing the tree
— one that would delete an ACL that still owns sets, say — fails the deployment, and the tree ok
identity holds stays as it was. Another repository's `identity:` folder is files like any.

### Imported repositories

An imported repository ([automation.md](automation.md), "Imported repositories") deploys nothing.
Each plan at its head reads its settings first, and records itself with no deployment and the note
"the repository is imported: ok deploys nothing from it" — without reading its workflow files or
running Bazel.

**Manual deploys.** Anyone who may act on the repository's CLs can **Retry** a deployment that
failed or was halted, which runs it again, at its commit, as a new deployment. A running deployment
can be **halted**, which rolls it back.

## A deployment

### 1. Push

Each image's `oci_push` runs on a worker, like any job: `bazel run //…:push -- --tag <commit>`, the
image's digest read from its OCI layout. The pushes of a deployment run in parallel, on workers,
which hold the registry credentials Bazel's push needs; the deployer never builds. A push that
fails fails the deployment; nothing has changed.

### 2. Migrations

The deployer runs each `migrate` target in order, on its checkout of H, with the environment the
configuration gives migrations (`deploy.migration_env`: where each finds its database). A migration
speaks **ok's migrate protocol** (`github.com/Cidan/ok/migrate`):

```
bazel run //services/api/db:migrate -- plan
bazel run //services/api/db:migrate -- apply [--allow-hazard CLASS]...
```

Either prints one JSON report on its standard output — the database and server, each step with its
SQL and hazards, the gated classes it refused, whether it executed, and the error that ended it —
and exits 0 when it planned or applied, 2 when it refused, and 1 when it failed. The deployer runs
`apply`, keeps the report, and shows it on the deployment.

- **Hazards.** Every step is classified: `DELETES_DATA` (drops or empties a table, column, sequence,
  schema, or extension, or deletes rows), `ACCESS_EXCLUSIVE_LOCK`, `TABLE_REWRITE`, `INDEX_BUILD`,
  `VALIDATION_SCAN`, `MAY_FAIL_ON_DATA`. Each is shown; `DELETES_DATA` is **gated**.
- **A gated hazard** stops the deployment before it applies anything, waiting for a person: a
  repository admin allows the plan's hazard classes on the CL's page, the allowance is recorded
  with who gave it, and the deployer applies with exactly those. A deployment waiting for an
  allowance holds its deploy's later deployments behind it; halting it lets them run.
- **A migration that fails** fails the deployment. No image has gone out; what earlier migrations
  applied stays.

Migrations run before any image goes out, so a service never starts before its schema exists.

**ok's PostgreSQL migrations** (`github.com/Cidan/ok/migrate/postgres`) are a ready migrator:
ordered SQL files, each applied once in a transaction of its own and recorded, their hazards read
from the statements and from `-- hazard: CLASS detail` lines a file declares for what statements
cannot show. A migration program is a few lines:

```go
package main

import (
	"embed"

	"github.com/Cidan/ok/migrate/postgres"
)

//go:embed *.sql
var files embed.FS

func main() { postgres.Main(files) } // -url-env APP_DATABASE_URL, from the BUILD rule's args
```

Any program that speaks the protocol works: a wrapper around another migration tool, or a schema
engine that plans by diffing.

### 3. Roll

When every pushed digest is the one already running, there is nothing to roll and the deployment
succeeds after its migrations. Otherwise the deployer rolls the workload in stages.

**A Deployment rolls blue/green.** The deployer keeps a second copy, green (`<name>-green`, labelled
so the workload's Service selects it too), beside the declared one, blue. The idle copy gets the
new pod template — always blue's (the declared manifest is the one pod spec), with the new images —
and the replica count of the live copy, N:

- **N = 1:** the live copy is scaled to 0 and its pod gone, then the new copy to 1: a workload with
  one replica may hold what only one pod may (a volume, a singleton), so the two never run
  together. One stage.
- **N > 1:** *canary* — the new copy to 1, beside the live copy's N; then *full* — the new copy to
  N, and once its pods are Ready the live copy to 0. Two stages.
- **Rollback** is scaling: the new copy to 0 and the live copy back to N. The old pods' template
  never changed.
- **Success** leaves the new copy live and the old at 0 replicas, ready for the next deployment.

A green copy is blue's pod template, with the new images and the label `deploy.ok.<domain>/color:
green` on its pods and its selector, annotated `deploy.ok.<domain>/copy-of`. The workload's Service
selects both copies' pods, by the labels they share. Blue's selector also matches green's pods,
which is sound because Kubernetes' controllers manage only the pods they own. A change to blue's
manifest reaches the running pods at the workload's next deployment, which copies blue's template;
the installation's manifests should leave a deployed Deployment's `replicas` to the deployer.

**A StatefulSet rolls in place:** a second copy cannot share its volumes. The deployer updates its
template's images with a rolling-update partition: *canary* updates the highest-ordinal pod
(partition N−1), *full* the rest (partition 0); with one replica there is one stage. Rollback
restores the previous images, recorded before the roll in a step of their own.

### 4. Watch

Every stage is watched the same way:

- The stage's new pods must be **Ready within 3 minutes** of starting.
- The stage **passes 30 seconds after its pods started**, with no failure.
- **Failure** is a pod not Ready in time, any container restart (a crash loop, an out-of-memory
  kill), an image that cannot be pulled, a pod that fails or is evicted, or Ready turning false.

A failure halts the deployment and rolls it back — the canary with it. A rollback that cannot
complete (the old pods do not come back) stops there and says so; nothing loops.

## Waves

A plan that makes several deployments runs them in three waves, so a change that breaks everything
it touches breaks one service first:

1. **One deployment**, all the way through.
2. **Half of the rest**, together (rounded up).
3. **The rest.**

Within a plan, multi-replica workloads go before single-replica ones (their canaries serve beside
healthy replicas); ties go by name. Migration-only deployments take their turn like any other.

**A deploy that runs alone** (`alone: true`) is one whose roll interrupts what other services need
— a database's, say. Each takes a wave of its own after the rest's, and its deployment runs while
**no other runs, of any plan**: it starts once every running deployment has finished, and no
deployment starts until it has.

**The deployer's own deploy takes a wave of its own after the rest**: it starts only once every
other deployment of its plan has succeeded, so it never rolls itself while driving another roll,
and its failure stops no other.

**A failure stops the plan:** every deployment of the plan still running is halted and rolled back,
and those not started are canceled. Deployments that already succeeded stay.

## Grants

The deployer holds nothing cluster-wide. Each namespace whose workloads it rolls grants it, as
Kubernetes RBAC to its ServiceAccount, exactly what rolling needs:

| Resource | Verbs | Why |
|---|---|---|
| `apps/deployments` `<name>`, `<name>-green` | `get`, `update` | read the workload, scale and re-template its copies |
| `apps/deployments` | `create` | make the green copy (Kubernetes cannot limit `create` to a name) |
| `apps/statefulsets` `<name>` | `get`, `update` | read it, set its images and partition |
| `pods` | `list` | watch the new pods, and observe what runs |

A deploy whose workload grants nothing fails its roll as a configuration error naming the missing
grant, and the Services page shows it. An admission policy can hold the deployer to its part: that
it creates only `-green` copies of Deployments it may update, never changes a workload's
ServiceAccount, and adds no host access.

## Where it runs

- **The deployer** is a role of its own (`ok deploy serve`), on its tables (the schema `deploy`).
  It follows `main`, checks each commit it plans out — through okfs, reading ahead only the build
  graph and the workflow files planning reads, or written to disk (`deploy.checkout`) — plans with
  Bazel, runs migrations, rolls, and watches. No replica leads ([hosted.md](hosted.md), "Work
  without leaders"): every replica plans each repository whose main moved under that repository's
  lease, starts the deployments whose turn has come in one transaction at a time — so no two
  deployments ever hold one deploy or workload, whichever replicas start them — and steps each
  deployment under that deployment's lock, every write it makes fenced by it, so a replica that
  stalled past its lease writes nothing after another has taken over.
- **Workers build and push.** A push is a job the deployer asks ok automation to run (its `Pushes`
  service); the worker runs Bazel and reports the digest. Workers never hold the deployer's
  permissions, and the deployer never builds an image.
- **ok control** shows deployments on the CLs they cover and asks the deployer to retry, halt, and
  allow a hazard, for a person it has authorized.

## Deploy agents

An organization's workloads may run in a cluster of its own, which ok's deployer holds no credential
to: its **deploy agent** runs there and connects out to ok, and the deployer rolls and migrates
through it ([hosted.md](hosted.md), "Deploy agents, as built"). A hosted service deploys every
repository so; a single installation deploys to its own cluster unless an owner designates an agent.

1. **A robot.** The organization's owners make a robot (`CreateOrganizationRobot`), designate it its
   deploy agent (ok identity's `DeployAgents.SetDeployAgent`), and grant it read on the
   repositories whose migrations it runs.
2. **Its sign-in.** Either the owners trust the cluster's ServiceAccount issuer (`TrustIssuer`) and
   bind the robot to the agent's ServiceAccount (`BindOrganizationRobot`,
   `system:serviceaccount:<namespace>:<release>-agent`), its token's audience the organization's ID;
   or they certify a key for it (`CertifyOrganizationRobot`) and keep it in a Secret.
3. **The agent.** The `ok-agent` chart (`deploy/helm/ok-agent`) runs it: `address` (ok's public
   address), `organizationID` or `credentialSecret`, `namespaces` (each granted what *Grants*
   lists), and `migrations.env` (the environment migrations run with, from Secrets — where they find
   their databases). It runs one agent: a newer connection of an organization's agent replaces the
   older.

```
ok deploy agent -address ok.example.com:443 -token-file /var/run/secrets/ok/token/token -state /var/lib/ok-agent
```

What the agent does for a deployer is narrow, whatever its RBAC allows: it reads and rolls
Deployments and StatefulSets, reads ReplicaSets and pods, and reads discovery — never a Secret, a
pod's exec, attach, log, or port, a proxy, a token, a delete — and it passes on no header that
authenticates or impersonates. It runs a migration only of its organization's repositories, only at
a commit on main, checked out as its robot. While it is not connected, a repository's plans and
deployments wait — the deployment's log and the Services page say so — and go on once it is.

Each deployer replica advertises where the others reach it (`OK_DEPLOY_ADDRESS`, its pod's address
and port; the chart sets it), so whichever replica an agent connected to, every replica's requests
reach it.

## Resuming, and deploying the deployer

The deployer may restart in the middle of a deployment — a node drains, or a change to ok deploys
the deployer itself.

- **Every decision is taken again from the database and the cluster.** A deployment's stage, what
  it rolled from, and when each stage began are stored; the watch reads pod start times, readiness,
  and restart counts from the cluster, not from timers in memory. Whichever replica takes a
  deployment's lock resumes it where it stands.
- **Its own deployment is driven by the old version** until the new one has passed. Each copy of
  the deployer knows which it is — its pod's `deploy.ok.<domain>/color` label, given it as
  `OK_DEPLOY_COLOR` through the downward API — and the deployment records the copy that started it:
  only that copy drives it. The old copy keeps running through every stage, records the success,
  and only then scales itself to 0; if it stops before it does, whoever finds the success finishes
  it. A new deployer that crash-loops is rolled back by the old one.
- **It goes last**, in a wave of its own (*Waves*), so a change that breaks every service is caught
  before it reaches the deployer.

## Services

A **service** is a deploy that rolls a workload. Each plan records the services `main` declares at
its head. For each, the deployer answers what ok deployed — its latest deployment that succeeded:
the images by digest, the commit they were built at — its latest deployment of any status, and what
the cluster runs: the live copy, the replicas it wants and has Ready, and each pod's copy,
readiness, restarts, and the image digest each container actually runs. ok control's Services
pages show them, read-only.

**Groups.** A deploy's `group` puts its service with others that make up one system, and the
Services pages show groups: one namespace across repositories, so a group's services may be
declared anywhere, and a service with no group is a group of its own. A group's status is its worst
service's (Last Deploy Failed, then Degraded, Unknown, Drifted, Deploying, Not Deployed Yet,
Healthy).

**What runs is read in the background, never on a request.** The deployer's observer reads each
service's workload from the cluster and records it, ten seconds after its last read ended: each
service's reading is keyed work, taken by whichever replica is free, one at a time, its record
fenced by its lease. A request reads only what was recorded.

## On the CL

A deployment covers every CL landed in its range, and each CL's page shows its deployments on its
**Deployments** tab; each opens its page: the deploy, its stage or outcome, the pushed images, the
migrations' plans, the push runs' logs, and the actions — Retry, Halt, and Allow for a waiting
hazard. A failed deployment offers Roll Back of the CLs it covers ([automation.md](automation.md),
"Rollbacks").

**Who may act:** Retry and Halt, the authors of the CLs a deployment covers and repository admins;
allowing a hazard, repository admins.
