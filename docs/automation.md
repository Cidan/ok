# ok automation — pre and post jobs, lint, and the landing queue

ok automation checks every change before it lands and keeps `main` green: **pre** runs on every
patchset, **post** on the exact commit that would land, and the **landing queue** moves `main` only
on what passed. Every job is a [Bazel](https://bazel.build) command on a worker. Deploys after
landing are [deploy.md](deploy.md).

Related: [scm.md](scm.md) (candidates, landing, `Export`), [changelists.md](changelists.md)
(ChangeLists and submit), [owners.md](owners.md) (OWNERS, whose directory resolution workflows
share), [configuration.md](configuration.md) (the role and its workers).

## Vocabulary

- **Submit** lands a CL on `main`, the only line of history.
- **pre** — jobs that run on a CL in review: on a stack, once, at its top, over every CL of it.
  They always block submit.
- **The top of a stack** — a CL in review that no CL in review is stacked on. A CL stacked on
  nothing is its own stack, and its own top.
- **post** — jobs that run at submit on the CL's **candidate**, the exact commit that would land,
  before `main` moves. `main` advances only once they pass.
- **Job** — one Bazel target that must succeed, with optional arguments.
- **Built-in job** — a job ok always runs, without any workflow naming it: the build, tests, and
  lint.
- **Workflow file** — any `*.yaml` in a directory's `.ok/workflow/`, declaring jobs.
- **Affected set** — every Bazel target a change can alter: the targets whose inputs it changes,
  and everything that depends on them, however indirectly (see [Affected
  targets](#affected-targets)).
- **Job context** — what ok tells a job about the change it runs for: the changed files, the CL,
  the commits, the phase.

## Principles

1. **ok-native.** No GitHub Actions: no event triggers, no inline shell steps. A repository's
   `.github/workflows` means nothing to ok.
2. **Every job is a Bazel target.** Bazel already knows what every target is, what it depends on,
   and how to build, test, or run it. No workflow contains a command string.
3. **Every repository is a Bazel workspace, or is imported.** A repository's settings
   (`.ok/repository.yaml`) say `build: bazel`, or `build: none` for a repository brought into ok as
   it was, on which ok runs nothing (see [Imported repositories](#imported-repositories)).
4. **Pure TAP.** Every test a change can affect runs, whether or not any workflow names it; only
   what the change can affect executes; everything else keeps its result from `main`.
5. **No bad code is checked in.** pre always blocks submit and post always gates `main`. Every
   commit on a Bazel workspace's `main` has passed every test and lint its change affects, and
   every job its workflows select. There are no advisory jobs and no overrides.
6. **Jobs are first-class**, shown on the CL and on `main` with status, logs, rerun and cancel,
   and why each ran.

## What runs

A CL's jobs for a phase are the union, de-duplicated by job identity, of the built-in jobs and the
workflow jobs, over the change's **affected set**.

### Affected targets

`bazel query` does not answer "what does this change affect" for edits to `.bzl` files, macros, or
attributes, so ok asks a sharper question — which targets differ between the change and its base —
the way `bazel-diff` and target-determinator do (`internal/buildsys/bazel`):

1. At each commit, `bazel query 'deps(//...)' --output=streamed_jsonproto
   --proto:include_synthetic_attribute_hash --noproto:locations --noproto:default_values`. A worker
   keeps each commit's answer, so `main`'s head is queried once.
2. Each target's **own hash**: a rule's `$internal_attr_hash` — Bazel's hash of its attributes and
   its rule's Starlark definition, with every `.bzl` it loads — and a source file's content. Only
   the files the change touches are read, at both commits: every other file is the same at both.
   Its **hash** is its own and its inputs', so a change reaches everything downstream, through
   generated files to the rules that generate them.
3. **Affected** is every main-repository rule whose hash differs or that is new; **direct**, those
   whose own hash differs or that read a changed file; **upstream**, what the affected depend on;
   **tests**, the affected rules whose class ends in `_test`, and `test_suite`s. Each affected
   target says why: a chain from it to the changed file — a source it reads, or its package's
   BUILD file.
4. **Everything is affected** when the change touches `MODULE.bazel`, `MODULE.bazel.lock`,
   `.bazelversion`, `.bazelrc`, `WORKSPACE*`, `.ok/repository.yaml`, or a `toolchain` target: a
   toolchain is resolved when Bazel analyzes, out of a query's sight, so who uses one cannot be
   told.
5. A deleted file a target still names makes it affected; a file no target reads affects no target,
   and still selects its workflow directory's jobs. A base whose graph does not load affects
   nothing the change rests on: every target is checked.

### Built-in jobs

ok runs these on every CL; no workflow declares them, and none can remove them.

- **The graph.** The change leaves a graph Bazel can load, and its workflow files name targets
  that exist and can back them — the plan, pre and post. A change that leaves no graph fails its
  plan.
- **Build** — `bazel build --keep_going` of the affected roots that are not tests (a test builds
  what it needs) — pre and post.
- **Tests** — `bazel test` of every affected test, one job each — pre and post.
- **Lint** — the repository's linters over the change (see [Lint](#lint)) — pre and post, when the
  repository names any.
- **OWNERS** — a change that touches an OWNERS file, the groups-and-ACLs tree, or the repository's
  settings fails its plan when any owner set could not be approved by its own people
  ([owners.md](owners.md), "Approvability").

### Workflow jobs

Workflows add what is not a test or a lint: builds with parameters, end-to-end suites that belong
in post only, checks a directory wants on its changes.

- A **changed file's** jobs are its governing workflow directory's: the closest directory at or
  above the file with a `.ok/workflow/`, stopping at the repository root. Resolution stops at the
  closest one: a directory's workflows replace those above it. Changed files are counted as for
  OWNERS: the CL's diff against its base, both paths of a rename, and the old path of a deletion.
- An **affected target's** jobs are its package directory's governing workflow directory's, so a
  change to a library brings in the workflows of everything built on it.
- An **upstream target's** jobs are its package directory's governing workflow directory's too,
  so a change shows the state of what it builds on.
- A workflow job whose target is not affected keeps its result from `main` rather than running
  again.

## Workflow files

### Location and resolution

- A directory declares automation in `<dir>/.ok/workflow/*.yaml`. Every YAML file there is
  read, and any file may declare jobs for any phase.
- **Includes** pull in shared workflows. `include:` lists workflow files by `//` path; each must
  be declared by a proper ancestor of the directory the including file declares automation for
  (`//services/api/.ok/workflow/api.yaml` may include `//services/.ok/workflow/*.yaml` and
  `//.ok/workflow/*.yaml`), never by the same directory, a sibling, or anything below it.
  Included files may include further; every include points strictly upward, so includes cannot
  cycle. An include brings in all of the file's jobs, in every phase.

### Trust

- Workflow files are always read from `main`, never from the CL, so a CL cannot weaken or skip its
  own gates. An edit to a workflow file takes effect once it lands.
- Workflow files are ordinary files under OWNERS: changing one needs approval from its
  directory's owners.
- A CL that adds or edits a workflow file fails pre, naming the file and line, when the file fails
  its schema, an include does not resolve or is not an ancestor, a job's target does not exist in
  the Bazel graph at the CL's revision, its kind cannot back the job, or an argument or
  placeholder is not valid for it. What needs no build graph — the schema, the includes, the
  placeholders — is checked already at upload, which refuses the CL
  (`REASON_INVALID_WORKFLOW`), as it does an invalid OWNERS file; the rest is checked in pre, on
  a worker with Bazel. A CL that deletes or edits an included file is checked through every
  file that includes it. The parser and validator are `internal/workflow`, shared by the SCM
  and automation.

### Format

```yaml
# //services/api/.ok/workflow/api.yaml
include:
  - //.ok/workflow/common.yaml          # ancestors only
pre:
  build:
    target: //services/api:server
  schema-comments:
    target: //tools/sqlcheck:sqlcheck
    args: ["--require-comments", "{changed_files:**/*.sql}"]
post:
  e2e:
    target: //services/api/e2e:e2e_test
    pool: large                          # the pool of workers it runs in
```

- The keys are `include`, `pre`, `post`, and `deploy` ([deploy.md](deploy.md), "The stanza"); a
  job's are `target` (a Bazel label, `//package:name`), `args` (a list of strings), and `pool`.
  Anything else is an error, as is a key given twice. Deploys are not selected through directories or
  includes: every deploy in every workflow file counts once.
- Jobs are keyed by name: lowercase letters, digits, and `_.-`, at most 64, starting with a letter
  or digit. A job's identity is (defining file, phase, name), written
  `//<file>:<phase>:<name>`; de-duplication, history, and the UI key on it.
- A job means "this target must succeed". The target's kind decides what that means: a test
  target runs its tests, a runnable target (a `*_binary`) is run with `args`, in the sandbox ([Execution](#execution)), and anything else
  must build.
- `args` are the job's arguments, with [placeholders](#the-job-context) expanded.
- `pool` is the pool of workers the job runs in (`worker.pool`; [hosted.md](hosted.md), "Hosted
  automation, as built"): lowercase letters, digits, and `-`. A job that names none runs in its
  repository's (`pool:` in `.ok/repository.yaml` on main), else `default`; so do the built-in jobs.
- `secrets`, a post job's alone, are the secrets its binary reads, `//secret/<path>`, which its
  folder's `acl.yaml` must give the job (`runs:`): files in `$OK_SECRETS_DIR` within its sandbox,
  read with its lease's key ([secrets.md](secrets.md), "Readers that are not people"). Its target
  must be a binary: a test's and a build's actions are cached.

### The job context

Every job knows the change it runs for:

- **The context file.** ok writes `ok-context.json` into the job's checkout and names it in
  `OK_CONTEXT`: the phase; the repository; the CL, its patchset, and its author; the commit
  under test (the patchset, or the candidate) and its base; and every changed file with its status
  (added, modified, deleted, renamed, copied) and, for a rename or copy, its old path:

  ```json
  {"phase": "pre", "repository": {"id": 3, "name": "app"}, "cl": 42, "patchset": 2,
   "author": "ada.human.example.com", "commit": "<hex>", "base": "<hex>",
   "changed_files": [{"path": "docs/new.md", "status": "renamed", "old_path": "docs/old.md"}]}
  ```
- **Placeholders** in `args` expand from it, so the common case stays declarative:
  - `{changed_files}` — every changed file that exists at the commit under test;
  - `{changed_files:<glob>}` — those matching a glob (`**/*.go`, `services/api/**`);
  - `{deleted_files}`, `{deleted_files:<glob>}` — the paths the change removes (a deleted file, a
    rename's old path);
  - `{cl}`, `{patchset}`, `{commit}`, `{base}`, `{phase}`.

  A list placeholder is a whole argument; it expands to one argument per path, and to none when
  nothing matches — a job that must not run with an empty list checks for it itself. A scalar
  placeholder may sit inside an argument (`--cl={cl}`). `{{` and `}}` are literal braces. A glob
  matches the whole repository-relative path: `*` and `?` stop at `/`, `**` does not, and `**/`
  also matches no directory at all (`**/*.go` matches `a.go`); `[a-c]`, `[!a]`, and `{a,b}` work
  as in a shell.

- **Where it is made.** A change can touch any number of files, so its files never travel with
  a job: automation records the change — everything above but `changed_files` — and the worker
  that runs each job reads the files from the SCM, writes the context file, and expands the
  placeholders then. A plan holds a job's arguments as the workflow file writes them; planning
  expands them only to check them.

## Phases

### pre

- **Runs at the top of a stack**, against the top's current patchset, over the stack's whole
  change: its diff from where the stack leaves `main` — the base of the stack's lowest unsubmitted
  CL — so every path any CL of the stack changes is planned, tested, and linted once, at the top.
  The CLs under the top run no pre of their own: the top's tree holds every one of their changes,
  and a stack lands whole (post, below).
- **Not in draft.** A draft runs no pre until it is sent for review. `jj cl pre` runs it by hand,
  on any draft or CL in review — the same plan, over everything under it (ok control's `RunPre`,
  automation's `Runs.StartPre`).
- **The top moves.** A CL sent for review stacked on the top becomes the top: the old top's
  unfinished pre is canceled. When the top is abandoned or returns to draft, the CL under it, if
  in review, is the top again, and its pre is planned — or resumed, where it was canceled.
  `jj cl ready` sends a stack for review top first, so no CL about to be stacked on is planned.
- Always blocks submit: Submit is enabled only when every selected pre job has passed on the
  current patchset of the top of what is submitted. Submitting part of a stack — a CL others are
  stacked on — starts pre on that CL, if it has none, and is refused until it passes.
- A new patchset cancels the previous patchset's running jobs.

### post

- Runs at submit on the CL's **candidate**: the SCM prepares it (`PrepareCandidate`, the
  path-level splice of the CL onto `main`'s head, [scm.md](scm.md), "Submit"), unpinned and
  invisible through `main`.
- `main` advances — `Land` of exactly that candidate, against the head it was prepared on — only
  when every selected post job has passed on it. On a failure nothing lands, the CL returns to
  Open, and the failure shows on it.
- **What pre already proved is not run again.** Pure TAP applies between the patchset and its
  candidate as it does between `main` and a change: a job whose inputs are the same on both has
  the same result. post's planning run diffs the candidate against the commit the CL's pre ran
  on — what `main` brought in under the CL, since the candidate holds the CL's own paths as the
  patchset does — and asks the graphs what that difference affects. A build, test, or lint
  job none of whose targets it affects, nor the linters — the formatter's target, and anything in
  a lint aspect's package, which no query sees — whose arguments name nothing of the change, and
  whose repository settings are the patchset's, is **unchanged since pre**: when its latest pre run passed — the same built-in job, or a workflow
  job running the same Bazel command — post records it passed at once, carrying that run
  (`Run.carried_from`), and no worker runs it. A run job reads the job context, which names the
  commit under test, so it always runs; so does every post-only workflow job, which pre never
  ran. The build splits: `build` holds the affected roots pre built on the same inputs — every
  one of them, since an affected target unchanged since pre was affected in pre too — and
  `build since pre` builds the rest. A pass in pre is as good as a cached result, which is what
  running the job again would replay; like that cache, it holds for the Bazel of the time.
- A stack submits through the same pipeline, each CL's candidate built on the one below it, and
  post is planned once, on the top's candidate, over the submission's whole change: from the
  commit its lowest candidate is prepared on to the top's. The candidates under it land with it,
  each its own commit, and run nothing of their own: the top's candidate holds their changes, and
  carries what the top's pre proved.

### Deploy

Runs only after a commit has landed, with its own stages and rollbacks: [deploy.md](deploy.md).
Its stanza is a third phase of workflow files, `deploy:`.

## Imported repositories

A repository brought into ok as it was — holding code no Bazel graph builds and no linter has seen
— is **imported**: its `.ok/repository.yaml` says `build: none`, or it has none. ok checks nothing
in it.

- **Nothing runs.** pre and post plan each of its changes with no job: the planning run reads the
  settings, says in its log that the repository is imported, and passes, without running Bazel.
  Its workflow files select no job, and the deployer plans no deploy from it.
- **Review is unchanged:** an LGTM from someone other than the author, every OWNERS set's
  Approval, no unresolved comments. The SCM still validates OWNERS files, workflow files, and the
  settings at upload.
- **A change's standing** comes from its settings and, when it touches them, its base's
  (`workflow.StandingOf`):

  | The change's `build` | Its base's | The change |
  |---|---|---|
  | `none` | `none` | an imported repository's: nothing runs |
  | `none` | `bazel` | makes a Bazel workspace imported: its plan fails, so it never lands |
  | `bazel` | `none` | makes the repository a Bazel workspace: checked whole |
  | `bazel` | `bazel` | a Bazel workspace's, planned as any |

- **Never back.** A Bazel workspace never becomes imported: nothing would check that change, or
  any after it, and a change may not switch off its own gates — the reason workflow files are read
  from `main`.
- **Becoming a Bazel workspace** is one change that sets `build: bazel`. It is planned like any
  change, but whole: the settings are among the files that affect every target, so everything
  builds and every test runs; its lint is the whole repository's (`//...`), none of which was ever
  linted; and its workflow jobs are the change's own workflow files' — every one checked in pre —
  not `main`'s, which nothing checked while the repository was imported.
- **In the landing queue** an imported change's footprint is its paths alone, and a change that
  touches the settings conflicts with every change, so nothing lands unchecked beside the change
  that makes a repository a Bazel workspace.

## Rollbacks

A **rollback** is a new CL that undoes a landed CL.

- **Creating one.** Anyone may press Roll Back on a merged CL, saying why; ok control asks the
  SCM for the inverse change ([scm.md](scm.md), "Rollbacks"): every path the CL changed, set
  back to what it was before it landed, applied path by path on `main`'s head. A path a later
  landing has changed since is left as `main` has it, as in landing: the rollback is still
  created, without that path, and names it. Such paths are rolled back by hand in a CL stacked
  on the rollback (`jj cl get <n>`, then `jj new cl/<n>@ok`), reviewed like any change, and
  Submit Stack lands both. A CL that an open rollback already rolls back offers that rollback
  instead of Roll Back.
- **A stack rolls back as a unit.** Rolling back a CL in a stack rolls back it and every CL
  landed above it in the stack, as one rollback CL, so `main` never holds a stack's top without
  its bottom.
- **Its author** is the person who asked for it; its description names the CLs it undoes and why.
  Its reviewers are the rolled-back CL's, and the authors of every CL it rolls back. Each CL it
  rolls back names it in its change log and in a Rolled Back panel.
- **Expedited review.** A rollback needs one LGTM from anyone but its author — even where OWNERS
  allow self — and no OWNERS Approval for the inverse change the SCM computed. A later patchset's
  edits that differ from it are its author's own, and need their owners' Approval as any change
  does. Unresolved comments hold it back like any CL. Its pre and post jobs run and gate it like
  any CL's: a rollback may not land broken code either.
- **Rolling back a rollback** is a rollback like any other.

## Lint

Lint is the repository's own, named in its settings, and ok runs it as a built-in job:

```yaml
# .ok/repository.yaml
build: bazel
bazel:
  lint:
    aspects: ["//tools/lint:linters.bzl%eslint", "//tools/lint:linters.bzl%golangci_lint"]
    format: //tools/format:format     # a rules_lint format_multirun
pool: default                         # the pool of workers its jobs run in, unless a job names one
```

- **Aspects** ([rules_lint](https://github.com/aspect-build/rules_lint)'s, or any aspect with a
  `rules_lint_human` output group that fails on a finding) run over the rules that read the
  change's files, and those that held a file it deletes: `bazel build --keep_going
  --aspects=… --@aspect_rules_lint//lint:fail_on_violation --output_groups=rules_lint_human`. Code
  the change does not touch cannot gain a finding, so they do not run downstream.
- **The formatter** checks the change's files in check mode (`bazel run <format>.check -- files…`,
  in the sandbox), a thousand files to a command.
- **Every finding fails the job.** A repository that names neither has no lint job.

## The landing queue

At ~500 commits a minute, running post one CL at a time cannot keep up. Landing rests on one
invariant that hermeticity makes decidable:

> A CL's post result, obtained on `main` at head H0, still holds at a later head Hn exactly when no
> commit landed between H0 and Hn **conflicts** with it.

The queue follows Uber's SubmitQueue ("Keeping Master Green at Scale", EuroSys '19), in ok
automation (`internal/automation/queue`):

- **A submission** is what ok control hands the queue: one CL, or a stack bottom first, for
  the person who submitted it, once every gate but post has passed. A CL is in one active
  submission at a time; a new patchset, or the CL's being abandoned or returned to draft,
  cancels it; its author or a repository admin cancels it too.
- **An attempt** is a submission's candidates (`PrepareCandidate`) on one arrangement of main:
  main's head when it was prepared, and the earlier submissions it **speculates** land first (its
  chain), each chain CL's candidate prepared on the one before it and the submission's own on
  the last. Its top's candidate gets a post plan, over the whole submission, planned on a worker
  like pre's; a stack's every CL lands as its own commit on main, the top tested over all of
  them.
- **A footprint** is what a change can alter and what that rests on, from its post plan: its affected targets, their upstream, and the paths it touches. Two changes
  **conflict** when a target either affects is one the other affects or builds on — so an edge
  either adds, removes, or changes shows up on both sides — or either touches a file in a package
  the other reaches (a package one of them deletes has no targets left to be affected), or either
  touches the repository's settings, which decide how every file is checked — files outside every package
  too — and whether any is (an imported repository's). An imported repository's change, which
  affects no target, has its paths alone. Two
  changes that do not conflict keep their results whatever order they land in. Two that touch a
  common path (or one a path under the other's) cannot both land: the SCM refuses the second. A footprint is kept bounded, never missing anything: past 64 KiB, its paths become their
  directories a level at a time, each standing for everything under it (`/` for everything), and
  its targets become every target (`//...`) — so an import, a vendored tree, or a long stack
  conflicts with more and lands more alone, as a change that large does anyway. A submission whose
  attempt the store cannot record fails, saying why, rather than holding its repository's queue.
- **Grouping.** Every submission's first attempt is on main alone; its footprint — the
  submission's **reference**, re-read whenever it is tested on main alone again — is what the
  queue groups it by. A submission's chain is every earlier active submission it conflicts with,
  and theirs, in queue order, less those that touch a path it touches (**blockers**) and any that
  touch a path an earlier chain member touches: of two that cannot both land, the earlier one is
  predicted to.
- **Independent submissions** — an empty chain — run post in parallel, each on main, and land
  in any order with no re-run: the SCM re-splices a candidate onto a moved head.
- **A conflict group** speculates: a later submission runs post on a candidate stacked on the
  candidates of everything ahead of it that it conflicts with, predicting that they land first.
  Every submission has passed pre, so each is predicted to pass; the chain is bounded (depth 8,
  `automation.speculation_depth`), and a submission further back waits for its group to
  shorten. A submission lands only after everything in its chain has, and after its blockers
  have finished.
- **A failure** fails its submission once everything its attempt speculated on has landed — the
  failure then holds for main — at once for an attempt on main alone. When a submission a chain
  speculated on fails or is canceled, every attempt stacked on it is discarded and the queue tests
  again without it; Bazel's cache re-executes only what changed.
- **Landing.** A passed attempt lands, as one `Land` of its candidates against the current head,
  when every commit that landed since its base is in its chain or does not conflict with it. A
  conflicting commit — or one landed without the queue, whose footprint it cannot know — sends it
  back to be tested on the new head. The SCM's refusal (a path conflict, a CL no longer open)
  fails it with the SCM's words.
- **Batches.** The queue lands, in one `Land` — one transaction in the SCM, main moved once — the
  submissions it would land one after another: the first that may land, then each next that the
  queue, decided again as it would stand once the ones before had landed (`Snapshot.AfterLanding`),
  would land next — so a landing whose footprint interacts with the next ends the batch, and the
  next is tested again on top of it as it would be anyway. Each CL is committed for its own
  submitter (`LandRequest.Item.actor_id`). `automation.landing_batch` bounds a batch (32), and
  the SCM's 1,000 CLs a landing do too. A batch the SCM refuses lands its first submission alone,
  which says whose the refusal is. `ok_automation_landings_total` and
  `ok_automation_landed_submissions_total` count landings and what they landed.
- **Hot spots** — the repository's settings (which conflict with everything), `MODULE.bazel`,
  root configuration — conflict with nearly everything, so they form one long chain, each tested at its position: a batch whose every
  commit has passed, bisected by the rebases a failure causes. Testing only a batch's top would
  leave commits on main that never passed their tests (principle 5), so ok does not.
- **Results are cached by action inputs**, so a rebase re-executes only targets whose inputs
  actually changed. When nothing affecting a job has landed since the CL's pre, post carries
  pre's pass and does not run it at all (see [post](#post)).
- **Priority.** Post's runs go before pre's: main waits on them.
- **Crash safety.** Every decision is taken again from the database on each pass, so a restart
  resumes. Candidates no active attempt holds are discarded (`ListCandidates`) — one prepared
  just before a crash, after a grace period — and a CL main has though the queue lost the record
  of landing it is adopted when its attempt had passed. A batch lands whole or not at all, so a
  crash or a deploy between its landing and its records leaves every one of its submissions
  adoptable.

Each repository's queue is keyed work (`internal/work`; hosted.md, "Work without leaders"): a row
marked, in the same transaction, by everything that changes what it acts on — a submission, an
attempt made, released, or discarded, a post plan, a post run's end however it ends (finished,
released canceled, or lost with its worker), a commit on main — and done by one replica at a time,
whichever takes it first, and by every replica between them. The one thing time decides, a
candidate no attempt holds coming of age to be discarded, is a mark for that time. Every replica
looks for marked work as each mark's signal arrives (the bus's, sent once the mark commits), and at
least every ten seconds. A submission has one active attempt at a time:
the store refuses another while one is active, so its post never runs twice. Each submission
says what it waits for (its note: testing on top of which CLs, waiting for which to land), written
only when it changes.

A pass lands as soon as anything has passed: each landing lands a batch of the passed submissions in
queue order, up to 32, in one transaction of the SCM's, then records them all in one of its own
(`RecordLandings`), and the queue is read and decided again. Beside each landing, and once nothing
is left to land, it does everything else that reading said — preparing attempts, releasing them,
discarding, failing, canceling: with a landing every second at a busy monorepo's peak, doing them
only once landings stopped left attempts unprepared for up to a minute. An attempt prepared on main
as it lands is checked against what landed, as one another replica's landing raced is. Each
submission's actions are done in order, up to 16 submissions at once: preparing an attempt is a few
of the SCM's calls and a transaction, and one submission after another made a busy monorepo's pass
minutes long.
Attempts prepared at once that need one candidate (a chain's CL on one base) share its
preparation. Candidates no attempt holds are collected at most once a minute. Where each landed
submission's time went is a metric (`ok_automation_landing_stage_seconds`, by stage: queued,
plan_wait, planning, release, job_wait, job_run, land, total), as are each pass's and each action's
(`ok_automation_queue_pass_seconds`, `ok_automation_queue_action_seconds`).

A path conflict with main is refused, as in landing: `jj sync`, resolve, and upload again.

## Execution

- **ok automation** is a role of its own (`ok automation serve`), beside ok control and the SCM. It
  follows the SCM's change feed, plans each stack's pre at its top and each submission's post, dispatches
  jobs, runs the landing queue — preparing candidates and landing them through the SCM, which
  admits it to both — and records every job, run, and submission in its tables, and every run's
  log in its bucket (`automation.logs`). ok control shows what it records and asks it to submit,
  rerun, and cancel.
- **Planning is a job too.** Computing the affected set and reading the workflows needs a checkout
  and Bazel, so a patchset's first job, on a worker, plans the rest.
- **Workers** (`ok worker run`) take runs, each in a **slot**: a checkout of its own, and Bazel
  output bases of its own outside it, kept from run to run so each slot builds incrementally. A
  worker runs as many runs at once as it has slots, while its cgroup has the memory for another.
  The run's commit is checked out one of two ways (`worker.checkout`):
  - **virtual** (the default): presented read-only through okfs ([okfs.md](okfs.md), "ok's
    roles"), each file read from the SCM when something first reads it — ok's jj runs as the
    worker's daemon, in a user and mount namespace the worker makes for itself, and needs
    `/dev/fuse`;
  - **real**: written to disk from the SCM, only what differs from the slot's last commit, every
    file read-only — for a node without `/dev/fuse`.
- **Every Bazel command** a worker runs gets `--symlink_prefix=/` (no convenience symlinks in a
  read-only checkout), `--sandbox_default_allow_network=false` (actions and tests without network),
  the lockfile honored (`--lockfile_mode=error` when `MODULE.bazel.lock` exists), the installation's
  flags (`worker.bazel_flags`: a remote cache, say), then the repository's (`bazel.flags`). A test
  command prints a failing test's log (`--test_output=errors`).
- **The sandbox.** What a run job runs, and the formatter a lint runs, run under bubblewrap
  (`bazel run --run_under`): every file read-only but the run's own scratch directory, its
  `TMPDIR`; no network; no other process. A build's and a test's actions run in Bazel's own
  sandbox.
- **What a change's code sees.** Bazel, and everything it runs — repository rules, actions,
  tests — runs as the worker, with the worker's network: in a pod, Bazel's own sandbox often cannot
  isolate actions (its `linux-sandbox` needs a fresh `/proc`, which a pod refuses), and repository
  rules are never sandboxed. So every Bazel command runs in a mount namespace of its own in which
  the worker's secrets are covered — every secret file its configuration names, the configuration
  itself, the okfs daemon's state and credential, the worker's token — and so are the other slots'
  checkouts, output bases, and scratch directories. The view is the slot's, the same for each of
  its runs, so one Bazel server serves them all.
- **What persists.** A slot's Bazel server and output base, and the worker's download cache, stay
  from run to run, which is what makes builds incremental — so a change's code can affect what
  later runs on that slot build. A run's own scratch directory is removed when it ends. Every
  worker takes runs of every repository's changes: an installation's workers run the code of
  everyone who uploads to it, so give them nothing a change's author should not have (the chart
  mounts no database URL into them), and keep code you trust differently in an installation of
  its own.
- **Hermeticity is Bazel's.** A repository that pins its toolchains builds the same anywhere; one
  that uses the host's (a system compiler) builds with the worker's. ok checks what Bazel builds;
  it does not make a build hermetic.
- **Identity.** Workers run as their robot (`roles.worker`); the SCM admits them to read code.
  Its credential is the worker process's alone: out of sight of what Bazel runs, as above.
- **Logs** stream live into the logs' bucket, never the database. A worker sends what its run
  writes every second, or at once when a megabyte is waiting; each request ok automation takes is
  an object of its own, a segment, under the run's lease, and the run's row records how long the
  log is — moved past a segment only once the segment is written, so a reader that sees a length
  finds every segment before it. A request that wrote its segment and lost a race to its own
  resend leaves the worker's bytes there, which the next append takes as they are. When the run
  finishes, its segments are gathered into one object and deleted.
- **Interactive.** Rerun, cancel, and "why did this run": which changed file resolved to which
  workflow directory, or which dependency chain made the target affected.

The end-to-end suite (`internal/automation/e2e`) runs ok identity, the SCM, ok automation, and
workers with real Bazel through pre, the landing queue, and the workers' own behavior.
