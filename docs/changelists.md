# ChangeLists (CLs)

ok does not do the branch-and-merge pull request. It does the **Gerrit / Perforce changelist**:
the open-source-shaped version of Google's Piper + CitC + Critique. This document is the product:
what a CL is, its lifecycle, review and approval, submit, and comments. The SCM (`internal/scm`)
holds CLs' identity, patchsets, lifecycle, and landing; ok control (`internal/control`) holds
review and the pages.

Pairs with [scm.md](scm.md) (the SCM: upload, landing, and the change feed, in detail),
[owners.md](owners.md) (OWNERS), [identity.md](identity.md) (the groups-and-ACLs tree),
[jj.md](jj.md) (the client), and [web.md](web.md) (the pages).

## Why CLs, not PRs

A **PR is a branch + a merge**: the unit is a long-lived branch, its history is whatever commits
you pushed, and merging leaves a merge commit (or a squash) plus a dead branch. A **CL is a change
against `main`, with a stable identity across revisions**: you develop off `main`, you iterate by
*replacing* the proposed content (patchsets), not by appending commits, and submitting lands the
final patchset on `main` as one commit — linear history, no branch, no merge commit. The stable
identity and its patchsets are the whole point: they make diff, review, and checks coherent.

Naming is end to end: **`cl`** everywhere (routes `/cls` and `/cl/<n>`, the `jj cl` command, the
`changelist` tables). No "PR" in the code, database, or UI.

## Decisions (locked)

1. **Review model:** Gerrit-style named reviewers, submit requirements, inline comments, and
   threads — but **binary labels, not scores**. Each label (**LGTM**, **Approval**) is a checkbox a
   reviewer grants or revokes. No ±1/±2 and no blocking negative votes. **LGTM** comes from anyone
   but the author; **Approval** comes from the **OWNERS** governing each changed file (nearest
   OWNERS only, read from `main`'s head; one Approval per owner set) — see [owners.md](owners.md)
   — except the files of a repository's groups-and-ACLs tree (the folder its
   `.ok/repository.yaml` names as `identity:`), which the ACLs owning what the change changes
   approve instead ([identity.md](identity.md), "Groups and ACLs").
   `set allowself` in OWNERS lets the author's own grants count, LGTM included, and so does a file no
   OWNERS governs: OWNERS are opt-in, so in a repository with none, an author may LGTM and approve
   their own change — add a root OWNERS (without `set allowself`) to require someone else. As in
   Critique, a grant stays in
   force across later patchsets (a reviewer re-reviews by revoking), and a CL is approved only with
   an LGTM, every owner set's Approval, and no unresolved comments.
2. **Client:** ok's **`jj`** only ([jj.md](jj.md)). It is the only way code enters ok: `jj cl`
   uploads, `jj cl get` fetches someone else's CL, `jj sync` follows `main`.
3. **Identity:** a CL is one jj change — its **change ID** — in one repository. Re-uploading the
   change is a new patchset of the same CL; there is no `Change-Id` trailer and nothing in the
   commit message.
4. **Stacking:** a chain of changes uploads as a stack of CLs, each linked to its parent's CL.
5. **Submit:** a CL lands on `main` as **one commit**: the reviewed commit itself when it sits on
   `main`'s head, else its change spliced onto the head path by path ([scm.md](scm.md),
   "Submit: path-level landing"). A stack lands bottom-up, one CL at a time or all at once.
6. **Lifecycle:** Critique's — a new CL is a **draft** (visible to everyone, nobody asked to act)
   until its author **requests review**; **Abandon** closes a CL without deleting anything and
   **Restore** reopens it; only the author (or a repository admin) submits. See
   [Lifecycle](#lifecycle).
7. **Actions config:** automation is declared in the repository tree, never in a per-repository
   UI ([automation.md](automation.md)).

## The client story (jj)

```
jj clone app ~/src/app         # a workspace; the operation log lives on ok
cd ~/src/app
jj new 'trunk()' -m 'add rate limiter'
…edit…
jj cl                          # upload @ (and its mutable ancestors) as CLs; prints their URLs
jj cl --ready --reviewer bob   # …straight to review, with a reviewer
jj sync                        # follow main: retire landed changes, rebase the rest onto main
```

`jj cl` uploads the current change (`@`, or `@-` when `@` has no description) with its mutable
ancestors, creating or updating one CL per change; `jj cl -r <revset>` uploads others instead. An
upload that changes nothing is refused ("no new changes"); a change that is already its CL's
current patchset is left alone. `--ready` sends drafts for review and `--wip` returns open CLs to
draft (the author's or an admin's to use); each `--reviewer` — a handle or `<handle>@<email domain>` — is
assigned to every CL the upload creates or updates but one they wrote. What the SCM refuses at
upload — a merge commit, an unresolved conflict, an empty change or description, a parent that is
neither on `main` nor a CL, a stale stack, someone else's CL, a closed CL, a divergent change,
invalid OWNERS, an unknown reviewer — is listed in [scm.md](scm.md) ("Upload").

## From the command line

A CL's whole lifecycle — where it stands, review, submit, landing, and its deployments — is
`jj cl`'s, for a person or an agent, as well as the CL's page's. Every subcommand names CLs by
number or by revset (default: the change `jj cl` would upload) and works in a workspace of the
repository:

```
jj cl status                     # everything about the CL: review, pre, post, deployments, failure
jj cl status -f                  # follow it: a line each time it moves, until it is settled
jj cl status -f --until deployed # …or until a milestone: pre, submittable, landed, deployed
jj cl status --json              # ok control's JSON (its REST form); with -f, one object per line
jj cl ready --reviewer bob -m 'please look'   # send a draft for review
jj cl draft                      # take it out of review
jj cl reviewers --add cy --remove bob
jj cl review 87 --lgtm --approve -m 'ship it' # grant (or --no-lgtm/--no-approve revoke)
jj cl comment -m 'nit' --file lib/x.go --line 12     # a thread (on the CL without --file)
jj cl comment -m 'done' --reply 31                   # a reply
jj cl resolve 31                 # resolve a thread (--reopen to reopen it)
jj cl submit -f                  # submit, then follow it until deployed (--stack for its ancestors)
jj cl submit --cancel            # stop its submission
jj cl rerun                      # rerun every failed check of its pre (or name runs)
jj cl pre                        # run a draft's pre by hand, or a CL's under its stack's top
jj cl cancel 5012                # cancel runs
jj cl log 5012 -f                # a run's log (--tail BYTES for its end)
jj cl deployment retry 412       # retry, halt, or allow a deployment of a landed CL
jj cl abandon -m 'not now'; jj cl restore
jj cl rollback 87 -m 'broke prod'    # a new CL undoing a landed one
```

**Where a CL stands** is one value, its **stage**, derived the same way wherever ok shows it
(ok control's `internal/api`, `derive`):

| Stage | Means | Waits on |
|---|---|---|
| draft | not sent for review: its pre runs once it is (or by hand, `jj cl pre`) | its author |
| pre running | its current patchset's pre is being planned or run | ok |
| pre failed | | its author |
| in review | pre passed; no LGTM, an owner set's Approval, or an unresolved thread | reviewers (and its author, for threads) |
| submittable | approved and pre passed (`blocked` says what still stands in the way, a conflict with main say) | its author |
| submitted | in the landing queue: post being planned or run | ok |
| submit failed | its submission failed: post failed, or the SCM refused it (a conflict with `main`) | its author |
| landed | on main; the deployer has yet to plan its deployments | ok |
| deploying | each deploy's latest deployment covering its commit is under way | ok, or a repository admin for a migration's hazards |
| deployed | every one succeeded, or there were none | nobody |
| deploy failed | one failed or was halted | its author |
| abandoned | | nobody |

Pre runs at a stack's top in review, over every CL of it (docs/automation.md, "pre"): a CL under
the top (`pre_at`, the top's number) is in review or submittable on its review alone, and submits
with its stack, from the top (`jj cl submit --stack`).

**Following** (`-f`) prints a line whenever a CL's stage, summary, or who it waits on changes, and
stops once it is **settled**: deployed, failed, or waiting on a person — so an agent never hangs on
a review nobody is giving. `--until` stops at a milestone instead (or sooner, at a failure or a
person). A failure prints what failed, its link, and its log's last lines. The exit code says how
it ended:

| Code | Means |
|---|---|
| 0 | reached: deployed, or the `--until` milestone |
| 1 | something failed (pre, post, a deployment), or the CL was abandoned |
| 2 | it waits on a person — its author, a reviewer, or an admin — short of the milestone |
| 3 | `--timeout` passed |
| 4 | ok could not be asked about it: no such CL, a change with no CL, ok unreachable |

After landing one step of a change whose next step depends on it being deployed, run `jj cl status
-f --until deployed` before submitting the next.

**The service behind it** is ok control's `ok.control.v1.ChangeLists`
(`proto/ok/control/v1/control.proto`): `Status`, `Watch` (a stream: the status at once, then each
change, until the CL is finished or an hour passes), and every action above. Any person may call
it, and ok control decides who may do what exactly as on the CL's page: the author and repository
admins drive the lifecycle and rerun or cancel checks; anyone reviews and comments; a
deployment's retry and halt are its CL author's or an admin's, and allowing hazards an admin's.
The same server answers it as JSON — `POST https://ok.example.com/ok.control.v1.ChangeLists/<Method>`
with the request as JSON, a stream answering one `{"result": …}` per line, the caller proving
themselves with their credential as over gRPC — which is what `--json` prints, so a script reads
the same objects ok control's pages are built from.

A request from a browser on another site (a foreign `Origin`, or `Sec-Fetch-Site` other than
`same-origin` or `none`) is refused, as the CL page's own forms refuse it.

## Working with multiple CLs (the jj mental model)

The unit of work is a **change** (one commit with a stable jj change ID → one CL), **not a
branch**. You don't check out branches; you `jj edit <change>` to point the working copy at a
different change. Each CL is a different change; there are no branches to manage and no
staging/stash (the working copy *is* a commit, amended in place as you edit).

```
$ jj log
@  zxcvbnma  wire limiter into gateway     # you are here (a CL)
○  qpvuntsm  add rate limiter             (another CL)
│ ○  mlkjhgfd  fix README typo            (a third CL, off main)
├─╯
◆  main@ok
$ jj edit qpv     # work on "add rate limiter" — edits now apply to THAT change/CL
```

Two shapes, differing only in what a new change is based on:

- **Independent CLs** (parallel, unrelated) — each forks from `main`: `jj new 'trunk()' -m A`;
  `jj new 'trunk()' -m B`.
- **Stacked CLs** (a dependent chain) — each on top of the last: `jj new 'trunk()' -m A`;
  `jj new -m B`. B's CL names A's as its parent.

Server side a CL is named by its number (`ok.example.com/<owner>/<repo>/cl/2`); locally by its change ID or
description — the same object, two names. **Other people's CLs:** `jj cl get <n>` brings a CL's
current patchset into your view as the immutable bookmark `cl/<n>@ok`; you can test it or stack
your own changes on it, and `jj sync` follows it through new patchsets and its landing.

## Where a CL lives

| The SCM (the write path) | ok control (review) |
|---|---|
| identity: repository, number, change ID | reviewers |
| the author (who first uploaded the change) | LGTM and Approval grants |
| status (`draft`, `open`, `submitted`, `abandoned`) and every transition | comments, threads, resolution, and porting across patchsets |
| patchsets: commit, base, uploader | the `approved` projection and the live approval evaluation |
| the stack parent | suggested reviewers |
| landing: the landed commit, the submitter, and `main` | the change log, the Checks tab, and automation |

ok control drives transitions and landing through the SCM on behalf of the person it
authorized; the SCM records that person as the actor. Every rule an upload or a landing must
enforce atomically lives in the SCM's database; everything else is control's. Control follows the
SCM's change feed to keep its projection of each CL current ([scm.md](scm.md), "Events").

## Data model

ok control's tables (`internal/control/db/migrations`), in its own schema:

- **`changelist`** — the projection of the SCM's CL (status, author, stack parent, current
  patchset, description, landing) plus **`approved`**, the review evaluation's stored projection:
  lists and filters read it; nothing trusts it (submit and the CL page evaluate live).
- **`reviewer`** — who reviews a CL: assigned by Request Review, Add Reviewers, or `jj cl
  --reviewer`, or joined by granting a label; never the author.
- **`label_grant`** — one row per CL × reviewer × label: its current state and the patchset it was
  granted or revoked on. The history is in `event`.
- **`comment`** — CL-level (no file), file-level (a file, no line), or inline (a file, a line, and a
  side of the diff: base or patchset), threaded by parent, resolvable per thread (the root carries
  resolution), recording the patchset it was written on. Porting is computed at render time, so a
  row never changes.
- **`event`** — the CL's history: uploads, status changes (with an abandon reason), reviewer
  changes, grants and revocations, the landing, and approval gained or lost (actor empty = ok itself).
  The Change Log reads it, and it is the feed a notifier will read.

## Checks

A CL shows its checks on its **Checks** tab — never a separate menu — read live from ok
automation ([automation.md](automation.md)): the viewed patchset's **pre** and its verdict (Passed,
Failed, Running, or Not Planned), then the CL's **post**, from its latest submission (see Submit).
Each panel groups its runs by kind — Plan, Build, Test, Lint, Workflow Jobs, and the Unaffected
Workflow Jobs pre selected but left unrun because their targets are not affected; Post adds its
Attempts. A group's header tallies where its runs stand ("2 Running · 12 Passed") beside the
worst status, and opens to the latest run of each job — status, the job (a test by its target)
and the workflow file that defines it (or Built In), why it runs, why it failed, and how long it
took. A group with a failure starts open. While pre or a submission runs, the tab refreshes
itself, and each group stays open or closed as the reader left it. Each run has a page:
what it runs and why, where it stands, and its log, which grows while the run goes on (Raw has all
of it). The CL's author or a repository admin cancels a run that is queued or running, and reruns
one that finished, of an active CL's current patchset; anyone else sees those buttons disabled,
with the reason. A post run is its submission's: it is stopped by canceling the submission, and
run again by submitting again. ok automation being unreachable shows as the tab's own state, never breaks the
page.

## Submit

Only an **open** CL is submitted — a draft is refused with "request review first" — and only by its
author or a repository admin (Critique: the author commits the approved change). The gates, in
order, are one function (`review.Service.Blocked`, `internal/control/review`) shared by
Submit and the Submit button's disabled tooltip, so the button can't disagree with submit:

1. the CL is open and not already submitting, and the person may submit it;
2. the review, evaluated live against OWNERS at `main`'s head: an LGTM, every owner set's Approval,
   and no unresolved comments;
3. **pre** passed on the CL's current patchset ([automation.md](automation.md)): its plan and the
   latest run of every job it selected passed. Until then Submit says why — the checks haven't run
   yet, are still running, or which job failed, was canceled, or could not run — and ok
   automation being unreachable blocks it too. The Submit Requirements panel shows it as its
   **Pre** row;
4. a stacked CL's parent already submitted (an abandoned parent says to drop it locally and
   upload again);
5. a clean landing on `main`'s head — the SCM's own rule, checked by a **dry run**: a path the CL
   changed that `main` also changed behind it is a conflict (`jj sync`, resolve, and upload a new
   patchset), and a change `main` already has changes nothing.

Submit then hands the CL to ok automation's **landing queue** ([automation.md](automation.md),
"The landing queue"), which prepares its candidate — the exact commit that would land — runs
**post** on it, and lands it once post passes; `main` never moves otherwise. Meanwhile the CL is
**Submitting**: a chip beside its status, Submit disabled with what the queue waits for (testing,
on top of which CLs, waiting for which to land), and **Cancel Submit** for its author or a
repository admin. The Checks tab's **Post** panel shows the submission, its post runs, and its
**Attempts** at landing it — each on the `main` commit it was prepared on, with
the CLs it speculated would land first, and why the queue moved on from it. When it lands, the CL
records the landed commit and its submitter; the change log shows "‹submitter› sent it to the
landing queue" and "‹submitter› submitted to main as ‹id›". When post fails or the SCM refuses
the landing, nothing lands: the CL stays open, the Post panel and the change log ("ok could not
land it") say why, and it can be submitted again. A new patchset, abandoning the CL, or returning
it to draft cancels its submission.

**Submit Stack.** A CL stacked on unsubmitted CLs also offers **Submit Stack (n)**, which submits it
with every unsubmitted ancestor, bottom first, as one submission — each CL through the same gates,
its own pre included — which lands them in one landing, each CL's post passed on its own
candidate. The plain Submit on such a CL says to submit the parent first, or the stack; while the
parent is submitting, to submit this once it lands.

A landing that edits an OWNERS file, the groups-and-ACLs tree, or `.ok/repository.yaml` (which says
where that tree is) re-derives the approval of every CL still active in the repository, since every
evaluation reads them from `main`.

## Lifecycle

A CL's lifecycle follows Critique (*Software Engineering at Google*, ch. 19): the author uploads a
snapshot, **mails** it to reviewers, reviewers comment and grant, and the author commits once it is
approved. The status is set only by explicit actions; approval is derived.

| Action | From → to | Who |
|---|---|---|
| Upload a new change (`jj cl`) | new → `draft` (`open` with `--ready`) | the uploader, who becomes the author |
| **Request Review** | `draft` → `open` | author, repository admin |
| **Return to Draft** | `open` → `draft` | author, repository admin |
| **Submit** / **Submit Stack** | `open` → `submitted` | author, repository admin |
| **Abandon** (optional reason) | `draft`/`open` → `abandoned` | author, repository admin |
| **Restore** | `abandoned` → the status it was abandoned from | author, repository admin |
| **Roll Back** (a reason) | a `submitted` CL stays submitted; a new `open` rollback CL undoes it | anyone |

A **repository admin** is the repository's creator, or one of ok's administrators, as ok identity
says: a member of `admins.acl.<domain>`, or, before any groups-and-ACLs tree is applied, one of
identity's `bootstrap_admins` ([identity.md](identity.md)). Each transition takes the CL's row lock in the SCM, re-reads its status,
and refuses a CL no longer in the transition's from-set, so it is ordered against any concurrent
upload, landing, or transition; every one is in the SCM's change feed and control's `event`.

**Roll Back** makes a new CL, the viewer's, that undoes a submitted CL and those landed above it in
its stack, reviewed expedited: an LGTM from anyone but its author, and no OWNERS Approval for the
inverse change ([automation.md](automation.md), "Rollbacks"). The rolled-back CL names its
rollbacks; while one is open it offers that instead of Roll Back.

**What each state permits.**

| | `draft` | `open` | `submitted` | `abandoned` |
|---|---|---|---|---|
| New patchset (`jj cl`) | yes | yes | refused | refused (restore first) |
| Label grants (Reply) | yes | yes | refused | refused |
| Comments and threads | yes | yes | yes | yes |
| Reviewer changes | yes | yes | no | no |
| Submit | no | if approved and the other gates pass | — | — |

**Drafts are visible to everyone,** like Gerrit's work-in-progress changes: quiet, not private. The
header offers the author Request Review instead of leading with Reply; Submit is shown greyed to
everyone ("request review first") — never hidden, so the path to landing is always discoverable.

**Approval** is Critique's scoring: an LGTM from anyone but the author, an Approval from every
governing owner set, and **no unresolved comment threads** (from any patchset). Grants stay in
force across patchsets. `approved` is recomputed wherever an input changes — a grant or
revocation, a new patchset (its changed files pick the owner sets), a thread opening, resolving,
or being deleted, a reviewer's removal (which revokes their grants), Request Review, Restore, and a
landing that edits OWNERS — and each change is recorded in the change log. An open CL whose
projection is set shows as **Approved**; a draft keeps its Draft chip.

**Reviewers.** Request Review (the header dialog on a draft) and Add Reviewers (Change Info's
Reviewers row on an open CL) share one dialog, following Critique's "smallest set of reviewers
needed … prioritizing reviewers that have recently edited and/or reviewed the included files":

- one row per owner set still needing Approval, listing its `user:` owners whose Approval would
  count; the fewest people covering every set are pre-checked (a greedy set cover, ties broken by
  recent activity on the changed paths, then name), and a set an assigned reviewer already covers
  needs nobody new;
- **Recent Reviewers and Editors** — for the recent commits on `main` touching the changed paths'
  directories, the reviewers who granted on the CL that landed each one, then its author;
- **Anyone Else** — a live search of the people in ok identity's directory;
- an optional message, posted as a CL-level comment.

The author, an admin, and the CL's reviewers can add reviewers (a reviewer may hand a review on);
the author and an admin can remove anyone, and a reviewer can remove themself. The author is never a
reviewer. Granting a label makes the granter a reviewer.

**Abandoned CLs in checkouts.** Abandon deletes nothing: the CL's patchsets stay, so Restore is a
status change and its diffs keep rendering. What lingers is the author's own jj change: the next
`jj cl` of it is refused ("CL n is abandoned: restore it first, or drop the change with jj
abandon"), and the abandoned CL's page shows the exact command, `jj abandon <change id>`, to copy.

**The finder** (`/<owner>/<repo>/cls?f=`) has views: **Open** (the default — open CLs plus the viewer's own
drafts), **Drafts** (everyone's), **Needs My Review** (open CLs the viewer reviews and hasn't
granted on), **Merged**, **Abandoned**, and **All**.

**Notifications:** ok sends none. Every event a notifier needs is in the change log; Critique's
attention set ("whose turn") and CC (notified, not reviewing) would come with notifications.

## Ported comments

Inline threads follow the code across patchsets, Gerrit-style (`internal/control/review/port.go`,
placed on the page by `internal/control/web/diff.go`). A thread is anchored
to a line on one side of the diff it was written on: **Patchset** (right) is a line of the
patchset's file, **Base** (left) a line of its base's file. Viewing patchset N:

- A thread written on N renders at its line.
- An **unresolved** thread written on an earlier patchset M is **ported** onto N: a Patchset-side
  line is followed through the diff between M's and N's commits, a Base-side line through the diff
  between their bases (no change unless the CL was rebased). Renames are followed with the diff
  view's rename detection, so a thread moves with its file. Each pair of trees is diffed once per
  page (the SCM's `DiffTrees`, then a zero-context `DiffFiles` over just the files with threads); a
  failed diff puts its threads at file level instead of failing the page.
- Where the line itself changed: a modified line maps to the line at the same offset into its
  replacement (clamped to the replacement's last line); a removed line maps to the line just above
  the removal (the first line if the removal started at the top), so the thread sits where the line
  was. Either way the thread is marked **Line Changed Since Patchset M**. A file-level thread follows
  only the path; a binary change loses the line (file level).
- Resolved threads from earlier patchsets, and threads from later ones, aren't ported: they are
  **outdated** and show only on the Comments tab.

Where a thread renders on N's Files tab: at its row; when the diff's hunks don't show that line,
with the three unchanged lines either side of it spliced in (Gerrit never hides a commented line);
at the top of its file when no line can be shown (a deleted side, a binary / too-large / generated
file, a line past the end of the file); and for a file N's diff doesn't show, in an extra file row
after the changed files — **Unmodified** (showing the lines around its threads), **Removed** (the
file no longer exists; its threads are marked **File Deleted Since Patchset M**), or, when the diff
was cut short before the file, its change with the threads at file level. A ported thread's header
links to the patchset it came from; on the Comments tab every thread names and links its patchset.
Replying to or resolving a ported thread is the same as on its own patchset — the thread stays
recorded against M. A new comment is recorded against the patchset being viewed.

## Open questions

- What happens to descendants when a parent CL is amended beyond `jj sync`'s rebase.
- Auto-abandon of inactive CLs (Gerrit's `changeCleanup.abandonAfter`).
- `jj sync` retires landed changes but not abandoned ones; dropping those automatically would need
  the client to ask the SCM which of its changes' CLs are abandoned.
