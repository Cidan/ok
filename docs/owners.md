# OWNERS files

An `OWNERS` file in a directory names who may **approve** changes to the files beneath it. ok
enforces it on every ChangeList (CL): a CL can be submitted only when it has an **LGTM** and an
**Approval** from the owners of every file it changes ([changelists.md](changelists.md)). The
format follows Google's OWNERS files (as published by Chromium and Gerrit's find-owners), naming
ok's people and ACLs ([identity.md](identity.md)), with one deliberate difference in how owners are
found (nearest-only — see below).

## Syntax

One directive per line. `#` starts a comment anywhere on a line; blank lines are ignored.

```
# //services/api/OWNERS
user: ada                # a person, by handle (ada.human.example.com)
user: grace
acl: api-eng             # every member of api-eng.acl.example.com, through the sets it holds
*                        # anyone may approve

set noparent             # accepted for compatibility; nearest-only makes it implicit
set allowself            # the CL author's own LGTM/Approval count

per-file *.proto=user: lin, user: grace    # extra owners for matching files
per-file BUILD,*.yaml=*                      # anyone may approve these
per-file secrets/**=set noparent             # these files are owned ONLY by their per-file owners…
per-file secrets/**=file: //infra/SECURITY_OWNERS   # …imported from another owners file

file: //web/OWNERS       # import another file's directory-level owners
include ../OWNERS_shared # import everything: owners, per-file rules, set allowself
```

| Directive | Meaning |
|---|---|
| `user: <handle>` | A person. The handle is the name before `.human.<domain>`; `user: @<handle>`, as a hosted service writes it, is the same. |
| `acl: @<organization>/<team>` | Every member of an organization's team, `<team>.<organization>.team.<domain>`, and of the teams below it — read from ok identity when approval is evaluated, as an ACL's are. |
| `acl: <name>` | Every member of an ACL of the groups-and-ACLs tree, `<name>.acl.<domain>` ([identity.md](identity.md)), through the sets it holds — read from ok identity when approval is evaluated. A member who authors the change satisfies the set, as a listed user does. |
| `*` | Anyone may approve (but the author only with `set allowself`). |
| `set noparent` | Accepted for compatibility with Google-style files. Owners are never inherited from parent directories anyway (nearest-only), so it changes nothing. |
| `set allowself` | The CL author's own grants count: they may Approve where they are an eligible approver (e.g. under `*`), and their own LGTM counts when every file the CL changes is governed this way (or by no OWNERS). |
| `per-file <globs>=<owners>` | Owners added for files matching any glob (comma-separated). The right side is an owner list (`user:`/`acl:`/`*`, comma-separated), `set noparent` (matching files are owned **only** by their per-file owners), or `file: <path>`. |
| `file: <path>` | Import the target's directory-level owners (not its per-file rules or `set` options). |
| `include <path>` | Import everything from the target: owners, per-file rules, and `set allowself` (an included `set noparent` changes nothing, as above). |

**Paths** in `file:`/`include`: `//dir/OWNERS` (or `/dir/OWNERS`) is repo-root-relative; anything
else is relative to the importing file's directory. Import targets must be named `OWNERS`,
`<prefix>_OWNERS`, or `OWNERS_<ext>` — so imports only ever point at owner files.

**Globs** match the changed file's path relative to the governing OWNERS file's directory — also
for per-file rules pulled in by `include`. A glob without `/` matches at any depth (`*.go` matches
`a.go` and `sub/dir/a.go`); a glob with `/` matches the whole relative path. Wildcards: `*` (not
across `/`), `**` (across `/`), `?`, `[a-z]`, `{a,b}`. `**` is literally "any characters", so
`docs/**/*.md` needs at least one subdirectory — write `docs/{**/,}*.md` to also match `docs/a.md`.
Commas inside `{…}`/`[…]` don't split the glob list (`per-file *.{js,ts}=…` works).

## How owners are found — nearest-only

For each changed file, ok walks up from the file's directory and **stops at the first
`OWNERS` file** — that file (plus its per-file rules and imports) alone governs the file. Parent
OWNERS are never consulted. If no OWNERS exists anywhere up to and including the repo root, the
file is **unrestricted**: anyone — the author included — may LGTM and approve it. OWNERS are
opt-in: a repo (or subtree) behaves as if unowned until an OWNERS file governs it, so a lone
developer can land the first OWNERS file without a second reviewer.

This differs from Google, where a file's owners are the union of every OWNERS up the tree
(unless `set noparent`), making root owners global approvers. In ok, a change spanning several
directories needs an owner of each directory's nearest OWNERS.

- **Deleted** files are governed by their old path; **renamed** files need the owners of both
  the old and the new path.
- **OWNERS are read from the base branch head**, never from the CL — a change cannot make its
  own author an owner. A change *to* an OWNERS file needs that OWNERS file's current owners.

## Approval rules

- **LGTM** — anyone except the author. The author's own LGTM counts only if every changed file
  is governed by an OWNERS with `set allowself` or by no OWNERS at all.
- **Approval** — changed files are grouped by the owner set that governs them (the nearest
  OWNERS + matching per-file rules). **Each owner set needs one Approval** from one of its
  owners; one person can satisfy many sets. In the CL view, clicking **Approval** under Submit
  Requirements opens a "Who Can Approve" popover listing each owner set (its OWNERS, file count
  — hover for the files — and who approved or can approve). The Request Review dialog offers each
  unsatisfied owner set's owners, pre-selecting the fewest people who cover every set.
- **An author who is listed** (`user:`) in an owner set satisfies it implicitly. `*` does not make
  the author an implicit approver — they need `set allowself` to approve under `*`. Files no
  OWNERS governs are the exception: the author may approve them (see above).
- Grants persist across patchsets (Critique's model: a reviewer re-reviews by revoking), but
  requirements are recomputed for the current patchset: a new patchset touching another directory
  adds that directory's owner set.
- If an OWNERS file cannot be resolved (bad syntax, broken import), Approval **fails closed**
  and the "Who Can Approve" popover shows the error.

## Validation at upload

When a CL adds or edits an OWNERS-config file (`OWNERS`, `<prefix>_OWNERS`, or `OWNERS_<ext>`),
the SCM's `Upload` (what `jj cl` calls) validates it against the uploaded commit's tree and
**refuses the upload** — listing every problem with its line — for syntax errors,
broken/cyclic/misnamed imports, unknown users (a handle that names no person in ok identity's
directory), and `acl:` lines naming no ACL in ok identity's directory.
Because nothing above the nearest OWNERS can fill a gap, it also rejects an `OWNERS` file with no
directory-level owners (no `user:` line, `*`, or `file:`/`include` import that yields owners) and
a `per-file …=set noparent` rule with no owners for the same globs — either would leave files
unapprovable. Deleting an OWNERS file is not validated; it needs approval like any other change.

## Approvability

**No change should leave an OWNERS set its own people could not approve.** A change's LGTM must
come from someone other than its author unless every set governing it allows self, so a set whose
one person owns it alone, without `set allowself`, makes that person's own changes unapprovable —
in an installation of one, that is every change. `ok owners check` checks every `OWNERS` file in a
checkout against the groups-and-ACLs tree, and fails when any set (directory-level, or the
per-file rules sharing one set of globs, together):

- names no person — no `user:`, no ACL with a person in it (robots approve nothing), and no `*`;
- has exactly one person and no `set allowself`;
- is `*` without `set allowself` where fewer than two people are known (the people the tree names
  and the `user:` lines);
- names an `acl:` that the tree does not declare.

```bash
ok owners check -domain example.com .                       # the tree from .ok/repository.yaml
ok owners check -domain example.com -identity identity .    # or named
```

The failure names each set, its file, and the person it would lock out.

## Code

- `internal/owners` — the parser, glob matcher, nearest-only resolver, validator, and
  approvability check, shared by the SCM, which validates uploads with it, `ok owners check`, and ok
  control, which evaluates approval with it (resolving `acl:` owners' members with ok identity's
  `Directory.ExpandSet`, cached for a minute; an ACL identity cannot answer for fails approval
  closed).
- The approval evaluation (over OWNERS read through the SCM's `Browse` at `main`'s head) and
  reviewer suggestions are ok control's: `internal/control/review` (`policy.go`, `suggest.go`).
