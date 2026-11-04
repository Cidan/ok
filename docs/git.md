# git in ok workspaces

ok's jj answers git's commands. Run by the name `git`, the `jj` binary works an ok workspace with
git's commands — `git clone` makes a virtual workspace, `git commit` a change, `git push` uploads
ChangeLists, `git pull` syncs — and hands everything else, every repository that is not on ok, to
git itself. It is for people and agents whose hands type git: the commands, their options, their
output, and their exit statuses are git's, over ok's model. It is not git: there is no `.git`, no
git object, and no git hash, so tools that read a repository's files rather than run its commands
(an editor's git integration, libgit2) see no repository.

Related: [jj.md](jj.md) (ok's jj), [okfs.md](okfs.md) (virtual workspaces), [changelists.md](changelists.md)
(what a push uploads).

## Using it

```bash
ln -s "$(command -v jj)" ~/.local/bin/git     # ahead of git on the PATH
git clone app ~/src/app                        # a virtual workspace, as jj clone makes
cd ~/src/app
git checkout -b rate-limit
…edit…
git commit -am 'feat: rate limiter'            # nothing to add: every file is tracked
git push                                       # one ChangeList per commit
git push -o r=grace -o ready                   # a new patchset, sent for review to grace
git pull                                       # jj sync, once it lands
```

`git` must come before git's own on the PATH. Outside an ok workspace, it runs the next `git` on the
PATH that is not itself, or the one `OK_REAL_GIT` names. `git help ok` says all of this in short.

## The model

| git | in an ok workspace |
|---|---|
| HEAD | the working-copy change's parent (jj's `@-`): the last commit |
| the index and the working tree | one and the same: the working-copy change (`@`). ok tracks every file the ignore files (`.ignore`, `.gitignore`) do not name, so `git add` has nothing to do — it says so — and `git commit` commits every change, or the paths it names. `git diff`, `git diff --cached`, and `git diff HEAD` all show the working change; `git status` lists it as staged |
| a branch | a local bookmark, which this workspace alone sees (work moves between workspaces only as ChangeLists) |
| `main` | the branch every repository has. While there is no local `main`, `main` is `origin/main`; a commit on it makes one, ahead of `origin/main`, as a commit on git's local main is |
| `origin`, `origin/main` | ok, and its main (`main@ok`); every branch's upstream |
| `origin/cl/<n>` | someone's ChangeList, brought in by `git fetch origin cl/<n>` (`jj cl get`) |
| a commit ID | jj's: BLAKE3-256, 64 hex digits, abbreviated to at least 7 |
| a push | HEAD (or each refspec's source) and its unlanded ancestors uploaded as ChangeLists, one per commit (`jj cl`); a refspec's destination is ignored but for Gerrit's `%r=…,ready,wip` |
| a pull | `jj sync`: main brought up to date, landed commits retired, the rest rebased onto main. ok rebases always: a ChangeList is one change on one parent |
| a fetch | `origin/main` brought up to date, nothing of yours moved |
| a conflict | kept in the commit that has it. No command stops halfway: `merge`, `rebase`, `cherry-pick`, `revert`, and `stash pop` finish, name the conflicted paths, and exit 1. A `git commit` while HEAD has conflicts (or `--continue`) folds the working tree's resolution into HEAD; `--abort` undoes the command's operation |
| a stash | a commit on the HEAD it was made at, marked `Git-Stash: yes`, which nothing builds on: in ok like any change, so on every machine the workspace attaches to |
| a tag | a local name for a commit, as a branch is; an annotation is not kept |
| the reflog | none: `jj op log` lists every operation, and `jj undo` undoes the last |

Which branch HEAD is on is the bookmark at HEAD; when several are, the one last checked out, which
the workspace notes in `.jj/git-HEAD` beside git's `checkout -` (a missing or stale note is rebuilt
from the bookmarks, so a workspace attached on another machine still has a HEAD).

## The commands

| Command | Does |
|---|---|
| `clone` | `jj clone` of a repository named, or of a URL on the ok this machine signed in to (`--real`, `--workspace`, `--attach` as `jj clone` has them); any other source is git's |
| `status` | long, `-s`, `--porcelain` (v1 and v2), `-b`, `-z`, `-v`, pathspecs, ahead and behind `origin/main` |
| `diff` | the working change, `<rev>`, `<a> <b>`, `a..b`, `a...b`, `--merge-base`; `--stat`, `--numstat`, `--shortstat`, `--name-only`, `--name-status`, `--summary`, `--color-words`, `-U`, `-w`, `-b`, `-R`, `--diff-filter`, `--relative`, `--exit-code`, `--quiet`; also `diff-index`, `diff-tree`, `diff-files` |
| `log`, `show`, `rev-list`, `shortlog`, `whatchanged` | git's formats (`--oneline`, `--format`/`--pretty` with its placeholders, `medium`, `fuller`, `raw`, …), `--graph` (jj's renderer), `--decorate`, `-n`, `--skip`, `--author`, `--grep`, `--since`, `--until`, `--first-parent`, `--merges`, `--all`, `--reverse`, `--date`, diffs, pathspecs; `show <rev>:<path>` |
| `add` | nothing to do; `-f` tracks ignored files it names (`jj file track --include-ignored`), `--chmod` sets the executable bit |
| `rm`, `mv` | files removed or renamed in the working tree; `rm --cached` untracks (`jj file untrack`) |
| `restore`, `checkout -- <paths>` | paths as HEAD (or `--source`) has them; `--staged` alone has nothing to unstage |
| `commit` | `-m`, `-F`, `-a`, `--amend`, `--no-edit`, `-e`, `--allow-empty`, `--author`, `--date`, `--reset-author`, `-s`, `--trailer`, `-C`, `-c`, `--fixup`, `--squash`, paths; the editor shows git's comment lines |
| `reset` | `--soft`, `--mixed` (one here), `--hard`, `--keep`; HEAD and its branch move, commits left behind that nothing keeps are abandoned |
| `checkout`, `switch` | branches, `-b`/`-B`/`-c`/`-C`, `--detach`, `-` (the last place), `-f` (dropping the working change), `-m` (keeping conflicts); the working change goes along, refused where it would conflict |
| `branch`, `tag` | list (`-a`, `-r`, `-v`, `-vv`, `--contains`, `--merged`, `--format`, patterns), create, `-d`/`-D`, `-m`/`-M`, `-c`, `--show-current` |
| `stash` | `push` (`-m`, paths, `-k`), `list`, `show`, `pop`, `apply`, `drop`, `clear`, `branch` |
| `merge` | a fast-forward, or a merge commit (`--no-ff`, `--ff-only`, `--squash`, `--no-commit`, `-m`); it says that ok uploads no merge commits |
| `rebase` | `[<upstream>] [<branch>]`, `--onto`, `--keep-base`, `--root`, `--abort`, `--continue` |
| `cherry-pick`, `revert` | commits and ranges, `-n`, `-x`, `-m`, `-e`, `--abort`, `--continue` |
| `push`, `pull`, `fetch`, `remote` | as the model says; `push --dry-run`, `-o r=<handle>`, `-o ready`, `-o wip` (also `--reviewer`, `--ready`, `--wip`), `--all`; `pull --upstream` in a fork |
| `rev-parse` | revisions in git's syntax (`HEAD~2^2`, `@{u}`, `<rev>:<path>`, `<rev>^{tree}`, ranges), `--short`, `--verify`, `--abbrev-ref`, `--symbolic-full-name`, `--show-toplevel`, `--show-prefix`, `--show-cdup`, `--git-dir` (the workspace's `.jj`), `--is-inside-work-tree` |
| `show-ref`, `for-each-ref`, `symbolic-ref`, `merge-base`, `describe`, `name-rev` | over branches, `origin/…`, and tags |
| `ls-files`, `ls-tree`, `cat-file` (`-t`, `-s`, `-e`, `-p`, `--batch`, `--batch-check`), `blame` (`-L`, `-s`, `-e`, `--porcelain`, `--line-porcelain`), `grep`, `check-ignore` | over the working copy's and commits' files; a commit as `cat-file -p` shows one carries jj's `change-id` header |
| `config` | answers `user.name`, `user.email`, `remote.origin.url`, and each branch's upstream; writing `user.name` or `user.email` writes jj's configuration (`--global`: the user's); other keys are not kept |
| `clean` | nothing is untracked, so only `-x` and `-X` find ignored files (and, with `-d`, directories) that nothing tracks; redirections are left alone |
| `update-index`, `submodule`, `worktree`, `gc`, `fsck`, … | nothing to do (`--chmod` sets the executable bit; `worktree list` lists this workspace) |
| `init`, `version`, `hash-object`, `apply`, `diff --no-index`, `credential`, … | git's, in an ok workspace too (`apply --index` applies to the working tree, which is the index) |

Refused, saying what to use instead: interactive rebases and `-p`/`--patch` everywhere (jj's
`squash`, `split -i`, `restore -i`), `reflog` (`jj op log`), `bisect` (`jj bisect run`), `am` and
`format-patch` (ChangeLists), a second remote, `worktree add` (`git clone` makes another workspace),
orphan branches, `submodule add`, and git's object-writing plumbing (`update-ref`, `commit-tree`,
…). An option git has and this does not is a usage error (exit 129), as git's are.

## How it runs

- **The front end** (`git/front.rs`) runs first, in `main`, when the process was started by the
  name `git`. It reads git's global options (`-C`, `-c`, `--no-pager`, `--git-dir`, …) and finds
  the nearest `.git` or `.jj` above the directory: an ok workspace's `.jj` (its repository's store
  is `ok`) makes the command ok's; a `.git` (a colocated jj repository's too), a jj repository of
  another backend, or nothing makes it git's. A clone is ok's to decide, once it can read ok's
  configuration. `--git-dir`, `--work-tree`, `GIT_DIR`, and options only git answers
  (`--version`, `--exec-path`, …) are git's.
- **ok's commands run as jj's.** jj's runner reads its arguments from the process, so the front
  end executes the binary again as `jj --quiet git-compat -- <git's arguments>` (the hidden
  subcommand, `git.rs`): jj's configuration, workspace loading, snapshots, and transactions are
  every git command's. `--quiet` keeps jj's reports of the working copy moving out of git's
  output (a clone keeps them). `GIT_PAGER`, `GIT_EDITOR`, `core.editor`, `color.ui`, and a
  command's `--color` become jj's pager, editor, and colors.
- **Exit statuses are git's**: 1 for a difference (`diff --exit-code`), a key not set (`config`),
  a commit with nothing to commit; 128 for a fatal error; 129 for a usage error. jj's runner knows
  only its own, so the command records its status and `main` exits with it.
- **git's commands are each one jj transaction**, described as the command (`git commit`,
  `git rebase main`), so `jj op log` reads as what was typed and `--abort` finds what to undo.
- **Handing to git.** git runs by `exec`, with `OK_GIT_RAN` naming it: a front end that finds itself
  started by that path was taken for git, and says so. In an ok workspace (`apply`, `diff
  --no-index`), `GIT_CEILING_DIRECTORIES` keeps git from finding a repository around it.

## Layout

All of it is in the `jj` binary's crate (`jj/cli`):

| File | What it is |
|---|---|
| `git.rs` | The hidden `jj git-compat` subcommand: dispatch, exit statuses, and what every command shares |
| `git/front.rs` | The front end: the name `git`, git's global options, ok's or git's, and handing to git |
| `git/repo.rs` | The workspace as git sees it: HEAD and its branch, git's revision syntax, pathspecs |
| `git/fmt.rs` | git's output: dates, `--pretty` formats and `--format` placeholders, quoted paths |
| `git/inspect.rs` | What reads: `status`, the diffs, `log`, `show`, `rev-list`, `blame`, `grep`, `ls-files`, `ls-tree`, `cat-file`, `check-ignore` |
| `git/change.rs` | What changes the working tree and moves HEAD: `add`, `rm`, `mv`, `restore`, `commit`, `reset`, `checkout`, `switch`, `stash`, `clean` |
| `git/history.rs` | `merge`, `rebase`, `cherry-pick`, `revert` |
| `git/refs.rs` | `rev-parse`, `branch`, `tag`, the ref plumbing, `describe`, `config`, and the rest |
| `git/remote.rs` | `clone`, `push`, `pull`, `fetch`, `remote`, over `jj clone`, `jj cl`, and `jj sync` |

## Tests

The unit tests (`cargo test -p ok-jj --bin jj`) cover the formats, pathspecs, messages, and every
command's options (clap's own checks). The end-to-end scenario (`git_workflow`, in
`cli/tests/common`) runs as `git` — the binary by that name, a stand-in for git beside it — against a
real `ok scm`, in real checkouts (`cli.rs`) and virtual workspaces (`cli_virtual.rs`): git's
everywhere else, a clone, commits with nothing to add, branches, diffs, a stash, a restore and a
reset, pushes that upload CLs, a pull after one lands, a CL fetched, cherry-picked, and reverted, a
rebase's conflict and its abort, and the refusals.
