//! git's commands that change the working tree and move HEAD: `add` (nothing to do: ok tracks
//! every file), `rm`, `mv`, `restore`, `commit`, `reset`, `checkout`, `switch`, `stash`, and
//! `clean`; and what they share — carrying the working change to another commit, moving the
//! branch HEAD is on, and saying what a commit made.

use std::collections::HashSet;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;

use futures::TryStreamExt as _;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::internal_error;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::backend::Signature;
use jj_lib::backend::Timestamp;
use jj_lib::commit::Commit;
use jj_lib::copies::CopyRecords;
use jj_lib::matchers::EverythingMatcher;
use jj_lib::matchers::Matcher;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RefTarget;
use jj_lib::ref_name::RefName;
use jj_lib::repo::MutableRepo;
use jj_lib::repo::Repo;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::revset::ResolvedRevsetExpression;
use jj_lib::revset::RevsetExpression;
use jj_lib::rewrite::restore_tree;

use super::Code;
use super::Cx;
use super::error;
use super::fatal;
use super::fmt;
use super::hint;
use super::inspect;
use super::parse_or_return;
use super::repo;
use super::repo::Head;
use super::rev_error;

// ---------------------------------------------------------------------------------------------
// What the commands share.

/// How the working change goes along when HEAD moves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Carry {
    /// Its changes go along; a move they would conflict with is refused.
    Keep,
    /// Its changes go along, conflicts and all (`checkout -m`).
    Merge,
    /// Its changes are dropped (`checkout -f`, `reset --hard`).
    Discard,
}

/// The working change moved onto a commit, or why not: the paths where its changes conflict.
pub enum Carried {
    Moved(Commit),
    Conflicts(Vec<RepoPathBuf>),
}

/// Moves the working-copy commit `wc` onto `target`, its changes carried as `carry` says, and
/// what was built on it with it.
pub async fn carry(
    repo: &mut MutableRepo,
    wc: &Commit,
    target: &Commit,
    carry: Carry,
) -> Result<Carried, CommandError> {
    let base = wc.parent_tree(repo).await?;
    let changed = wc.tree().tree_ids() != base.tree_ids();
    if wc.parent_ids() == [target.id().clone()] && carry != Carry::Discard {
        return Ok(Carried::Moved(wc.clone()));
    }
    let tree = if !changed || carry == Carry::Discard {
        target.tree()
    } else {
        MergedTree::merge(Merge::from_vec(vec![
            (target.tree(), "the commit checked out".to_owned()),
            (base, "HEAD".to_owned()),
            (wc.tree(), "the working copy".to_owned()),
        ]))
        .await?
    };
    if carry == Carry::Keep && tree.has_conflict() {
        let theirs = target.tree();
        let mut paths = Vec::new();
        for (path, _) in tree.conflicts() {
            if theirs.path_value(&path).await?.is_resolved() {
                paths.push(path);
            }
        }
        if !paths.is_empty() {
            return Ok(Carried::Conflicts(paths));
        }
    }
    let moved = repo
        .rewrite_commit(wc)
        .set_parents(vec![target.id().clone()])
        .set_tree(tree)
        .write()
        .await?;
    repo.rebase_descendants().await?;
    Ok(Carried::Moved(moved))
}

/// git's refusal of a move the working tree's changes would conflict with.
fn refuse_overwrite(
    ui: &Ui,
    cx: &Cx<'_>,
    ws: &WorkspaceCommandHelper,
    what: &str,
    paths: &[RepoPathBuf],
) -> Code {
    let converter = inspect::cwd_converter(cx, ws);
    let mut e = ui.stderr();
    writeln!(
        e,
        "error: Your local changes to the following files would be overwritten by {what}:"
    )?;
    for p in paths {
        writeln!(e, "\t{}", converter.format_file_path(p))?;
    }
    writeln!(
        e,
        "Please commit your changes or stash them before you {}.",
        if what == "checkout" {
            "switch branches"
        } else {
            what
        }
    )?;
    writeln!(e, "Aborting")?;
    Ok(1)
}

/// Moves the branch HEAD is on from `old` to `new`, as a commit on it does. On `main` while there
/// is no local `main`, the first commit makes one: git's local main, ahead of `origin/main`.
pub fn advance(repo: &mut MutableRepo, head: &Head, old: &CommitId, new: &CommitId) {
    let Head::Branch(b) = head else {
        return;
    };
    match repo::branch_target(repo, b) {
        Some(t) if t == *old => {
            repo.set_local_bookmark_target(RefName::new(b), RefTarget::normal(new.clone()))
        }
        None if b == repo::MAIN && repo::origin_main(repo).as_ref() != Some(new) => {
            repo.set_local_bookmark_target(RefName::new(b), RefTarget::normal(new.clone()))
        }
        _ => {}
    }
}

/// The commits git leaves behind when HEAD moves from `from` to `to` (`reset`): those of
/// `to..from` that nothing else — no other branch, no other line of work, no stash — keeps.
/// ok abandons them, as git lets them go; `jj undo` brings them back.
pub async fn left_behind(
    repo: &dyn Repo,
    from: &CommitId,
    to: &CommitId,
    wc: &CommitId,
    branch: Option<&str>,
) -> Result<Vec<Commit>, CommandError> {
    let range = RevsetExpression::commits(vec![to.clone()])
        .range(&RevsetExpression::commits(vec![from.clone()]));
    let mut keep: Vec<CommitId> = repo
        .view()
        .heads()
        .iter()
        .filter(|h| *h != wc)
        .cloned()
        .collect();
    for (name, id) in repo::branches(repo) {
        if Some(name.as_str()) != branch
            && let Some(id) = id
        {
            keep.push(id);
        }
    }
    keep.extend(repo::remote_branches(repo).into_iter().map(|(_, id)| id));
    let range_ids: Vec<CommitId> = evaluate(repo, &range).await?;
    let range_set: HashSet<CommitId> = range_ids.iter().cloned().collect();
    // A head inside the range keeps nothing but itself as git sees it: it is the range.
    keep.retain(|k| !range_set.contains(k));
    let kept: HashSet<CommitId> = if keep.is_empty() {
        HashSet::new()
    } else {
        evaluate(
            repo,
            &RevsetExpression::commits(keep)
                .ancestors()
                .intersection(&range),
        )
        .await?
        .into_iter()
        .collect()
    };
    let immutable: HashSet<CommitId> = match repo::origin_main(repo) {
        Some(m) => evaluate(
            repo,
            &RevsetExpression::commits(vec![m])
                .ancestors()
                .intersection(&range),
        )
        .await?
        .into_iter()
        .collect(),
        None => HashSet::new(),
    };
    let mut out = Vec::new();
    for id in range_ids {
        if !kept.contains(&id) && !immutable.contains(&id) {
            out.push(repo.store().get_commit_async(&id).await?);
        }
    }
    Ok(out)
}

async fn evaluate(
    repo: &dyn Repo,
    expression: &std::sync::Arc<ResolvedRevsetExpression>,
) -> Result<Vec<CommitId>, CommandError> {
    expression
        .clone()
        .evaluate(repo)
        .map_err(internal_error)?
        .stream()
        .try_collect()
        .await
        .map_err(internal_error)
}

/// What a commit made, as `git commit` reports it: `[branch abc1234] subject`, then its
/// shortstat and summary.
pub async fn report_commit(
    ui: &Ui,
    cx: &Cx<'_>,
    ws: &WorkspaceCommandHelper,
    commit: &Commit,
    head: &Head,
) -> Result<(), CommandError> {
    let repo = ws.repo().as_ref();
    let short = repo::short(repo, commit.id()).await?;
    let root = commit.parent_ids() == [repo.store().root_commit_id().clone()];
    let place = match head {
        Head::Branch(b) if root => format!("{b} (root-commit)"),
        Head::Branch(b) => b.clone(),
        Head::Detached => "detached HEAD".to_owned(),
    };
    let mut out = ui.stdout_formatter();
    writeln!(
        out,
        "[{place} {short}] {}",
        fmt::subject(commit.description())
    )?;
    let show = inspect::DiffShow {
        patch: false,
        words: false,
        stat: false,
        numstat: false,
        shortstat: true,
        name_only: false,
        name_status: false,
        summary: true,
        context: 3,
        prefix: true,
        compare: jj_lib::diff_presentation::LineCompareMode::Exact,
        filter: None,
        reverse: false,
        relative: false,
        z: false,
    };
    let (from, to, copies) = inspect::commit_trees(repo, commit, &EverythingMatcher).await?;
    let any = inspect::show_diff(
        ui,
        cx,
        ws,
        repo,
        out.as_mut(),
        (&from, &to),
        &copies,
        &EverythingMatcher,
        &show,
    )
    .await?;
    if !any {
        writeln!(out, " 0 files changed")?;
    }
    Ok(())
}

/// Where HEAD is, before a command moves it: the workspace's state and the branch.
pub struct Before {
    pub st: repo::State,
    pub head: Head,
}

pub async fn before(ws: &WorkspaceCommandHelper) -> Result<Before, CommandError> {
    let st = repo::state(ws).await?;
    let head = repo::current(ws.repo().as_ref(), ws.workspace_root(), st.head.id());
    Ok(Before { st, head })
}

/// Records where HEAD went, for the next command's `current` and `checkout -`.
pub fn record(ws: &WorkspaceCommandHelper, b: &Before, head: &Head, new_head: &CommitId) {
    repo::write_hint(ws.workspace_root(), &b.head, b.st.head.id(), head, new_head);
}

/// A commit message as git keeps one: trailing whitespace off each line, comment lines dropped
/// when `strip`, runs of blank lines made one, none at either end, and a final newline.
pub fn cleanup(message: &str, strip: bool) -> String {
    let mut out: Vec<&str> = Vec::new();
    let mut blank = false;
    for line in message.lines() {
        if strip && line.starts_with('#') {
            continue;
        }
        let line = line.trim_end();
        if line.is_empty() {
            blank = !out.is_empty();
            continue;
        }
        if blank {
            out.push("");
            blank = false;
        }
        out.push(line);
    }
    if out.is_empty() {
        String::new()
    } else {
        format!("{}\n", out.join("\n"))
    }
}

/// Opens the editor on a message, with git's comment lines beneath it, and returns what is
/// left once they are stripped.
pub fn edit_message(
    cx: &Cx<'_>,
    initial: &str,
    comments: &[String],
) -> Result<String, CommandError> {
    let editor = jj_cli::description_util::TextEditor::from_settings(cx.command.settings())
        .map_err(jj_cli::command_error::config_error)?;
    let mut text = initial.trim_end().to_owned();
    text.push_str("\n\n# Please enter the commit message for your changes. Lines starting\n# with '#' will be ignored, and an empty message aborts the commit.\n");
    for c in comments {
        text.push('#');
        if !c.is_empty() {
            text.push(' ');
            text.push_str(c);
        }
        text.push('\n');
    }
    let edited = editor
        .edit_str(text, Some(".COMMIT_EDITMSG"))
        .map_err(|e| jj_cli::command_error::user_error_with_message("the editor failed", e))?;
    Ok(cleanup(&edited, true))
}

/// `Name <email>`, as `--author` gives it.
fn parse_ident(s: &str) -> Option<(String, String)> {
    let s = s.trim();
    let open = s.rfind('<')?;
    let close = s.rfind('>')?;
    (open < close).then(|| {
        (
            s[..open].trim().to_owned(),
            s[open + 1..close].trim().to_owned(),
        )
    })
}

/// A `--date` as a timestamp, in this machine's zone.
fn parse_date(s: &str) -> Option<Timestamp> {
    let secs = inspect::approxidate(s)?;
    let offset = chrono::Local::now().offset().local_minus_utc() / 60;
    Some(Timestamp {
        timestamp: jj_lib::backend::MillisSinceEpoch(secs * 1000),
        tz_offset: offset,
    })
}

/// Appends a trailer, after the message's trailers if it ends in some, else after a blank line.
fn add_trailer(message: &str, trailer: &str) -> String {
    let m = message.trim_end();
    if m.lines().any(|l| l.trim() == trailer) {
        return format!("{m}\n");
    }
    let last = m.rsplit("\n\n").next().unwrap_or("");
    let is_trailers = !last.is_empty()
        && m.contains("\n\n")
        && last.lines().all(|l| {
            l.split_once(':')
                .is_some_and(|(k, _)| !k.is_empty() && !k.contains(' '))
        });
    if m.is_empty() {
        format!("{trailer}\n")
    } else if is_trailers {
        format!("{m}\n{trailer}\n")
    } else {
        format!("{m}\n\n{trailer}\n")
    }
}

/// `git <cmd> -- <path>` arguments: those after `--`, and those before it.
fn all_paths(before: &[String], after: &[String]) -> Vec<String> {
    let mut v = before.to_vec();
    v.extend(after.iter().cloned());
    v
}

// ---------------------------------------------------------------------------------------------
// add, rm, mv

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct AddArgs {
    #[arg(short = 'n', long = "dry-run")]
    dry_run: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(short = 'f', long)]
    force: bool,
    #[arg(short = 'i', long)]
    interactive: bool,
    #[arg(short = 'p', long)]
    patch: bool,
    #[arg(short = 'e', long)]
    edit: bool,
    #[arg(short = 'u', long)]
    update: bool,
    #[arg(short = 'A', long = "all", visible_alias = "no-ignore-removal")]
    all: bool,
    #[arg(long = "no-all", visible_alias = "ignore-removal")]
    no_all: bool,
    #[arg(short = 'N', long = "intent-to-add")]
    intent_to_add: bool,
    #[arg(long)]
    refresh: bool,
    #[arg(long = "ignore-errors")]
    ignore_errors: bool,
    #[arg(long = "ignore-missing")]
    ignore_missing: bool,
    #[arg(long)]
    sparse: bool,
    #[arg(long)]
    renormalize: bool,
    #[arg(long)]
    chmod: Option<String>,
    #[arg(long = "pathspec-from-file")]
    pathspec_from_file: Option<String>,
    #[arg(long = "pathspec-file-nul")]
    pathspec_file_nul: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    paths: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// `git add`: there is nothing to add — ok tracks every file the ignore files do not name, and
/// the index is the working tree. `-f` tracks ignored files it names (`jj file track
/// --include-ignored`), and `--chmod` sets the executable bit, as the next commit records it.
pub async fn add(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: AddArgs = parse_or_return!(ui, "add", args);
    let paths = all_paths(&a.paths, &a.rest);
    if let Some(mode) = &a.chmod {
        let exec = match mode.as_str() {
            "+x" => true,
            "-x" => false,
            _ => {
                return fatal(
                    ui,
                    format!("--chmod param '{mode}' must be either -x or +x"),
                );
            }
        };
        for p in &paths {
            let path = cx.command.cwd().join(p);
            if let Err(e) = set_executable(&path, exec) {
                return fatal(ui, format!("cannot chmod {p}: {e}"));
            }
        }
    }
    if a.force && !paths.is_empty() && !a.dry_run {
        let status = std::process::Command::new(std::env::current_exe()?)
            .args(["--no-pager", "file", "track", "--include-ignored", "--"])
            .args(&paths)
            .current_dir(cx.command.cwd())
            .status()?;
        return Ok(if status.success() { 0 } else { 128 });
    }
    if !a.quiet {
        writeln!(
            ui.stderr(),
            "ok tracks every file automatically: there is nothing to add."
        )?;
    }
    Ok(0)
}

#[cfg(unix)]
fn set_executable(path: &Path, exec: bool) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let mut perms = std::fs::metadata(path)?.permissions();
    let mode = perms.mode();
    perms.set_mode(if exec {
        mode | ((mode & 0o444) >> 2)
    } else {
        mode & !0o111
    });
    std::fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_executable(_path: &Path, _exec: bool) -> std::io::Result<()> {
    Ok(())
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct RmArgs {
    #[arg(short = 'f', long)]
    force: bool,
    #[arg(short = 'n', long = "dry-run")]
    dry_run: bool,
    #[arg(short = 'r')]
    recursive: bool,
    #[arg(long)]
    cached: bool,
    #[arg(long = "ignore-unmatch")]
    ignore_unmatch: bool,
    #[arg(long)]
    sparse: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(long = "pathspec-from-file")]
    pathspec_from_file: Option<String>,
    #[arg(long = "pathspec-file-nul")]
    pathspec_file_nul: bool,
    paths: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// `git rm`: tracked files removed from the working tree (what the next commit records), or,
/// with `--cached`, untracked and kept (`jj file untrack`, which keeps only what the ignore files
/// name).
pub async fn rm(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: RmArgs = parse_or_return!(ui, "rm", args);
    let specs = all_paths(&a.paths, &a.rest);
    if specs.is_empty() {
        return fatal(ui, "No pathspec was given. Which files should I remove?");
    }
    let ws = cx.ws(ui).await?;
    let st = repo::state(&ws).await?;
    let matcher = match cx.pathspec(&ws, &specs) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let files = repo::files(&st.wc.tree(), &matcher).await?;
    let converter = inspect::cwd_converter(cx, &ws);
    for p in matcher.literal_paths() {
        let matched: Vec<_> = files.iter().filter(|(f, _)| f.starts_with(&p)).collect();
        if matched.is_empty() {
            if a.ignore_unmatch {
                continue;
            }
            return fatal(
                ui,
                format!(
                    "pathspec '{}' did not match any files",
                    converter.format_file_path(&p)
                ),
            );
        }
        if !a.recursive && matched.iter().any(|(f, _)| *f != p) {
            return fatal(
                ui,
                format!(
                    "not removing '{}' recursively without -r",
                    converter.format_file_path(&p)
                ),
            );
        }
    }
    if a.cached {
        if a.dry_run {
            for (f, _) in &files {
                writeln!(ui.stdout(), "rm '{}'", f.as_internal_file_string())?;
            }
            return Ok(0);
        }
        drop(ws);
        let status = std::process::Command::new(std::env::current_exe()?)
            .args(["--no-pager", "file", "untrack", "--"])
            .args(&specs)
            .current_dir(cx.command.cwd())
            .status()?;
        return Ok(if status.success() { 0 } else { 128 });
    }
    let root = ws.workspace_root().to_owned();
    let mut dirs: Vec<PathBuf> = Vec::new();
    for (f, _) in &files {
        if !a.quiet {
            writeln!(ui.stdout(), "rm '{}'", f.as_internal_file_string())?;
        }
        if a.dry_run {
            continue;
        }
        let path = f.to_fs_path_unchecked(&root);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return fatal(
                    ui,
                    format!("git rm: '{}': {e}", f.as_internal_file_string()),
                );
            }
        }
        if let Some(parent) = path.parent() {
            dirs.push(parent.to_owned());
        }
    }
    // git leaves no directory the removal emptied.
    dirs.sort();
    dirs.dedup();
    for d in dirs.iter().rev() {
        let mut d = d.as_path();
        while d != root && std::fs::remove_dir(d).is_ok() {
            match d.parent() {
                Some(p) => d = p,
                None => break,
            }
        }
    }
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct MvArgs {
    #[arg(short = 'f', long)]
    force: bool,
    #[arg(short = 'k')]
    skip_errors: bool,
    #[arg(short = 'n', long = "dry-run")]
    dry_run: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(long)]
    sparse: bool,
    paths: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// `git mv`: tracked files or directories renamed in the working tree; the next snapshot sees
/// the rename (ok's diffs find renames as git's do).
pub async fn mv(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: MvArgs = parse_or_return!(ui, "mv", args);
    let all = all_paths(&a.paths, &a.rest);
    if all.len() < 2 {
        writeln!(
            ui.stderr(),
            "usage: git mv [<options>] <source>... <destination>"
        )?;
        return Ok(129);
    }
    let ws = cx.ws(ui).await?;
    let st = repo::state(&ws).await?;
    let cwd = cx.command.cwd().to_owned();
    let (sources, dest) = all.split_at(all.len() - 1);
    let dest = &dest[0];
    let dest_path = cwd.join(dest);
    let into_dir =
        dest_path.is_dir() && !(sources.len() == 1 && dest_path == cwd.join(&sources[0]));
    if sources.len() > 1 && !into_dir {
        return fatal(ui, format!("destination '{dest}' is not a directory"));
    }
    for s in sources {
        let rp = match ws.parse_file_path(s) {
            Ok(p) => p,
            Err(_) => return fatal(ui, format!("'{s}' is outside repository")),
        };
        let tracked = !st.wc.tree().path_value(&rp).await?.is_absent();
        let target = if into_dir {
            dest_path.join(Path::new(s).file_name().unwrap_or_default())
        } else {
            dest_path.clone()
        };
        let shown_target = target
            .strip_prefix(&cwd)
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| target.display().to_string());
        let problem = if !tracked {
            Some("not under version control")
        } else if target.exists() && !a.force {
            Some("destination exists")
        } else {
            None
        };
        if let Some(why) = problem {
            if a.skip_errors {
                continue;
            }
            return fatal(ui, format!("{why}, source={s}, destination={shown_target}"));
        }
        if a.verbose || a.dry_run {
            writeln!(ui.stdout(), "Renaming {s} to {shown_target}")?;
        }
        if a.dry_run {
            continue;
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if let Err(e) = std::fs::rename(cwd.join(s), &target) {
            if a.skip_errors {
                continue;
            }
            return fatal(ui, format!("renaming '{s}' failed: {e}"));
        }
    }
    Ok(0)
}

// ---------------------------------------------------------------------------------------------
// restore

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct RestoreArgs {
    #[arg(short = 's', long)]
    source: Option<String>,
    #[arg(short = 'S', long)]
    staged: bool,
    #[arg(short = 'W', long)]
    worktree: bool,
    #[arg(short = 'p', long)]
    patch: bool,
    #[arg(long)]
    ours: bool,
    #[arg(long)]
    theirs: bool,
    #[arg(short = 'm', long)]
    merge: bool,
    #[arg(long)]
    conflict: Option<String>,
    #[arg(long = "ignore-unmerged")]
    ignore_unmerged: bool,
    #[arg(long = "ignore-skip-worktree-bits")]
    ignore_skip_worktree_bits: bool,
    #[arg(long = "overlay")]
    overlay: bool,
    #[arg(long = "no-overlay")]
    no_overlay: bool,
    #[arg(long = "recurse-submodules")]
    recurse_submodules: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(long)]
    progress: bool,
    #[arg(long = "pathspec-from-file")]
    pathspec_from_file: Option<String>,
    #[arg(long = "pathspec-file-nul")]
    pathspec_file_nul: bool,
    paths: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// Restores paths of the working copy from `source` (HEAD by default), in one transaction.
async fn restore_from(
    ui: &mut Ui,
    cx: &Cx<'_>,
    mut ws: WorkspaceCommandHelper,
    source: Option<&str>,
    specs: &[String],
    description: &str,
) -> Code {
    let st = repo::state(&ws).await?;
    let matcher = match cx.pathspec(&ws, specs) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let from = match source {
        Some(s) => {
            let r = cx.resolver(&ws).await?;
            match r.commit_of(s).await {
                Ok(c) => c.tree(),
                Err(e) => return rev_error(ui, s, e),
            }
        }
        None => st.wc.parent_tree(ws.repo().as_ref()).await?,
    };
    let converter = inspect::cwd_converter(cx, &ws);
    for p in matcher.literal_paths() {
        let single = jj_lib::matchers::PrefixMatcher::new([p.clone()]);
        if repo::files(&st.wc.tree(), &single).await?.is_empty()
            && repo::files(&from, &single).await?.is_empty()
        {
            return error(
                ui,
                format!(
                    "pathspec '{}' did not match any file(s) known to git",
                    converter.format_file_path(&p)
                ),
                1,
            );
        }
    }
    let tree = restore_tree(
        &from,
        &st.wc.tree(),
        "the source".to_owned(),
        "the working copy".to_owned(),
        &matcher,
    )
    .await?;
    if tree.tree_ids() == st.wc.tree().tree_ids() {
        return Ok(0);
    }
    let mut tx = ws.start_transaction();
    tx.repo_mut()
        .rewrite_commit(&st.wc)
        .set_tree(tree)
        .write()
        .await?;
    tx.repo_mut().rebase_descendants().await?;
    tx.finish(ui, description).await?;
    Ok(0)
}

/// `git restore`: paths of the working tree as HEAD (or `--source`) has them. The index is the
/// working tree, so `--staged` alone has nothing to undo.
pub async fn restore(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: RestoreArgs = parse_or_return!(ui, "restore", args);
    let specs = all_paths(&a.paths, &a.rest);
    if specs.is_empty() {
        return fatal(ui, "you must specify path(s) to restore");
    }
    if a.patch {
        fatal(ui, "git restore --patch is not available in ok workspaces")?;
        hint(ui, "`jj restore -i` restores parts of files interactively")?;
        return Ok(128);
    }
    if a.ours || a.theirs || a.merge || a.conflict.is_some() {
        fatal(
            ui,
            "ok keeps conflicts in the commit: resolve them in the files",
        )?;
        hint(ui, "`jj resolve` runs a merge tool on them")?;
        return Ok(128);
    }
    if a.staged && !a.worktree {
        if !a.quiet {
            writeln!(
                ui.stderr(),
                "ok has no staging area: every change is in the next commit, and nothing is unstaged."
            )?;
            hint(ui, "`git restore <file>` discards a file's changes")?;
        }
        return Ok(0);
    }
    let ws = cx.ws(ui).await?;
    restore_from(ui, cx, ws, a.source.as_deref(), &specs, "git restore").await
}

// ---------------------------------------------------------------------------------------------
// commit

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct CommitArgs {
    #[arg(short = 'm', long = "message")]
    message: Vec<String>,
    #[arg(short = 'F', long = "file")]
    file: Option<String>,
    #[arg(short = 'a', long)]
    all: bool,
    #[arg(short = 'i', long)]
    include: bool,
    #[arg(short = 'o', long)]
    only: bool,
    #[arg(long)]
    amend: bool,
    #[arg(long = "no-edit")]
    no_edit: bool,
    #[arg(short = 'e', long)]
    edit: bool,
    #[arg(long = "allow-empty")]
    allow_empty: bool,
    #[arg(long = "allow-empty-message")]
    allow_empty_message: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(short = 'n', long = "no-verify")]
    no_verify: bool,
    #[arg(long)]
    verify: bool,
    #[arg(long = "no-post-rewrite")]
    no_post_rewrite: bool,
    #[arg(long)]
    author: Option<String>,
    #[arg(long)]
    date: Option<String>,
    #[arg(short = 's', long)]
    signoff: bool,
    #[arg(long = "no-signoff")]
    no_signoff: bool,
    #[arg(long = "reset-author")]
    reset_author: bool,
    #[arg(short = 'C', long = "reuse-message")]
    reuse_message: Option<String>,
    #[arg(short = 'c', long = "reedit-message")]
    reedit_message: Option<String>,
    #[arg(long)]
    fixup: Option<String>,
    #[arg(long)]
    squash: Option<String>,
    #[arg(long)]
    trailer: Vec<String>,
    #[arg(long)]
    cleanup: Option<String>,
    #[arg(long = "dry-run")]
    dry_run: bool,
    #[arg(long)]
    status: bool,
    #[arg(long = "no-status")]
    no_status: bool,
    #[arg(short = 'S', long = "gpg-sign", num_args = 0..=1, require_equals = true, default_missing_value = "")]
    gpg_sign: Option<String>,
    #[arg(long = "no-gpg-sign")]
    no_gpg_sign: bool,
    #[arg(short = 'z', long = "null")]
    null: bool,
    #[arg(long)]
    porcelain: bool,
    #[arg(long)]
    short: bool,
    #[arg(long)]
    branch: bool,
    #[arg(long)]
    long: bool,
    #[arg(long)]
    interactive: bool,
    #[arg(short = 'p', long)]
    patch: bool,
    #[arg(long = "pathspec-from-file")]
    pathspec_from_file: Option<String>,
    #[arg(long = "pathspec-file-nul")]
    pathspec_file_nul: bool,
    paths: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// `-u<mode>` (untracked files) and the like, which clap cannot read and an ok workspace does not
/// need: nothing is untracked.
fn drop_short_attached(args: &[String], flag: char) -> Vec<String> {
    let prefix = format!("-{flag}");
    let mut out = Vec::new();
    let mut after = false;
    for a in args {
        if a == "--" {
            after = true;
        }
        if !after && a.starts_with(&prefix) && !a.starts_with("--") {
            continue;
        }
        out.push(a.clone());
    }
    out
}

/// `git commit`: the working change, or the paths named of it, becomes a commit on HEAD, and the
/// branch moves to it. `--amend` folds the working change into HEAD instead; so does a commit
/// while HEAD has conflicts, which finishes the merge, cherry-pick, or revert that made them.
pub async fn commit(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let args = drop_short_attached(args, 'u');
    let a: CommitArgs = parse_or_return!(ui, "commit", &args);
    if a.interactive || a.patch {
        fatal(ui, "git commit --patch is not available in ok workspaces")?;
        hint(
            ui,
            "`git commit <paths>` commits some files; `jj split -i` chooses parts of them",
        )?;
        return Ok(128);
    }
    let mut ws = cx.ws(ui).await?;
    let b = before(&ws).await?;
    let repo = ws.repo().clone();
    let r = cx.resolver(&ws).await?;
    let specs = all_paths(&a.paths, &a.rest);
    let matcher = match cx.pathspec(&ws, &specs) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let wc = b.st.wc.clone();
    let head = b.st.head.clone();
    let parent_tree = wc.parent_tree(repo.as_ref()).await?;
    let converter = inspect::cwd_converter(cx, &ws);
    for p in matcher.literal_paths() {
        let single = jj_lib::matchers::PrefixMatcher::new([p.clone()]);
        if repo::files(&wc.tree(), &single).await?.is_empty()
            && repo::files(&parent_tree, &single).await?.is_empty()
        {
            return error(
                ui,
                format!(
                    "pathspec '{}' did not match any file(s) known to git",
                    converter.format_file_path(&p)
                ),
                1,
            );
        }
    }
    let selected = if matcher.is_everything() {
        wc.tree()
    } else {
        restore_tree(
            &wc.tree(),
            &parent_tree,
            "the working copy".to_owned(),
            "HEAD".to_owned(),
            &matcher,
        )
        .await?
    };
    let selected_conflicts = selected.has_conflict()
        && selected.conflicts().any(|(p, _)| matcher.matches(&p))
        && !head.has_conflict();
    if selected_conflicts {
        error(
            ui,
            "Committing is not possible because you have unmerged files.",
            128,
        )?;
        hint(ui, "Fix them up in the work tree, then commit again.")?;
        return fatal(ui, "Exiting because of an unresolved conflict.");
    }
    let empty = selected.tree_ids() == parent_tree.tree_ids();
    // A commit while HEAD has conflicts finishes what made them: the working change folds into
    // HEAD, as --amend folds it.
    let finishing = !a.amend && !b.st.unborn && head.has_conflict();
    let amend = a.amend || finishing;
    if amend {
        if b.st.unborn {
            return fatal(ui, "You have nothing to amend.");
        }
        let imm = ws.env().immutable_expression().clone();
        let immutable: Vec<CommitId> = ws
            .attach_revset_evaluator(
                imm.intersection(&RevsetExpression::commits(vec![head.id().clone()])),
            )
            .evaluate_to_commit_ids()?
            .try_collect()
            .await?;
        if !immutable.is_empty() {
            fatal(
                ui,
                "HEAD is on main (it has landed): there is nothing to amend.",
            )?;
            hint(ui, "`git commit` makes a new commit on it")?;
            return Ok(128);
        }
    }
    if !amend && empty && !a.allow_empty {
        let mut out = ui.stdout();
        match &b.head {
            Head::Branch(br) => writeln!(out, "On branch {br}")?,
            Head::Detached => writeln!(
                out,
                "HEAD detached at {}",
                repo::short(repo.as_ref(), head.id()).await?
            )?,
        }
        writeln!(out, "nothing to commit, working tree clean")?;
        return Ok(1);
    }
    // The message.
    let base_message = if amend {
        head.description().to_owned()
    } else {
        wc.description().to_owned()
    };
    let mut author_from: Option<Signature> = None;
    let mut message: Option<String> = None;
    let mut use_editor = a.edit;
    if !a.message.is_empty() {
        message = Some(cleanup(
            &jj_cli::description_util::join_message_paragraphs(&a.message),
            false,
        ));
    } else if let Some(f) = &a.file {
        let text = if f == "-" {
            let mut s = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut s)?;
            s
        } else {
            match std::fs::read_to_string(cx.command.cwd().join(f)) {
                Ok(t) => t,
                Err(e) => return fatal(ui, format!("could not read log file '{f}': {e}")),
            }
        };
        message = Some(cleanup(&text, a.cleanup.as_deref() == Some("strip")));
    } else if let Some(rv) = a.reuse_message.as_ref().or(a.reedit_message.as_ref()) {
        let c = match r.commit_of(rv).await {
            Ok(c) => c,
            Err(e) => return rev_error(ui, rv, e),
        };
        message = Some(c.description().to_owned());
        author_from = Some(c.author().clone());
        use_editor = a.reedit_message.is_some() || a.edit;
    } else if let Some(rv) = a.fixup.as_ref().or(a.squash.as_ref()) {
        let c = match r
            .commit_of(
                rv.trim_start_matches("amend:")
                    .trim_start_matches("reword:"),
            )
            .await
        {
            Ok(c) => c,
            Err(e) => return rev_error(ui, rv, e),
        };
        let kind = if a.fixup.is_some() { "fixup" } else { "squash" };
        message = Some(format!("{kind}! {}\n", fmt::subject(c.description())));
        use_editor = a.squash.is_some() || a.edit;
    } else if a.no_edit {
        message = Some(base_message.clone());
    } else {
        use_editor = true;
    }
    let mut msg = message.unwrap_or_else(|| base_message.clone());
    for t in &a.trailer {
        let t = match t.split_once('=') {
            Some((k, v)) if !t.contains(':') => format!("{}: {}", k.trim(), v.trim()),
            _ => t.trim().to_owned(),
        };
        msg = add_trailer(&msg, &t);
    }
    if a.signoff && !a.no_signoff {
        let s = cx.command.settings();
        msg = add_trailer(
            &msg,
            &format!("Signed-off-by: {} <{}>", s.user_name(), s.user_email()),
        );
    }
    if a.dry_run {
        return inspect::status(ui, cx, &[])
            .await
            .map(|_| if empty { 1 } else { 0 });
    }
    if use_editor {
        let mut comments = vec![String::new()];
        match &b.head {
            Head::Branch(br) => comments.push(format!("On branch {br}")),
            Head::Detached => comments.push(format!(
                "HEAD detached at {}",
                repo::short(repo.as_ref(), head.id()).await?
            )),
        }
        comments.push("Changes to be committed:".to_owned());
        let changes = inspect::entries(
            &parent_tree,
            &selected,
            &EverythingMatcher,
            &CopyRecords::default(),
        )
        .await?;
        for e in &changes {
            let what = match e.status {
                'A' => "new file:   ",
                'D' => "deleted:    ",
                _ => "modified:   ",
            };
            comments.push(format!("\t{what}{}", e.target.as_internal_file_string()));
        }
        comments.push(String::new());
        msg = edit_message(cx, &msg, &comments)?;
    }
    if msg.trim().is_empty() && !a.allow_empty_message {
        writeln!(ui.stderr(), "Aborting commit due to empty commit message.")?;
        return Ok(1);
    }
    let author = |current: &Signature| -> Result<Signature, String> {
        let mut sig = author_from.clone().unwrap_or_else(|| current.clone());
        if a.reset_author {
            sig = cx.command.settings().signature();
        }
        if let Some(au) = &a.author {
            let Some((name, email)) = parse_ident(au) else {
                return Err(format!(
                    "--author '{au}' is not 'Name <email>' and matches no existing author"
                ));
            };
            sig.name = name;
            sig.email = email;
        }
        if let Some(d) = &a.date {
            match parse_date(d) {
                Some(t) => sig.timestamp = t,
                None => return Err(format!("invalid date format: {d}")),
            }
        }
        Ok(sig)
    };
    let name = ws_name(&ws);
    let mut tx = ws.start_transaction();
    let new_head = if amend {
        let sig = match author(head.author()) {
            Ok(s) => s,
            Err(e) => return fatal(ui, e),
        };
        let folded = if matcher.is_everything() {
            wc.tree()
        } else {
            restore_tree(
                &wc.tree(),
                &head.tree(),
                "the working copy".to_owned(),
                "HEAD".to_owned(),
                &matcher,
            )
            .await?
        };
        let amended = tx
            .repo_mut()
            .rewrite_commit(&head)
            .set_tree(folded)
            .set_description(msg)
            .set_author(sig)
            .write()
            .await?;
        tx.repo_mut().rebase_descendants().await?;
        amended
    } else {
        let sig = match author(wc.author()) {
            Ok(s) => s,
            Err(e) => return fatal(ui, e),
        };
        let committed = tx
            .repo_mut()
            .rewrite_commit(&wc)
            .set_tree(selected)
            .set_description(msg)
            .set_author(sig)
            .write()
            .await?;
        let new_wc = tx
            .repo_mut()
            .new_commit(vec![committed.id().clone()], wc.tree())
            .write()
            .await?;
        tx.repo_mut()
            .edit(name.clone(), &new_wc)
            .await
            .map_err(internal_error)?;
        advance(tx.repo_mut(), &b.head, head.id(), committed.id());
        committed
    };
    let what = if amend {
        "git commit --amend"
    } else {
        "git commit"
    };
    tx.finish(ui, what).await?;
    let now_head = match &b.head {
        Head::Branch(br) => Head::Branch(br.clone()),
        Head::Detached => Head::Detached,
    };
    record(&ws, &b, &now_head, new_head.id());
    if !a.quiet {
        report_commit(ui, cx, &ws, &new_head, &now_head).await?;
    }
    Ok(0)
}

fn ws_name(ws: &WorkspaceCommandHelper) -> jj_lib::ref_name::WorkspaceNameBuf {
    ws.workspace_name().to_owned()
}

// ---------------------------------------------------------------------------------------------
// reset

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct ResetArgs {
    #[arg(long)]
    soft: bool,
    #[arg(long)]
    mixed: bool,
    #[arg(long)]
    hard: bool,
    #[arg(long)]
    merge: bool,
    #[arg(long)]
    keep: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'p', long)]
    patch: bool,
    #[arg(short = 'N', long = "intent-to-add")]
    intent_to_add: bool,
    #[arg(long = "no-refresh")]
    no_refresh: bool,
    #[arg(long)]
    refresh: bool,
    #[arg(long = "recurse-submodules")]
    recurse_submodules: bool,
    #[arg(long = "pathspec-from-file")]
    pathspec_from_file: Option<String>,
    #[arg(long = "pathspec-file-nul")]
    pathspec_file_nul: bool,
    args: Vec<String>,
    #[arg(last = true)]
    paths: Vec<String>,
}

/// `git reset [--soft|--mixed|--hard|--keep] [<commit>]`: HEAD, and the branch it is on, moved to
/// the commit. `--soft` and `--mixed` (one here: the index is the working tree) keep the working
/// tree as it is; `--hard` makes it the commit's; `--keep` carries its changes, refusing where
/// they conflict. Commits left behind that nothing else keeps are abandoned, as git lets them go
/// (`jj undo` brings them back). With paths, it would unstage them: there is nothing to unstage.
pub async fn reset(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: ResetArgs = parse_or_return!(ui, "reset", args);
    if a.patch {
        fatal(ui, "git reset --patch is not available in ok workspaces")?;
        hint(ui, "`jj restore -i` restores parts of files interactively")?;
        return Ok(128);
    }
    let mut ws = cx.ws(ui).await?;
    let b = before(&ws).await?;
    let r = cx.resolver(&ws).await?;
    let (target_spec, paths) = match a.args.split_first() {
        Some((first, rest)) if r.commit_of(first).await.is_ok() => {
            (Some(first.clone()), rest.to_vec())
        }
        _ => (None, a.args.clone()),
    };
    let mut paths = paths;
    paths.extend(a.paths.iter().cloned());
    if !paths.is_empty() {
        if a.hard || a.soft || a.keep || a.merge {
            return fatal(ui, "Cannot do a hard/soft/keep reset with paths.");
        }
        if !a.quiet {
            writeln!(
                ui.stderr(),
                "ok has no staging area: every change is in the next commit, and nothing is unstaged."
            )?;
            hint(ui, "`git restore <file>` discards a file's changes")?;
        }
        return Ok(0);
    }
    let target = match &target_spec {
        Some(s) => match r.commit_of(s).await {
            Ok(c) => c,
            Err(e) => return rev_error(ui, s, e),
        },
        None => b.st.head.clone(),
    };
    let mode = if a.hard {
        Carry::Discard
    } else if a.keep || a.merge {
        Carry::Keep
    } else {
        Carry::Merge
    };
    let wc = b.st.wc.clone();
    let branch = b.head.branch().map(str::to_owned);
    let behind = left_behind(
        ws.repo().as_ref(),
        b.st.head.id(),
        target.id(),
        wc.id(),
        branch.as_deref(),
    )
    .await?;
    let mut tx = ws.start_transaction();
    let moved = if mode == Carry::Merge {
        // --soft, --mixed: the working tree stays exactly as it is, on the new HEAD.
        tx.repo_mut()
            .rewrite_commit(&wc)
            .set_parents(vec![target.id().clone()])
            .set_tree(wc.tree())
            .write()
            .await?
    } else {
        match carry(tx.repo_mut(), &wc, &target, mode).await? {
            Carried::Moved(c) => c,
            Carried::Conflicts(paths) => {
                drop(tx);
                return refuse_overwrite(ui, cx, &ws, "reset", &paths);
            }
        }
    };
    let _ = moved;
    for c in &behind {
        tx.repo_mut().record_abandoned_commit(c);
    }
    tx.repo_mut().rebase_descendants().await?;
    // The branch moves with HEAD; on `main` with no local one, a local `main` is made unless
    // HEAD is going back to `origin/main`.
    if let Some(br) = &branch {
        let local = repo::branch_target(tx.repo(), br).is_some();
        let to_origin = repo::origin_main(tx.repo()).as_ref() == Some(target.id());
        if local || (br == repo::MAIN && !to_origin) {
            tx.repo_mut().set_local_bookmark_target(
                RefName::new(br),
                RefTarget::normal(target.id().clone()),
            );
        }
    }
    let short = repo::short(tx.repo(), target.id()).await?;
    tx.finish(
        ui,
        format!("git reset {}", target_spec.as_deref().unwrap_or("HEAD")),
    )
    .await?;
    record(&ws, &b, &b.head, target.id());
    if a.hard && !a.quiet {
        writeln!(
            ui.stdout(),
            "HEAD is now at {short} {}",
            fmt::subject(target.description())
        )?;
    }
    Ok(0)
}

// ---------------------------------------------------------------------------------------------
// checkout, switch

/// Where a switch goes: the commit, and HEAD's place there.
struct Destination {
    commit: Commit,
    head: Head,
    /// A branch to make (or reset) at the commit first.
    create: Option<(String, bool)>,
}

/// Moves HEAD to a destination: the branch made, the working change carried, the hint written,
/// and git's messages said.
#[allow(clippy::too_many_arguments)]
async fn switch_to(
    ui: &mut Ui,
    cx: &Cx<'_>,
    mut ws: WorkspaceCommandHelper,
    b: Before,
    dest: Destination,
    mode: Carry,
    quiet: bool,
    shown: &str,
) -> Code {
    let wc = b.st.wc.clone();
    if let Some((name, reset)) = &dest.create {
        let exists = repo::branch_target(ws.repo().as_ref(), name).is_some();
        if exists && !*reset {
            return fatal(ui, format!("a branch named '{name}' already exists"));
        }
    }
    let same = dest.commit.id() == b.st.head.id() && mode != Carry::Discard;
    let mut tx = ws.start_transaction();
    if let Some((name, _)) = &dest.create {
        tx.repo_mut().set_local_bookmark_target(
            RefName::new(name),
            RefTarget::normal(dest.commit.id().clone()),
        );
    }
    if !same {
        match carry(tx.repo_mut(), &wc, &dest.commit, mode).await? {
            Carried::Moved(_) => {}
            Carried::Conflicts(paths) => {
                drop(tx);
                return refuse_overwrite(ui, cx, &ws, "checkout", &paths);
            }
        }
    }
    let track = inspect::tracking(
        tx.repo(),
        &dest.commit,
        dest.commit.id() == tx.repo().store().root_commit_id(),
    )
    .await?;
    let short = repo::short(tx.repo(), dest.commit.id()).await?;
    if tx.repo().has_changes() {
        tx.finish(ui, format!("git checkout {shown}")).await?;
    }
    record(&ws, &b, &dest.head, dest.commit.id());
    if quiet {
        return Ok(0);
    }
    let mut e = ui.stderr();
    match (&dest.head, &dest.create, &b.head) {
        (Head::Branch(n), Some((_, true)), _)
            if repo::branch_target(ws.repo().as_ref(), n).is_some()
                && b.head.branch() == Some(n) =>
        {
            writeln!(e, "Reset branch '{n}'")?
        }
        (Head::Branch(n), Some(_), _) => writeln!(e, "Switched to a new branch '{n}'")?,
        (Head::Branch(n), None, Head::Branch(o)) if n == o && same => {
            writeln!(e, "Already on '{n}'")?
        }
        (Head::Branch(n), None, _) => writeln!(e, "Switched to branch '{n}'")?,
        (Head::Detached, _, _) => writeln!(
            e,
            "HEAD is now at {short} {}",
            fmt::subject(dest.commit.description())
        )?,
    }
    if let (Head::Branch(_), Some((ahead, behind))) = (&dest.head, track) {
        let up = format!("{}/{}", repo::ORIGIN, repo::MAIN);
        match (ahead, behind) {
            (0, 0) => writeln!(e, "Your branch is up to date with '{up}'.")?,
            (n, 0) => writeln!(
                e,
                "Your branch is ahead of '{up}' by {n} commit{}.",
                if n == 1 { "" } else { "s" }
            )?,
            (0, n) => writeln!(
                e,
                "Your branch is behind '{up}' by {n} commit{}, and can be fast-forwarded.",
                if n == 1 { "" } else { "s" }
            )?,
            (x, y) => writeln!(
                e,
                "Your branch and '{up}' have diverged,\nand have {x} and {y} different commits each, respectively."
            )?,
        }
    }
    Ok(0)
}

/// What a name switches to: a branch (`main` being `origin/main` while there is no local one),
/// or, `detach`ing, any commit.
async fn destination(
    r: &repo::Resolver<'_>,
    root: &Path,
    name: &str,
    detach: bool,
) -> Result<Result<Destination, String>, CommandError> {
    let repo = r.repo;
    if name == "-" || name == "@{-1}" {
        return match repo::previous(root) {
            Some(repo::HintRef::Branch(b)) => Box::pin(destination(r, root, &b, false)).await,
            Some(repo::HintRef::Commit(c)) => Box::pin(destination(r, root, &c, true)).await,
            None => Ok(Err("no previous branch".to_owned())),
        };
    }
    if !detach {
        if let Some(id) = repo::branch_target(repo, name) {
            return Ok(Ok(Destination {
                commit: repo.store().get_commit_async(&id).await?,
                head: Head::Branch(name.to_owned()),
                create: None,
            }));
        }
        if name == repo::MAIN {
            let id =
                repo::origin_main(repo).unwrap_or_else(|| repo.store().root_commit_id().clone());
            return Ok(Ok(Destination {
                commit: repo.store().get_commit_async(&id).await?,
                head: Head::Branch(name.to_owned()),
                create: None,
            }));
        }
    }
    match r.commit_of(name).await {
        Ok(c) => Ok(Ok(Destination {
            commit: c,
            head: Head::Detached,
            create: None,
        })),
        Err(repo::RevError::Command(e)) => Err(e),
        Err(_) => Ok(Err(format!("invalid reference: {name}"))),
    }
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct SwitchArgs {
    #[arg(short = 'c', long = "create")]
    create: Option<String>,
    #[arg(short = 'C', long = "force-create")]
    force_create: Option<String>,
    #[arg(short = 'd', long)]
    detach: bool,
    #[arg(long)]
    guess: bool,
    #[arg(long = "no-guess")]
    no_guess: bool,
    #[arg(short = 'f', long = "force", visible_alias = "discard-changes")]
    force: bool,
    #[arg(short = 'm', long)]
    merge: bool,
    #[arg(long)]
    conflict: Option<String>,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(long)]
    progress: bool,
    #[arg(short = 't', long, num_args = 0..=1, require_equals = true, default_missing_value = "direct")]
    track: Option<String>,
    #[arg(long = "no-track")]
    no_track: bool,
    #[arg(long)]
    orphan: Option<String>,
    #[arg(long = "ignore-other-worktrees")]
    ignore_other_worktrees: bool,
    #[arg(long = "recurse-submodules")]
    recurse_submodules: bool,
    target: Option<String>,
}

/// `git switch`: to a branch (`-c` makes one; `--detach` goes to any commit), the working change
/// going along unless its changes would conflict there.
pub async fn switch(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: SwitchArgs = parse_or_return!(ui, "switch", args);
    if a.orphan.is_some() {
        return fatal(
            ui,
            "an ok repository has one line of history: there are no orphan branches",
        );
    }
    let mode = if a.force {
        Carry::Discard
    } else if a.merge {
        Carry::Merge
    } else {
        Carry::Keep
    };
    let ws = cx.ws(ui).await?;
    let b = before(&ws).await?;
    let root = ws.workspace_root().to_owned();
    let r = cx.resolver(&ws).await?;
    let create = a
        .create
        .clone()
        .map(|n| (n, false))
        .or(a.force_create.clone().map(|n| (n, true)));
    let dest = if let Some((name, reset)) = create {
        let start = a.target.clone().unwrap_or_else(|| "HEAD".to_owned());
        let commit = if start == "HEAD" && b.st.unborn {
            b.st.head.clone()
        } else {
            match r.commit_of(&start).await {
                Ok(c) => c,
                Err(e) => return rev_error(ui, &start, e),
            }
        };
        Destination {
            commit,
            head: Head::Branch(name.clone()),
            create: Some((name, reset)),
        }
    } else {
        let Some(target) = a.target.clone() else {
            return fatal(ui, "missing branch or commit argument");
        };
        match destination(&r, &root, &target, a.detach).await? {
            Ok(d) => {
                if d.head == Head::Detached && !a.detach && target != "-" {
                    fatal(ui, format!("a branch is expected, got '{target}'"))?;
                    hint(
                        ui,
                        "If you want to detach HEAD at the commit, try again with the --detach option.",
                    )?;
                    return Ok(128);
                }
                d
            }
            Err(e) => return fatal(ui, e),
        }
    };
    let shown = a.target.clone().unwrap_or_default();
    drop(r);
    switch_to(ui, cx, ws, b, dest, mode, a.quiet, &shown).await
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct CheckoutArgs {
    #[arg(short = 'b')]
    branch: Option<String>,
    #[arg(short = 'B')]
    force_branch: Option<String>,
    #[arg(long)]
    detach: bool,
    #[arg(short = 'f', long)]
    force: bool,
    #[arg(short = 'm', long)]
    merge: bool,
    #[arg(long)]
    conflict: Option<String>,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(long)]
    progress: bool,
    #[arg(short = 't', long, num_args = 0..=1, require_equals = true, default_missing_value = "direct")]
    track: Option<String>,
    #[arg(long = "no-track")]
    no_track: bool,
    #[arg(long)]
    guess: bool,
    #[arg(long = "no-guess")]
    no_guess: bool,
    #[arg(short = 'l')]
    reflog: bool,
    #[arg(long)]
    orphan: Option<String>,
    #[arg(long = "ignore-skip-worktree-bits")]
    ignore_skip_worktree_bits: bool,
    #[arg(long = "ignore-other-worktrees")]
    ignore_other_worktrees: bool,
    #[arg(long)]
    ours: bool,
    #[arg(long)]
    theirs: bool,
    #[arg(short = 'p', long)]
    patch: bool,
    #[arg(long = "overlay")]
    overlay: bool,
    #[arg(long = "no-overlay")]
    no_overlay: bool,
    #[arg(long = "recurse-submodules")]
    recurse_submodules: bool,
    #[arg(long = "pathspec-from-file")]
    pathspec_from_file: Option<String>,
    #[arg(long = "pathspec-file-nul")]
    pathspec_file_nul: bool,
    args: Vec<String>,
    #[arg(last = true)]
    paths: Vec<String>,
}

/// `git checkout`: a branch or commit (as `switch`, a commit detaching HEAD), or, with paths,
/// those paths as HEAD (or the commit named) has them — git's way to discard a file's changes.
pub async fn checkout(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: CheckoutArgs = parse_or_return!(ui, "checkout", args);
    if a.patch {
        fatal(ui, "git checkout --patch is not available in ok workspaces")?;
        hint(ui, "`jj restore -i` restores parts of files interactively")?;
        return Ok(128);
    }
    if a.orphan.is_some() {
        return fatal(
            ui,
            "an ok repository has one line of history: there are no orphan branches",
        );
    }
    if a.ours || a.theirs {
        fatal(
            ui,
            "ok keeps conflicts in the commit: resolve them in the files",
        )?;
        hint(ui, "`jj resolve` runs a merge tool on them")?;
        return Ok(128);
    }
    let ws = cx.ws(ui).await?;
    let b = before(&ws).await?;
    let root = ws.workspace_root().to_owned();
    let mode = if a.force {
        Carry::Discard
    } else if a.merge {
        Carry::Merge
    } else {
        Carry::Keep
    };
    let r = cx.resolver(&ws).await?;
    // Paths: after `--`, or arguments that are not revisions (git guesses the same way).
    let (rev, mut paths) = match a.args.split_first() {
        Some((first, rest)) if !a.paths.is_empty() || !rest.is_empty() => {
            if r.commit_of(first).await.is_ok() || first == "-" {
                (Some(first.clone()), rest.to_vec())
            } else {
                (None, a.args.clone())
            }
        }
        Some((first, _)) if a.branch.is_none() && a.force_branch.is_none() => {
            let is_rev = first == "-"
                || repo::branch_target(r.repo, first).is_some()
                || first == repo::MAIN
                || r.commit_of(first).await.is_ok();
            if is_rev {
                (Some(first.clone()), Vec::new())
            } else if cx.command.cwd().join(first).exists() {
                (None, vec![first.clone()])
            } else {
                return error(
                    ui,
                    format!("pathspec '{first}' did not match any file(s) known to git"),
                    1,
                );
            }
        }
        Some((first, _)) => (Some(first.clone()), Vec::new()),
        None => (None, Vec::new()),
    };
    paths.extend(a.paths.iter().cloned());
    if !paths.is_empty() {
        if a.branch.is_some() || a.force_branch.is_some() {
            return fatal(ui, "'-b' cannot be used with updating paths");
        }
        drop(r);
        return restore_from(
            ui,
            cx,
            ws,
            rev.as_deref(),
            &paths,
            "git checkout -- <paths>",
        )
        .await;
    }
    let create = a
        .branch
        .clone()
        .map(|n| (n, false))
        .or(a.force_branch.clone().map(|n| (n, true)));
    let dest = if let Some((name, reset)) = create {
        let start = rev.clone().unwrap_or_else(|| "HEAD".to_owned());
        let commit = if start == "HEAD" && b.st.unborn {
            b.st.head.clone()
        } else {
            match r.commit_of(&start).await {
                Ok(c) => c,
                Err(e) => return rev_error(ui, &start, e),
            }
        };
        Destination {
            commit,
            head: Head::Branch(name.clone()),
            create: Some((name, reset)),
        }
    } else {
        let Some(target) = rev.clone() else {
            // `git checkout` alone: nothing moves; git reports the working tree.
            return Ok(0);
        };
        match destination(&r, &root, &target, a.detach).await? {
            Ok(d) => d,
            Err(e) => {
                return error(
                    ui,
                    format!("pathspec '{target}' did not match any file(s) known to git ({e})"),
                    1,
                );
            }
        }
    };
    let shown = rev.unwrap_or_default();
    drop(r);
    switch_to(ui, cx, ws, b, dest, mode, a.quiet, &shown).await
}

// ---------------------------------------------------------------------------------------------
// stash

/// The trailer that marks a commit as a stash: a commit on the HEAD it was made at, holding the
/// working change it took, which nothing builds on.
const STASH_TRAILER: &str = "Git-Stash: yes";

/// Stashes, newest first (`stash@{0}` first).
pub async fn stashes(repo: &dyn Repo) -> Result<Vec<Commit>, CommandError> {
    let mut found = Vec::new();
    let wcs: HashSet<&CommitId> = repo.view().wc_commit_ids().values().collect();
    for id in repo.view().heads() {
        if wcs.contains(id) {
            continue;
        }
        let c = repo.store().get_commit_async(id).await?;
        if c.description().lines().any(|l| l.trim() == STASH_TRAILER) {
            found.push(c);
        }
    }
    found.sort_by(|x, y| {
        y.committer()
            .timestamp
            .timestamp
            .cmp(&x.committer().timestamp.timestamp)
            .then(y.id().cmp(x.id()))
    });
    Ok(found)
}

/// A stash's message, as `stash list` shows it: its first line.
fn stash_message(c: &Commit) -> String {
    c.description().lines().next().unwrap_or("").to_owned()
}

/// `stash@{n}`, `n`, or none (the newest).
fn stash_index(arg: Option<&str>) -> Option<usize> {
    let Some(a) = arg else { return Some(0) };
    let inner = a
        .strip_prefix("stash@{")
        .and_then(|r| r.strip_suffix('}'))
        .unwrap_or(a);
    inner.parse().ok()
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct StashPushArgs {
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
    #[arg(short = 'k', long = "keep-index")]
    keep_index: bool,
    #[arg(long = "no-keep-index")]
    no_keep_index: bool,
    #[arg(short = 'u', long = "include-untracked")]
    include_untracked: bool,
    #[arg(long = "no-include-untracked")]
    no_include_untracked: bool,
    #[arg(short = 'a', long)]
    all: bool,
    #[arg(short = 'S', long)]
    staged: bool,
    #[arg(short = 'p', long)]
    patch: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(long = "pathspec-from-file")]
    pathspec_from_file: Option<String>,
    #[arg(long = "pathspec-file-nul")]
    pathspec_file_nul: bool,
    paths: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct StashApplyArgs {
    #[arg(long)]
    index: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    stash: Option<String>,
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct StashShowArgs {
    #[arg(long = "include-untracked")]
    include_untracked: bool,
    #[arg(long = "only-untracked")]
    only_untracked: bool,
    #[command(flatten)]
    flags: inspect::DiffFlags,
    #[arg(short = 'z')]
    z: bool,
    stash: Option<String>,
}

/// `git stash`: the working change set aside as a commit on HEAD (marked with a trailer, so it is
/// in ok like any change, on every machine the workspace attaches to), and the working tree
/// returned to HEAD; `list`, `show`, `pop`, `apply`, `drop`, `clear`, and `branch`.
pub async fn stash(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let (sub, rest) = match args.split_first() {
        Some((s, rest)) if !s.starts_with('-') => (s.as_str(), rest.to_vec()),
        _ => ("push", args.to_vec()),
    };
    match sub {
        "push" | "save" => {
            let rest = if sub == "save" && !rest.is_empty() && !rest[0].starts_with('-') {
                vec!["-m".to_owned(), rest.join(" ")]
            } else {
                rest
            };
            stash_push(ui, cx, &rest).await
        }
        "list" => {
            let ws = cx.ws_read(ui).await?;
            let mut out = ui.stdout();
            for (i, s) in stashes(ws.repo().as_ref()).await?.iter().enumerate() {
                writeln!(out, "stash@{{{i}}}: {}", stash_message(s))?;
            }
            Ok(0)
        }
        "show" => {
            let a: StashShowArgs = parse_or_return!(ui, "stash show", &rest);
            let ws = cx.ws_read(ui).await?;
            let repo = ws.repo().as_ref();
            let all = stashes(repo).await?;
            let Some(s) = stash_index(a.stash.as_deref()).and_then(|i| all.get(i)) else {
                return fatal(ui, "no stash entries found.");
            };
            let mut show = match inspect::DiffShow::from_flags(&a.flags, false, a.z) {
                Ok(s) => s,
                Err(e) => return fatal(ui, e),
            };
            if !show.shows_anything() {
                show.stat = true;
            }
            let (from, to, copies) = inspect::commit_trees(repo, s, &EverythingMatcher).await?;
            ui.request_pager();
            let mut out = ui.stdout_formatter();
            inspect::show_diff(
                ui,
                cx,
                &ws,
                repo,
                out.as_mut(),
                (&from, &to),
                &copies,
                &EverythingMatcher,
                &show,
            )
            .await?;
            Ok(0)
        }
        "pop" | "apply" => {
            let a: StashApplyArgs = parse_or_return!(ui, &format!("stash {sub}"), &rest);
            stash_apply(ui, cx, a.stash.as_deref(), sub == "pop", a.quiet).await
        }
        "drop" => {
            let mut ws = cx.ws_read(ui).await?;
            let quiet = rest.iter().any(|r| r == "-q" || r == "--quiet");
            let which = rest
                .iter()
                .find(|r| !r.starts_with('-'))
                .map(String::as_str);
            let all = stashes(ws.repo().as_ref()).await?;
            let Some(i) = stash_index(which) else {
                return fatal(
                    ui,
                    format!("{} is not a stash reference", which.unwrap_or("")),
                );
            };
            let Some(s) = all.get(i).cloned() else {
                return error(ui, "No stash entries found.", 1);
            };
            let mut tx = ws.start_transaction();
            tx.repo_mut().record_abandoned_commit(&s);
            tx.repo_mut().rebase_descendants().await?;
            tx.finish(ui, "git stash drop").await?;
            if !quiet {
                writeln!(ui.stdout(), "Dropped stash@{{{i}}} ({})", s.id().hex())?;
            }
            Ok(0)
        }
        "clear" => {
            let mut ws = cx.ws_read(ui).await?;
            let all = stashes(ws.repo().as_ref()).await?;
            if all.is_empty() {
                return Ok(0);
            }
            let mut tx = ws.start_transaction();
            for s in &all {
                tx.repo_mut().record_abandoned_commit(s);
            }
            tx.repo_mut().rebase_descendants().await?;
            tx.finish(ui, "git stash clear").await?;
            Ok(0)
        }
        "branch" => {
            let Some(name) = rest.first().cloned() else {
                writeln!(
                    ui.stderr(),
                    "usage: git stash branch <branchname> [<stash>]"
                )?;
                return Ok(129);
            };
            let which = rest.get(1).cloned();
            let ws = cx.ws(ui).await?;
            let all = stashes(ws.repo().as_ref()).await?;
            let Some(s) = stash_index(which.as_deref())
                .and_then(|i| all.get(i))
                .cloned()
            else {
                return fatal(ui, "no stash entries found.");
            };
            let base = ws
                .repo()
                .store()
                .get_commit_async(&s.parent_ids()[0])
                .await?;
            let b = before(&ws).await?;
            let code = switch_to(
                ui,
                cx,
                ws,
                b,
                Destination {
                    commit: base,
                    head: Head::Branch(name.clone()),
                    create: Some((name.clone(), false)),
                },
                Carry::Keep,
                false,
                &name,
            )
            .await?;
            if code != 0 {
                return Ok(code);
            }
            stash_apply(ui, cx, which.as_deref(), true, false).await
        }
        "create" | "store" => fatal(
            ui,
            format!("git stash {sub} is not available in ok workspaces"),
        ),
        _ if sub.starts_with('-') => stash_push(ui, cx, args).await,
        _ => {
            writeln!(ui.stderr(), "error: unknown subcommand: `{sub}'")?;
            Ok(129)
        }
    }
}

async fn stash_push(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: StashPushArgs = parse_or_return!(ui, "stash push", args);
    if a.patch {
        fatal(ui, "git stash --patch is not available in ok workspaces")?;
        hint(ui, "`jj split -i` sets parts of a change apart")?;
        return Ok(128);
    }
    let mut ws = cx.ws(ui).await?;
    let b = before(&ws).await?;
    let repo = ws.repo().clone();
    let wc = b.st.wc.clone();
    let parent_tree = wc.parent_tree(repo.as_ref()).await?;
    let specs = all_paths(&a.paths, &a.rest);
    let matcher = match cx.pathspec(&ws, &specs) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let taken = if matcher.is_everything() {
        wc.tree()
    } else {
        restore_tree(
            &wc.tree(),
            &parent_tree,
            "the working copy".to_owned(),
            "HEAD".to_owned(),
            &matcher,
        )
        .await?
    };
    if taken.tree_ids() == parent_tree.tree_ids() {
        if !a.quiet {
            writeln!(ui.stdout(), "No local changes to save")?;
        }
        return Ok(0);
    }
    let place = match &b.head {
        Head::Branch(br) => br.clone(),
        Head::Detached => "(no branch)".to_owned(),
    };
    let first = match &a.message {
        Some(m) => format!("On {place}: {m}"),
        None => format!(
            "WIP on {place}: {} {}",
            repo::short(repo.as_ref(), b.st.head.id()).await?,
            fmt::subject(b.st.head.description())
        ),
    };
    let left = if a.keep_index {
        wc.tree()
    } else if matcher.is_everything() {
        parent_tree.clone()
    } else {
        restore_tree(
            &parent_tree,
            &wc.tree(),
            "HEAD".to_owned(),
            "the working copy".to_owned(),
            &matcher,
        )
        .await?
    };
    let mut tx = ws.start_transaction();
    tx.repo_mut()
        .new_commit(wc.parent_ids().to_vec(), taken)
        .set_description(format!("{first}\n\n{STASH_TRAILER}\n"))
        .write()
        .await?;
    tx.repo_mut()
        .rewrite_commit(&wc)
        .set_tree(left)
        .write()
        .await?;
    tx.repo_mut().rebase_descendants().await?;
    tx.finish(ui, "git stash").await?;
    if !a.quiet {
        writeln!(
            ui.stdout(),
            "Saved working directory and index state {first}"
        )?;
    }
    Ok(0)
}

async fn stash_apply(
    ui: &mut Ui,
    cx: &Cx<'_>,
    which: Option<&str>,
    pop: bool,
    quiet: bool,
) -> Code {
    let mut ws = cx.ws(ui).await?;
    let b = before(&ws).await?;
    let repo = ws.repo().clone();
    let all = stashes(repo.as_ref()).await?;
    let Some(i) = stash_index(which) else {
        return fatal(
            ui,
            format!("{} is not a stash reference", which.unwrap_or("")),
        );
    };
    let Some(s) = all.get(i).cloned() else {
        return error(ui, "No stash entries found.", 1);
    };
    let wc = b.st.wc.clone();
    let stash_base = s.parent_tree(repo.as_ref()).await?;
    let wc_base = wc.parent_tree(repo.as_ref()).await?;
    let merged = MergedTree::merge(Merge::from_vec(vec![
        (wc.tree(), "the working copy".to_owned()),
        (stash_base, "the stash's base".to_owned()),
        (s.tree(), "the stash".to_owned()),
    ]))
    .await?;
    let mut conflicted = Vec::new();
    for (path, _) in merged.conflicts() {
        if wc.tree().path_value(&path).await?.is_resolved() {
            conflicted.push(path);
        }
    }
    if !conflicted.is_empty() {
        // git refuses where the working tree's own changes are in the way.
        let mut in_the_way = Vec::new();
        for p in &conflicted {
            if wc.tree().path_value(p).await? != wc_base.path_value(p).await? {
                in_the_way.push(p.clone());
            }
        }
        if !in_the_way.is_empty() {
            return refuse_overwrite(ui, cx, &ws, "merge", &in_the_way);
        }
    }
    let mut tx = ws.start_transaction();
    tx.repo_mut()
        .rewrite_commit(&wc)
        .set_tree(merged)
        .write()
        .await?;
    let dropped = pop && conflicted.is_empty();
    if dropped {
        tx.repo_mut().record_abandoned_commit(&s);
    }
    tx.repo_mut().rebase_descendants().await?;
    tx.finish(
        ui,
        if pop {
            "git stash pop"
        } else {
            "git stash apply"
        },
    )
    .await?;
    let _ = b;
    if !conflicted.is_empty() {
        let converter = inspect::cwd_converter(cx, &ws);
        for p in &conflicted {
            writeln!(
                ui.stdout(),
                "CONFLICT (content): Merge conflict in {}",
                converter.format_file_path(p)
            )?;
        }
        if pop {
            writeln!(
                ui.stdout(),
                "The stash entry is kept in case you need it again."
            )?;
        }
        return Ok(1);
    }
    if !quiet {
        inspect::status(ui, cx, &[]).await?;
        if dropped {
            writeln!(ui.stdout(), "Dropped stash@{{{i}}} ({})", s.id().hex())?;
        }
    }
    Ok(0)
}

// ---------------------------------------------------------------------------------------------
// clean

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct CleanArgs {
    #[arg(short = 'd')]
    d: bool,
    #[arg(short = 'f', long, action = clap::ArgAction::Count)]
    force: u8,
    #[arg(short = 'i', long)]
    interactive: bool,
    #[arg(short = 'n', long = "dry-run")]
    dry_run: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'e', long = "exclude")]
    exclude: Vec<String>,
    #[arg(short = 'x')]
    x: bool,
    #[arg(short = 'X')]
    only_ignored: bool,
    paths: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// `git clean`: an ok workspace has no untracked files — every file is tracked unless the ignore
/// files name it — so only `-x` and `-X` find anything: ignored files (and, with `-d`, ignored
/// directories) that nothing tracks. Redirections (`.ok/redirections`) are left alone.
pub async fn clean(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: CleanArgs = parse_or_return!(ui, "clean", args);
    if a.interactive {
        return fatal(
            ui,
            "git clean --interactive is not available in ok workspaces",
        );
    }
    if a.force == 0 && !a.dry_run {
        return fatal(
            ui,
            "clean.requireForce is true and -f not given: refusing to clean",
        );
    }
    if !a.x && !a.only_ignored {
        return Ok(0);
    }
    let ws = cx.ws(ui).await?;
    let st = repo::state(&ws).await?;
    let root = ws.workspace_root().to_owned();
    let base = ws.base_ignores().map_err(internal_error)?;
    let ignores = ok_working_copy::ignores(&root, &base, &jj_lib::matchers::NothingMatcher)
        .map_err(internal_error)?;
    let redirections: HashSet<String> =
        std::fs::read_to_string(root.join(".ok").join("redirections"))
            .unwrap_or_default()
            .lines()
            .map(|l| {
                l.split('#')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .trim_matches('/')
                    .to_owned()
            })
            .filter(|l| !l.is_empty())
            .collect();
    let specs = all_paths(&a.paths, &a.rest);
    let matcher = match cx.pathspec(&ws, &specs) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let tree = st.wc.tree();
    let mut found: Vec<(RepoPathBuf, bool)> = Vec::new();
    let mut stack = vec![RepoPathBuf::root()];
    while let Some(dir) = stack.pop() {
        let fs_dir = dir.to_fs_path_unchecked(&root);
        let Ok(entries) = std::fs::read_dir(&fs_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if dir.is_root() && name == ".jj" {
                continue;
            }
            let Ok(path) = RepoPathBuf::from_internal_string(if dir.is_root() {
                name.to_owned()
            } else {
                format!("{}/{name}", dir.as_internal_file_string())
            }) else {
                continue;
            };
            if redirections.contains(path.as_internal_file_string()) {
                continue;
            }
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                let tracked_below = !tree.path_value(&path).await?.is_absent();
                if ignores.matches_dir(&path) && !tracked_below {
                    if a.d && matcher.matches(&path) {
                        found.push((path, true));
                    }
                } else {
                    stack.push(path);
                }
            } else if ignores.matches_file(&path)
                && tree.path_value(&path).await?.is_absent()
                && matcher.matches(&path)
            {
                found.push((path, false));
            }
        }
    }
    found.sort();
    let converter = inspect::cwd_converter(cx, &ws);
    let excludes: Vec<globset::GlobMatcher> = a
        .exclude
        .iter()
        .filter_map(|e| globset::Glob::new(e).ok().map(|g| g.compile_matcher()))
        .collect();
    let mut out = ui.stdout();
    for (path, is_dir) in found {
        let internal = path.as_internal_file_string();
        let base_name = internal.rsplit('/').next().unwrap_or(internal);
        if excludes
            .iter()
            .any(|g| g.is_match(base_name) || g.is_match(internal))
        {
            continue;
        }
        let shown = format!(
            "{}{}",
            converter.format_file_path(&path),
            if is_dir { "/" } else { "" }
        );
        if a.dry_run {
            writeln!(out, "Would remove {shown}")?;
            continue;
        }
        let fs = path.to_fs_path_unchecked(&root);
        let removed = if is_dir {
            std::fs::remove_dir_all(&fs)
        } else {
            std::fs::remove_file(&fs)
        };
        match removed {
            Ok(()) => {
                if !a.quiet {
                    writeln!(out, "Removing {shown}")?;
                }
            }
            Err(e) => writeln!(ui.stderr(), "warning: failed to remove {shown}: {e}")?,
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn args() {
        AddArgs::command().debug_assert();
        RmArgs::command().debug_assert();
        MvArgs::command().debug_assert();
        RestoreArgs::command().debug_assert();
        CommitArgs::command().debug_assert();
        ResetArgs::command().debug_assert();
        SwitchArgs::command().debug_assert();
        CheckoutArgs::command().debug_assert();
        StashPushArgs::command().debug_assert();
        StashApplyArgs::command().debug_assert();
        StashShowArgs::command().debug_assert();
        CleanArgs::command().debug_assert();
    }

    #[test]
    fn cleanups() {
        assert_eq!(
            cleanup("  \n\nfirst  \n\n\n\nsecond\n# comment\n\n", true),
            "first\n\nsecond\n"
        );
        assert_eq!(cleanup("# only a comment\n", true), "");
        assert_eq!(
            cleanup("keep # this\n#hash kept\n", false),
            "keep # this\n#hash kept\n"
        );
    }

    #[test]
    fn trailers() {
        assert_eq!(
            add_trailer("subject\n", "Signed-off-by: A <a@x>"),
            "subject\n\nSigned-off-by: A <a@x>\n"
        );
        assert_eq!(
            add_trailer("subject\n\nBug: 1\n", "Signed-off-by: A <a@x>"),
            "subject\n\nBug: 1\nSigned-off-by: A <a@x>\n"
        );
        assert_eq!(
            add_trailer(
                "subject\n\nSigned-off-by: A <a@x>\n",
                "Signed-off-by: A <a@x>"
            ),
            "subject\n\nSigned-off-by: A <a@x>\n"
        );
    }

    #[test]
    fn idents() {
        assert_eq!(
            parse_ident("Ada Lovelace <ada@example.com>"),
            Some(("Ada Lovelace".to_owned(), "ada@example.com".to_owned()))
        );
        assert_eq!(parse_ident("nobody"), None);
    }

    #[test]
    fn stash_refs() {
        assert_eq!(stash_index(None), Some(0));
        assert_eq!(stash_index(Some("stash@{2}")), Some(2));
        assert_eq!(stash_index(Some("1")), Some(1));
        assert_eq!(stash_index(Some("nope")), None);
    }
}
