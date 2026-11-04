//! git's commands about names: `rev-parse`, `branch`, `tag`, `show-ref`, `for-each-ref`,
//! `symbolic-ref`, `merge-base`, `describe`, `name-rev`, and `config`; and `update-index`,
//! `submodule`, `worktree`, and `help ok`.

use std::collections::HashMap;
use std::io::Write as _;

use futures::StreamExt as _;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::internal_error;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::commit::Commit;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RefTarget;
use jj_lib::ref_name::RefName;
use jj_lib::repo::Repo;
use jj_lib::revset::RevsetExpression;

use super::Code;
use super::Cx;
use super::change;
use super::error;
use super::fatal;
use super::fmt;
use super::hint;
use super::inspect;
use super::parse_or_return;
use super::repo;
use super::repo::Head;
use super::repo::Object;
use super::rev_error;

/// git's name for a ref, from its kind and short name.
fn full_ref(kind: RefKind, name: &str) -> String {
    match kind {
        RefKind::Branch => format!("refs/heads/{name}"),
        RefKind::Remote => format!("refs/remotes/{}/{name}", repo::ORIGIN),
        RefKind::Tag => format!("refs/tags/{name}"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefKind {
    Branch,
    Remote,
    Tag,
}

/// Every ref git would list: branches (with `main` while there is no local one, it being
/// `origin/main`'s, as git's own clone makes it), `origin/…`, and tags, by full name.
fn all_refs(repo: &dyn Repo, with_virtual_main: bool) -> Vec<(RefKind, String, CommitId)> {
    let root = repo.store().root_commit_id().clone();
    let mut out = Vec::new();
    let branches = repo::branches(repo);
    for (name, id) in &branches {
        if let Some(id) = id {
            out.push((RefKind::Branch, name.clone(), id.clone()));
        }
    }
    if with_virtual_main
        && !branches.iter().any(|(n, _)| n == repo::MAIN)
        && let Some(m) = repo::origin_main(repo).filter(|m| *m != root)
    {
        out.push((RefKind::Branch, repo::MAIN.to_owned(), m));
    }
    for (name, id) in repo::remote_branches(repo) {
        if id != root {
            out.push((RefKind::Remote, name, id));
        }
    }
    for (name, id) in repo::tags(repo) {
        if let Some(id) = id {
            out.push((RefKind::Tag, name, id));
        }
    }
    out.sort_by_key(|r| full_ref(r.0, &r.1));
    out
}

// ---------------------------------------------------------------------------------------------
// rev-parse

/// `git rev-parse`: options and revisions, read in order as git reads them.
pub async fn rev_parse(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    if args.iter().any(|a| a == "-h" || a == "--help") {
        writeln!(ui.stdout(), "usage: git rev-parse [<options>] <args>...")?;
        return Ok(0);
    }
    let ws = cx.ws_read(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let root = ws.workspace_root().to_owned();
    let cwd = cx.command.cwd().to_owned();
    let head = repo::current(repo, &root, st.head.id());
    let r = cx.resolver(&ws).await?;
    let prefix = cwd
        .strip_prefix(&root)
        .map(|p| p.to_string_lossy().replace('\\', "/"))
        .unwrap_or_default();
    let jj_dir = root.join(".jj");
    // Options apply to the revisions after them, as git reads them: output keeps their order.
    enum Item {
        Text(String),
        Rev {
            spec: String,
            short: Option<usize>,
            abbrev_ref: bool,
            symbolic_full: bool,
            symbolic: bool,
        },
    }
    let verify = args.iter().any(|a| a == "--verify");
    let quiet = args.iter().any(|a| a == "-q" || a == "--quiet");
    let mut short: Option<usize> = None;
    let mut abbrev_ref = false;
    let mut symbolic_full = false;
    let mut symbolic = false;
    let mut default: Option<String> = None;
    let mut items: Vec<Item> = Vec::new();
    let mut paths_mode = false;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        i += 1;
        let text = |s: String| Item::Text(s);
        if paths_mode {
            items.push(text(arg.to_owned()));
            continue;
        }
        match arg {
            "--" => {
                paths_mode = true;
                if !verify {
                    items.push(text("--".to_owned()));
                }
            }
            "--show-toplevel" => items.push(text(root.display().to_string())),
            "--show-prefix" => items.push(text(if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            })),
            "--show-cdup" => items.push(text(
                "../".repeat(prefix.split('/').filter(|s| !s.is_empty()).count()),
            )),
            "--git-dir" => items.push(text(if cwd == root {
                ".jj".to_owned()
            } else {
                jj_dir.display().to_string()
            })),
            "--absolute-git-dir" | "--git-common-dir" => {
                items.push(text(jj_dir.display().to_string()))
            }
            "--is-inside-work-tree" => items.push(text("true".to_owned())),
            "--is-inside-git-dir" | "--is-bare-repository" | "--is-shallow-repository" => {
                items.push(text("false".to_owned()))
            }
            "--show-superproject-working-tree" | "--local-env-vars" | "--shared-index-path" => {}
            "--show-object-format"
            | "--show-object-format=storage"
            | "--show-object-format=input"
            | "--show-object-format=output" => items.push(text("blake3".to_owned())),
            "--show-ref-format" => items.push(text("files".to_owned())),
            "--verify" | "-q" | "--quiet" => {}
            "--short" => short = Some(7),
            "--no-short" => short = None,
            "--abbrev-ref" | "--abbrev-ref=strict" | "--abbrev-ref=loose" => abbrev_ref = true,
            "--symbolic-full-name" => symbolic_full = true,
            "--symbolic" => symbolic = true,
            "--sq" | "--sq-quote" | "--revs-only" | "--no-revs" | "--flags" | "--no-flags"
            | "--not" => {}
            "--default" => {
                default = args.get(i).cloned();
                i += 1;
            }
            "--git-path" => {
                if let Some(p) = args.get(i) {
                    items.push(text(jj_dir.join(p).display().to_string()));
                }
                i += 1;
            }
            "--all" | "--branches" | "--tags" | "--remotes" => {
                for (kind, _, id) in all_refs(repo, false) {
                    let wanted = match arg {
                        "--branches" => kind == RefKind::Branch,
                        "--tags" => kind == RefKind::Tag,
                        "--remotes" => kind == RefKind::Remote,
                        _ => true,
                    };
                    if wanted {
                        items.push(text(id.hex()));
                    }
                }
            }
            "--parseopt" | "--stuck-long" | "--keep-dashdash" | "--stop-at-non-option" => {
                return fatal(
                    ui,
                    "git rev-parse --parseopt is not available in ok workspaces",
                );
            }
            _ if arg.starts_with("--short=") => {
                short = arg["--short=".len()..]
                    .parse()
                    .ok()
                    .map(|n: usize| n.max(4));
            }
            _ if arg.starts_with("--default=") => {
                default = Some(arg["--default=".len()..].to_owned())
            }
            _ if arg.starts_with("--since=") || arg.starts_with("--after=") => {
                if let Some(t) = inspect::approxidate(arg.split_once('=').unwrap().1) {
                    items.push(text(format!("--max-age={t}")));
                }
            }
            _ if arg.starts_with("--until=") || arg.starts_with("--before=") => {
                if let Some(t) = inspect::approxidate(arg.split_once('=').unwrap().1) {
                    items.push(text(format!("--min-age={t}")));
                }
            }
            _ if arg.starts_with("--") => {}
            _ => items.push(Item::Rev {
                spec: arg.to_owned(),
                short,
                abbrev_ref,
                symbolic_full,
                symbolic,
            }),
        }
    }
    let revs = items
        .iter()
        .filter(|i| matches!(i, Item::Rev { .. }))
        .count();
    if revs == 0
        && let Some(d) = default
    {
        items.push(Item::Rev {
            spec: d,
            short,
            abbrev_ref,
            symbolic_full,
            symbolic,
        });
    }
    let revs = items
        .iter()
        .filter(|i| matches!(i, Item::Rev { .. }))
        .count();
    if verify && revs != 1 {
        if quiet {
            return Ok(1);
        }
        return fatal(ui, "Needed a single revision");
    }
    let mut out: Vec<String> = Vec::new();
    for item in items {
        let (spec, short, abbrev_ref, symbolic_full, symbolic) = match item {
            Item::Text(t) => {
                out.push(t);
                continue;
            }
            Item::Rev {
                spec,
                short,
                abbrev_ref,
                symbolic_full,
                symbolic,
            } => (spec, short, abbrev_ref, symbolic_full, symbolic),
        };
        // Ranges and negations, as git prints them.
        if !verify {
            if let Some((x, y)) = spec.split_once("...") {
                let (x, y) = (
                    if x.is_empty() { "HEAD" } else { x },
                    if y.is_empty() { "HEAD" } else { y },
                );
                match (r.commit_of(x).await, r.commit_of(y).await) {
                    (Ok(cx_), Ok(cy)) => {
                        out.push(cy.id().hex());
                        out.push(cx_.id().hex());
                        if let Some(base) =
                            inspect::merge_base(repo, &[cx_.id().clone(), cy.id().clone()]).await?
                        {
                            out.push(format!("^{}", base.id().hex()));
                        }
                        continue;
                    }
                    (Err(e), _) => return flush_then(ui, &out, rev_error(ui, x, e)),
                    (_, Err(e)) => return flush_then(ui, &out, rev_error(ui, y, e)),
                }
            }
            if let Some((x, y)) = spec.split_once("..") {
                let (x, y) = (
                    if x.is_empty() { "HEAD" } else { x },
                    if y.is_empty() { "HEAD" } else { y },
                );
                match (r.commit_of(x).await, r.commit_of(y).await) {
                    (Ok(cx_), Ok(cy)) => {
                        out.push(cy.id().hex());
                        out.push(format!("^{}", cx_.id().hex()));
                        continue;
                    }
                    (Err(e), _) => return flush_then(ui, &out, rev_error(ui, x, e)),
                    (_, Err(e)) => return flush_then(ui, &out, rev_error(ui, y, e)),
                }
            }
        }
        let (neg, name) = match spec.strip_prefix('^') {
            Some(n) if !verify => ("^", n),
            _ => ("", spec.as_str()),
        };
        if abbrev_ref || symbolic_full {
            match symbolic_name(&r, &head, name, symbolic_full).await {
                Some(s) => {
                    out.push(format!("{neg}{s}"));
                    continue;
                }
                None if r.object(name).await.is_ok() => {
                    if abbrev_ref {
                        out.push(format!("{neg}{name}"));
                    }
                    continue;
                }
                None => {}
            }
        }
        if symbolic {
            out.push(spec.clone());
            continue;
        }
        match r.object(name).await {
            Ok(obj) => {
                let hex = inspect::object_id_hex(&obj);
                let shown = match (short, &obj) {
                    (Some(n), Object::Commit(c)) => {
                        let s = repo::short(repo, c.id()).await?;
                        if s.len() < n {
                            hex[..n.min(hex.len())].to_owned()
                        } else {
                            s
                        }
                    }
                    (Some(n), _) => hex[..n.min(hex.len())].to_owned(),
                    (None, _) => hex,
                };
                out.push(format!("{neg}{shown}"));
            }
            Err(repo::RevError::Command(e)) => return Err(e),
            Err(e) => {
                if verify {
                    if quiet {
                        return Ok(1);
                    }
                    flush(ui, &out)?;
                    return fatal(ui, "Needed a single revision");
                }
                out.push(spec.clone());
                return flush_then(ui, &out, rev_error(ui, &spec, e));
            }
        }
    }
    flush(ui, &out)?;
    Ok(0)
}

fn flush(ui: &Ui, lines: &[String]) -> Result<(), CommandError> {
    let mut o = ui.stdout();
    for l in lines {
        writeln!(o, "{l}")?;
    }
    Ok(())
}

fn flush_then(ui: &Ui, lines: &[String], then: Code) -> Code {
    flush(ui, lines)?;
    then
}

/// A revision's name as `--abbrev-ref` (`full`: `--symbolic-full-name`) gives it: HEAD's
/// branch, `origin/main` for an upstream, a branch's own name.
async fn symbolic_name(
    r: &repo::Resolver<'_>,
    head: &Head,
    name: &str,
    full: bool,
) -> Option<String> {
    let upstream = |full: bool| {
        if full {
            format!("refs/remotes/{}/{}", repo::ORIGIN, repo::MAIN)
        } else {
            format!("{}/{}", repo::ORIGIN, repo::MAIN)
        }
    };
    let lower = name.to_ascii_lowercase();
    if lower.ends_with("@{u}") || lower.ends_with("@{upstream}") || lower.ends_with("@{push}") {
        return Some(upstream(full));
    }
    if name == "HEAD" || name == "@" {
        return Some(match head {
            Head::Branch(b) if full => format!("refs/heads/{b}"),
            Head::Branch(b) => b.clone(),
            Head::Detached => "HEAD".to_owned(),
        });
    }
    if repo::branch_target(r.repo, name).is_some() || name == repo::MAIN {
        return Some(if full {
            format!("refs/heads/{name}")
        } else {
            name.to_owned()
        });
    }
    if let Some(b) = name.strip_prefix(&format!("{}/", repo::ORIGIN)) {
        return Some(if full {
            format!("refs/remotes/{}/{b}", repo::ORIGIN)
        } else {
            name.to_owned()
        });
    }
    None
}

// ---------------------------------------------------------------------------------------------
// branch, tag

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct BranchArgs {
    #[arg(short = 'd', long)]
    delete: bool,
    #[arg(short = 'D')]
    force_delete: bool,
    #[arg(short = 'm', long = "move")]
    move_: bool,
    #[arg(short = 'M')]
    force_move: bool,
    #[arg(short = 'c', long = "copy")]
    copy: bool,
    #[arg(short = 'C')]
    force_copy: bool,
    #[arg(short = 'f', long)]
    force: bool,
    #[arg(short = 'l', long)]
    list: bool,
    #[arg(short = 'a', long)]
    all: bool,
    #[arg(short = 'r', long)]
    remotes: bool,
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(long = "show-current")]
    show_current: bool,
    #[arg(long)]
    contains: Option<String>,
    #[arg(long = "no-contains")]
    no_contains: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "HEAD")]
    merged: Option<String>,
    #[arg(long = "no-merged", num_args = 0..=1, require_equals = true, default_missing_value = "HEAD")]
    no_merged: Option<String>,
    #[arg(long = "points-at")]
    points_at: Option<String>,
    #[arg(long)]
    format: Option<String>,
    #[arg(long)]
    sort: Vec<String>,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    color: Option<String>,
    #[arg(long = "no-color")]
    no_color: bool,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    column: Option<String>,
    #[arg(long = "no-column")]
    no_column: bool,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    abbrev: Option<String>,
    #[arg(long = "no-abbrev")]
    no_abbrev: bool,
    #[arg(short = 'i', long = "ignore-case")]
    ignore_case: bool,
    #[arg(short = 't', long, num_args = 0..=1, require_equals = true, default_missing_value = "direct")]
    track: Option<String>,
    #[arg(long = "no-track")]
    no_track: bool,
    #[arg(short = 'u', long = "set-upstream-to")]
    set_upstream_to: Option<String>,
    #[arg(long = "unset-upstream")]
    unset_upstream: bool,
    #[arg(long = "edit-description")]
    edit_description: bool,
    #[arg(long = "create-reflog")]
    create_reflog: bool,
    #[arg(long = "recurse-submodules")]
    recurse_submodules: bool,
    #[arg(long = "omit-empty")]
    omit_empty: bool,
    args: Vec<String>,
}

/// `git branch`: local bookmarks. Listing marks the one HEAD is on (`main`, while there is no
/// local one, being `origin/main`); `-r` lists `origin/…`; creating, deleting, renaming, and
/// copying move bookmarks. Every branch's upstream is `origin/main`.
pub async fn branch(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: BranchArgs = parse_or_return!(ui, "branch", args);
    if a.edit_description {
        return fatal(
            ui,
            "ok keeps no branch descriptions: a change's description is its own",
        );
    }
    let mut ws = cx.ws_read(ui).await?;
    let repo = ws.repo().clone();
    let st = repo::state(&ws).await?;
    let root = ws.workspace_root().to_owned();
    let head = repo::current(repo.as_ref(), &root, st.head.id());
    if a.show_current {
        if let Head::Branch(b) = &head {
            writeln!(ui.stdout(), "{b}")?;
        }
        return Ok(0);
    }
    if a.set_upstream_to.is_some() || a.unset_upstream {
        // Every branch's upstream is origin/main.
        return Ok(0);
    }
    let r = cx.resolver(&ws).await?;
    if a.delete || a.force_delete {
        if a.args.is_empty() {
            return fatal(ui, "branch name required");
        }
        let force = a.force_delete || a.force;
        let up = repo::origin_main(repo.as_ref());
        let mut deleted = Vec::new();
        for name in &a.args {
            let Some(id) = repo::branch_target(repo.as_ref(), name) else {
                return error(ui, format!("branch '{name}' not found"), 1);
            };
            if head.branch() == Some(name.as_str())
                && repo::branch_target(repo.as_ref(), name).as_ref() == Some(st.head.id())
            {
                return error(
                    ui,
                    format!(
                        "cannot delete branch '{name}' checked out at '{}'",
                        root.display()
                    ),
                    1,
                );
            }
            if !force {
                let merged = repo::is_ancestor(repo.as_ref(), &id, st.head.id()).await?
                    || match &up {
                        Some(u) => repo::is_ancestor(repo.as_ref(), &id, u).await?,
                        None => false,
                    };
                if !merged {
                    error(ui, format!("the branch '{name}' is not fully merged"), 1)?;
                    hint(
                        ui,
                        format!(
                            "If you are sure you want to delete it, run 'git branch -D {name}'"
                        ),
                    )?;
                    return Ok(1);
                }
            }
            deleted.push((name.clone(), id));
        }
        drop(r);
        let mut tx = ws.start_transaction();
        for (name, _) in &deleted {
            tx.repo_mut()
                .set_local_bookmark_target(RefName::new(name), RefTarget::absent());
        }
        let mut lines = Vec::new();
        for (name, id) in &deleted {
            lines.push(format!(
                "Deleted branch {name} (was {}).",
                repo::short(tx.repo(), id).await?
            ));
        }
        tx.finish(ui, format!("git branch -d {}", a.args.join(" ")))
            .await?;
        if !a.quiet {
            for l in lines {
                writeln!(ui.stdout(), "{l}")?;
            }
        }
        return Ok(0);
    }
    if a.move_ || a.force_move || a.copy || a.force_copy {
        let (old, new) = match a.args.as_slice() {
            [new] => match &head {
                Head::Branch(b) => (b.clone(), new.clone()),
                Head::Detached => {
                    return fatal(ui, "cannot rename the current branch while not on any");
                }
            },
            [old, new] => (old.clone(), new.clone()),
            _ => return fatal(ui, "too many arguments"),
        };
        let force = a.force_move || a.force_copy || a.force;
        let from = match repo::branch_target(repo.as_ref(), &old) {
            Some(id) => id,
            None if old == repo::MAIN => repo::origin_main(repo.as_ref())
                .unwrap_or_else(|| repo.store().root_commit_id().clone()),
            None => return error(ui, format!("no branch named '{old}'"), 1),
        };
        if repo::branch_target(repo.as_ref(), &new).is_some() && !force && old != new {
            return fatal(ui, format!("a branch named '{new}' already exists"));
        }
        if let Err(e) = check_ref_name(&new) {
            return fatal(ui, e);
        }
        drop(r);
        let moving = a.move_ || a.force_move;
        let mut tx = ws.start_transaction();
        tx.repo_mut()
            .set_local_bookmark_target(RefName::new(&new), RefTarget::normal(from.clone()));
        if moving && old != new {
            tx.repo_mut()
                .set_local_bookmark_target(RefName::new(&old), RefTarget::absent());
        }
        tx.finish(
            ui,
            format!(
                "git branch {} {old} {new}",
                if moving { "-m" } else { "-c" }
            ),
        )
        .await?;
        if moving && head.branch() == Some(old.as_str()) {
            let b = change::Before {
                st,
                head: head.clone(),
            };
            change::record(&ws, &b, &Head::Branch(new.clone()), b.st.head.id());
        }
        return Ok(0);
    }
    let listing = a.list
        || a.all
        || a.remotes
        || a.args.is_empty()
        || a.contains.is_some()
        || a.merged.is_some()
        || a.no_merged.is_some()
        || a.points_at.is_some();
    if !listing {
        let (name, start) = match a.args.as_slice() {
            [name] => (name.clone(), None),
            [name, start] => (name.clone(), Some(start.clone())),
            _ => return fatal(ui, "too many arguments"),
        };
        if let Err(e) = check_ref_name(&name) {
            return fatal(ui, e);
        }
        if repo::branch_target(repo.as_ref(), &name).is_some() && !a.force {
            return fatal(ui, format!("a branch named '{name}' already exists"));
        }
        let target = match &start {
            Some(s) => match r.commit_of(s).await {
                Ok(c) => c,
                Err(e) => return rev_error(ui, s, e),
            },
            None => {
                if st.unborn {
                    return fatal(
                        ui,
                        format!(
                            "not a valid object name: '{}'",
                            head.branch().unwrap_or("HEAD")
                        ),
                    );
                }
                st.head.clone()
            }
        };
        drop(r);
        let mut tx = ws.start_transaction();
        tx.repo_mut()
            .set_local_bookmark_target(RefName::new(&name), RefTarget::normal(target.id().clone()));
        tx.finish(ui, format!("git branch {name}")).await?;
        return Ok(0);
    }
    // Listing.
    let patterns: Vec<globset::GlobMatcher> = a
        .args
        .iter()
        .filter_map(|p| {
            globset::GlobBuilder::new(p)
                .case_insensitive(a.ignore_case)
                .build()
                .ok()
                .map(|g| g.compile_matcher())
        })
        .collect();
    let filter_commit = async |spec: &Option<String>| -> Result<Option<CommitId>, repo::RevError> {
        match spec {
            Some(s) => Ok(Some(r.commit_of(s).await?.id().clone())),
            None => Ok(None),
        }
    };
    let contains = match filter_commit(&a.contains).await {
        Ok(c) => c,
        Err(e) => return rev_error(ui, a.contains.as_deref().unwrap_or(""), e),
    };
    let no_contains = match filter_commit(&a.no_contains).await {
        Ok(c) => c,
        Err(e) => return rev_error(ui, a.no_contains.as_deref().unwrap_or(""), e),
    };
    let merged = match filter_commit(&a.merged).await {
        Ok(c) => c,
        Err(e) => return rev_error(ui, a.merged.as_deref().unwrap_or(""), e),
    };
    let no_merged = match filter_commit(&a.no_merged).await {
        Ok(c) => c,
        Err(e) => return rev_error(ui, a.no_merged.as_deref().unwrap_or(""), e),
    };
    let points_at = match filter_commit(&a.points_at).await {
        Ok(c) => c,
        Err(e) => return rev_error(ui, a.points_at.as_deref().unwrap_or(""), e),
    };
    let mut rows: Vec<(RefKind, String, CommitId)> = all_refs(repo.as_ref(), true)
        .into_iter()
        .filter(|(k, _, _)| match k {
            RefKind::Branch => !a.remotes || a.all,
            RefKind::Remote => a.remotes || a.all,
            RefKind::Tag => false,
        })
        .collect();
    let mut kept = Vec::new();
    for row in rows.drain(..) {
        let shown = match row.0 {
            RefKind::Remote => format!("{}/{}", repo::ORIGIN, row.1),
            _ => row.1.clone(),
        };
        if !patterns.is_empty() && !patterns.iter().any(|p| p.is_match(&shown)) {
            continue;
        }
        if let Some(c) = &contains
            && !repo::is_ancestor(repo.as_ref(), c, &row.2).await?
        {
            continue;
        }
        if let Some(c) = &no_contains
            && repo::is_ancestor(repo.as_ref(), c, &row.2).await?
        {
            continue;
        }
        if let Some(c) = &merged
            && !repo::is_ancestor(repo.as_ref(), &row.2, c).await?
        {
            continue;
        }
        if let Some(c) = &no_merged
            && repo::is_ancestor(repo.as_ref(), &row.2, c).await?
        {
            continue;
        }
        if let Some(c) = &points_at
            && row.2 != *c
        {
            continue;
        }
        kept.push((row, shown));
    }
    let width = kept
        .iter()
        .map(|(_, s)| s.chars().count() + if a.all { 8 } else { 0 })
        .max()
        .unwrap_or(0);
    let current = head.branch().map(str::to_owned);
    let detached_line = if head == Head::Detached && !a.remotes && !st.unborn {
        Some(format!(
            "(HEAD detached at {})",
            repo::short(repo.as_ref(), st.head.id()).await?
        ))
    } else {
        None
    };
    ui.request_pager();
    let mut out = ui.stdout_formatter();
    if let Some(line) = &detached_line {
        if a.format.is_some() {
            // A custom format lists refs only.
        } else if a.verbose > 0 {
            let c = repo.store().get_commit_async(st.head.id()).await?;
            writeln!(
                out,
                "* {line:<width$} {} {}",
                repo::short(repo.as_ref(), st.head.id()).await?,
                fmt::subject(c.description())
            )?;
        } else {
            writeln!(out, "* {line}")?;
        }
    }
    for ((kind, name, id), shown) in &kept {
        let is_current = *kind == RefKind::Branch && current.as_deref() == Some(name.as_str());
        let display = if a.all && *kind == RefKind::Remote {
            format!("remotes/{shown}")
        } else {
            shown.clone()
        };
        if let Some(f) = &a.format {
            let c = repo.store().get_commit_async(id).await?;
            let line = ref_format(repo.as_ref(), f, *kind, name, &c, is_current).await?;
            writeln!(out, "{line}")?;
            continue;
        }
        let marker = if is_current { "* " } else { "  " };
        if a.verbose == 0 {
            if is_current {
                writeln!(
                    jj_cli::formatter::FormatterExt::labeled(out.as_mut(), "bookmark"),
                    "{marker}{display}"
                )?;
            } else {
                writeln!(out, "{marker}{display}")?;
            }
            continue;
        }
        let c = repo.store().get_commit_async(id).await?;
        let mut track = String::new();
        if a.verbose > 1
            && *kind == RefKind::Branch
            && let Some((ahead, behind)) = inspect::tracking(repo.as_ref(), &c, false).await?
        {
            let up = format!("{}/{}", repo::ORIGIN, repo::MAIN);
            track = match (ahead, behind) {
                (0, 0) => format!("[{up}] "),
                (x, 0) => format!("[{up}: ahead {x}] "),
                (0, y) => format!("[{up}: behind {y}] "),
                (x, y) => format!("[{up}: ahead {x}, behind {y}] "),
            };
        }
        writeln!(
            out,
            "{marker}{display:<width$} {} {track}{}",
            repo::short(repo.as_ref(), id).await?,
            fmt::subject(c.description())
        )?;
    }
    Ok(0)
}

/// A branch or tag name git would take (`git check-ref-format`, simplified to what matters).
fn check_ref_name(name: &str) -> Result<(), String> {
    let bad = name.is_empty()
        || name.starts_with('-')
        || name.starts_with('/')
        || name.ends_with('/')
        || name.ends_with('.')
        || name.ends_with(".lock")
        || name.contains("..")
        || name.contains("//")
        || name.contains("@{")
        || name == "@"
        || name == "HEAD"
        || name
            .chars()
            .any(|c| c.is_control() || matches!(c, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\'));
    if bad {
        Err(format!("'{name}' is not a valid branch name"))
    } else {
        Ok(())
    }
}

/// `%(…)` fields of `for-each-ref` and `branch --format`.
async fn ref_format(
    repo: &dyn Repo,
    format: &str,
    kind: RefKind,
    name: &str,
    c: &Commit,
    is_head: bool,
) -> Result<String, CommandError> {
    let full = full_ref(kind, name);
    let short_name = match kind {
        RefKind::Remote => format!("{}/{name}", repo::ORIGIN),
        _ => name.to_owned(),
    };
    let mut out = String::new();
    let mut rest = format;
    while let Some(start) = rest.find("%(") {
        out.push_str(&rest[..start]);
        let Some(end) = rest[start..].find(')') else {
            out.push_str(&rest[start..]);
            rest = "";
            break;
        };
        let atom = &rest[start + 2..start + end];
        rest = &rest[start + end + 1..];
        let (field, modifier) = atom.split_once(':').unwrap_or((atom, ""));
        let track = async || -> Result<Option<(usize, usize)>, CommandError> {
            if kind == RefKind::Branch {
                inspect::tracking(repo, c, false).await
            } else {
                Ok(None)
            }
        };
        let value = match field {
            "refname" => match modifier {
                "short" => short_name.clone(),
                m if m.starts_with("lstrip=") || m.starts_with("strip=") => {
                    let n: usize = m.split_once('=').unwrap().1.parse().unwrap_or(0);
                    full.split('/').skip(n).collect::<Vec<_>>().join("/")
                }
                m if m.starts_with("rstrip=") => {
                    let n: usize = m.split_once('=').unwrap().1.parse().unwrap_or(0);
                    let parts: Vec<&str> = full.split('/').collect();
                    parts[..parts.len().saturating_sub(n)].join("/")
                }
                _ => full.clone(),
            },
            "objectname" => match modifier {
                "short" => repo::short(repo, c.id()).await?,
                m if m.starts_with("short=") => {
                    let n: usize = m["short=".len()..].parse().unwrap_or(7);
                    c.id().hex()[..n.min(64)].to_owned()
                }
                _ => c.id().hex(),
            },
            "objecttype" => "commit".to_owned(),
            "objectsize" => inspect::commit_text(repo, c).len().to_string(),
            "tree" => c
                .tree_ids()
                .as_resolved()
                .map(|t| t.hex())
                .unwrap_or_default(),
            "parent" => c
                .parent_ids()
                .iter()
                .filter(|p| *p != repo.store().root_commit_id())
                .map(|p| p.hex())
                .collect::<Vec<_>>()
                .join(" "),
            "HEAD" => {
                if is_head {
                    "*".to_owned()
                } else {
                    " ".to_owned()
                }
            }
            "upstream" | "push" => {
                if kind != RefKind::Branch {
                    String::new()
                } else {
                    match modifier {
                        "short" => format!("{}/{}", repo::ORIGIN, repo::MAIN),
                        "remotename" => repo::ORIGIN.to_owned(),
                        "remoteref" => format!("refs/heads/{}", repo::MAIN),
                        "track" | "track,nobracket" => match track().await? {
                            Some((0, 0)) | None => String::new(),
                            Some((x, 0)) => wrap(modifier, format!("ahead {x}")),
                            Some((0, y)) => wrap(modifier, format!("behind {y}")),
                            Some((x, y)) => wrap(modifier, format!("ahead {x}, behind {y}")),
                        },
                        "trackshort" => match track().await? {
                            Some((0, 0)) => "=".to_owned(),
                            Some((_, 0)) => ">".to_owned(),
                            Some((0, _)) => "<".to_owned(),
                            Some(_) => "<>".to_owned(),
                            None => String::new(),
                        },
                        _ => format!("refs/remotes/{}/{}", repo::ORIGIN, repo::MAIN),
                    }
                }
            }
            "subject" | "contents:subject" => fmt::subject(c.description()),
            "body" | "contents:body" => fmt::body(c.description()),
            "contents" => match modifier {
                "subject" => fmt::subject(c.description()),
                "body" => fmt::body(c.description()),
                _ => fmt::raw_message(c.description()),
            },
            "authorname" => c.author().name.clone(),
            "authoremail" => email(&c.author().email, modifier),
            "authordate" | "creatordate" => date_atom(&c.author().timestamp, modifier),
            "committername" => c.committer().name.clone(),
            "committeremail" => email(&c.committer().email, modifier),
            "committerdate" => date_atom(&c.committer().timestamp, modifier),
            "creator" | "committer" => format!(
                "{} {} {}",
                fmt::ident(c.committer()),
                fmt::unix(&c.committer().timestamp),
                fmt::zone(&c.committer().timestamp)
            ),
            "author" => format!(
                "{} {} {}",
                fmt::ident(c.author()),
                fmt::unix(&c.author().timestamp),
                fmt::zone(&c.author().timestamp)
            ),
            "symref" | "worktreepath" | "describe" | "signature" | "trailers" | "ahead-behind"
            | "is-base" | "deltabase" | "rest" => String::new(),
            _ => String::new(),
        };
        out.push_str(&value);
    }
    out.push_str(rest);
    Ok(out)
}

fn wrap(modifier: &str, s: String) -> String {
    if modifier.contains("nobracket") {
        s
    } else {
        format!("[{s}]")
    }
}

fn email(e: &str, modifier: &str) -> String {
    match modifier {
        "trim" => e.to_owned(),
        "localpart" => e.split('@').next().unwrap_or("").to_owned(),
        _ => format!("<{e}>"),
    }
}

fn date_atom(t: &jj_lib::backend::Timestamp, modifier: &str) -> String {
    let mode = if modifier.is_empty() {
        fmt::DateMode::Default { local: false }
    } else {
        fmt::DateMode::parse(modifier).unwrap_or(fmt::DateMode::Default { local: false })
    };
    fmt::date(t, &mode)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct TagArgs {
    #[arg(short = 'a', long)]
    annotate: bool,
    #[arg(short = 's', long)]
    sign: bool,
    #[arg(short = 'u', long = "local-user")]
    local_user: Option<String>,
    #[arg(short = 'f', long)]
    force: bool,
    #[arg(short = 'd', long)]
    delete: bool,
    #[arg(short = 'v', long)]
    verify: bool,
    #[arg(short = 'n', num_args = 0..=1, require_equals = false, default_missing_value = "1")]
    lines: Option<String>,
    #[arg(short = 'l', long)]
    list: bool,
    #[arg(long)]
    sort: Vec<String>,
    #[arg(short = 'm', long)]
    message: Vec<String>,
    #[arg(short = 'F', long = "file")]
    file: Option<String>,
    #[arg(long)]
    contains: Option<String>,
    #[arg(long = "no-contains")]
    no_contains: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "HEAD")]
    merged: Option<String>,
    #[arg(long = "no-merged", num_args = 0..=1, require_equals = true, default_missing_value = "HEAD")]
    no_merged: Option<String>,
    #[arg(long = "points-at")]
    points_at: Option<String>,
    #[arg(long)]
    format: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    color: Option<String>,
    #[arg(short = 'i', long = "ignore-case")]
    ignore_case: bool,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    column: Option<String>,
    #[arg(long = "no-column")]
    no_column: bool,
    #[arg(long = "create-reflog")]
    create_reflog: bool,
    #[arg(short = 'e', long)]
    edit: bool,
    #[arg(long)]
    cleanup: Option<String>,
    args: Vec<String>,
}

/// `git tag`: local tags — this workspace's names for commits, which ok does not share (work
/// moves between workspaces only as ChangeLists). An annotation has nowhere to go: it is
/// dropped, saying so.
pub async fn tag(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: TagArgs = parse_or_return!(ui, "tag", args);
    if a.verify {
        return fatal(ui, "ok keeps no signed tags to verify");
    }
    let mut ws = cx.ws_read(ui).await?;
    let repo = ws.repo().clone();
    if a.delete {
        let mut gone = Vec::new();
        for name in &a.args {
            match repo.view().get_local_tag(RefName::new(name)).as_normal() {
                Some(id) => gone.push((name.clone(), id.clone())),
                None => return error(ui, format!("tag '{name}' not found."), 1),
            }
        }
        let mut tx = ws.start_transaction();
        for (name, _) in &gone {
            tx.repo_mut()
                .set_local_tag_target(RefName::new(name), RefTarget::absent());
        }
        let mut lines = Vec::new();
        for (name, id) in &gone {
            lines.push(format!(
                "Deleted tag '{name}' (was {})",
                repo::short(tx.repo(), id).await?
            ));
        }
        tx.finish(ui, format!("git tag -d {}", a.args.join(" ")))
            .await?;
        for l in lines {
            writeln!(ui.stdout(), "{l}")?;
        }
        return Ok(0);
    }
    if a.list
        || a.args.is_empty()
        || a.contains.is_some()
        || a.points_at.is_some()
        || a.merged.is_some()
        || a.no_merged.is_some()
    {
        let r = cx.resolver(&ws).await?;
        let patterns: Vec<globset::GlobMatcher> = a
            .args
            .iter()
            .filter_map(|p| {
                globset::GlobBuilder::new(p)
                    .case_insensitive(a.ignore_case)
                    .build()
                    .ok()
                    .map(|g| g.compile_matcher())
            })
            .collect();
        let points = match &a.points_at {
            Some(p) => match r.commit_of(p).await {
                Ok(c) => Some(c.id().clone()),
                Err(e) => return rev_error(ui, p, e),
            },
            None => None,
        };
        let contains = match &a.contains {
            Some(p) => match r.commit_of(p).await {
                Ok(c) => Some(c.id().clone()),
                Err(e) => return rev_error(ui, p, e),
            },
            None => None,
        };
        let merged = match &a.merged {
            Some(p) => match r.commit_of(p).await {
                Ok(c) => Some(c.id().clone()),
                Err(e) => return rev_error(ui, p, e),
            },
            None => None,
        };
        let mut tags = repo::tags(repo.as_ref());
        tags.sort();
        let mut out = ui.stdout();
        for (name, id) in tags {
            let Some(id) = id else { continue };
            if !patterns.is_empty() && !patterns.iter().any(|p| p.is_match(&name)) {
                continue;
            }
            if points.as_ref().is_some_and(|p| *p != id) {
                continue;
            }
            if let Some(c) = &contains
                && !repo::is_ancestor(repo.as_ref(), c, &id).await?
            {
                continue;
            }
            if let Some(m) = &merged
                && !repo::is_ancestor(repo.as_ref(), &id, m).await?
            {
                continue;
            }
            match &a.format {
                Some(f) => {
                    let c = repo.store().get_commit_async(&id).await?;
                    writeln!(
                        out,
                        "{}",
                        ref_format(repo.as_ref(), f, RefKind::Tag, &name, &c, false).await?
                    )?;
                }
                None if a.lines.is_some() => {
                    let c = repo.store().get_commit_async(&id).await?;
                    writeln!(out, "{name:<15} {}", fmt::subject(c.description()))?;
                }
                None => writeln!(out, "{name}")?,
            }
        }
        return Ok(0);
    }
    let (name, target) = match a.args.as_slice() {
        [name] => (name.clone(), "HEAD".to_owned()),
        [name, target] => (name.clone(), target.clone()),
        _ => return fatal(ui, "too many arguments"),
    };
    if let Err(e) = check_ref_name(&name) {
        return fatal(ui, e.replace("branch", "tag"));
    }
    if repo.view().get_local_tag(RefName::new(&name)).is_present() && !a.force {
        return fatal(ui, format!("tag '{name}' already exists"));
    }
    let r = cx.resolver(&ws).await?;
    let commit = match r.commit_of(&target).await {
        Ok(c) => c,
        Err(e) => return rev_error(ui, &target, e),
    };
    drop(r);
    if a.annotate || !a.message.is_empty() || a.file.is_some() || a.sign {
        writeln!(
            ui.stderr(),
            "warning: ok keeps tags as names alone: the annotation is not kept"
        )?;
    }
    let mut tx = ws.start_transaction();
    tx.repo_mut()
        .set_local_tag_target(RefName::new(&name), RefTarget::normal(commit.id().clone()));
    tx.finish(ui, format!("git tag {name}")).await?;
    Ok(0)
}

// ---------------------------------------------------------------------------------------------
// show-ref, for-each-ref, symbolic-ref

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct ShowRefArgs {
    #[arg(long, visible_alias = "branches")]
    heads: bool,
    #[arg(long)]
    tags: bool,
    #[arg(long)]
    head: bool,
    #[arg(long)]
    verify: bool,
    #[arg(long)]
    exists: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 's', long = "hash", num_args = 0..=1, require_equals = true, default_missing_value = "")]
    hash: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "")]
    abbrev: Option<String>,
    #[arg(short = 'd', long)]
    dereference: bool,
    #[arg(long = "exclude-existing", num_args = 0..=1, require_equals = true)]
    exclude_existing: Option<String>,
    patterns: Vec<String>,
}

pub async fn show_ref(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: ShowRefArgs = parse_or_return!(ui, "show-ref", args);
    let ws = cx.ws_read(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let mut refs: Vec<(String, CommitId)> = all_refs(repo, false)
        .into_iter()
        .filter(|(k, _, _)| match k {
            RefKind::Branch => a.heads || !a.tags,
            RefKind::Tag => a.tags || !a.heads,
            RefKind::Remote => !a.heads && !a.tags,
        })
        .map(|(k, n, id)| (full_ref(k, &n), id))
        .collect();
    if (a.head || a.verify) && !st.unborn {
        refs.insert(0, ("HEAD".to_owned(), st.head.id().clone()));
    }
    let shown = async |id: &CommitId| -> Result<String, CommandError> {
        Ok(
            if a.abbrev.is_some() || a.hash.as_deref().is_some_and(|h| !h.is_empty()) {
                repo::short(repo, id).await?
            } else {
                id.hex()
            },
        )
    };
    let mut out = ui.stdout();
    if a.exists || a.verify {
        let mut code = 0;
        for p in &a.patterns {
            match refs.iter().find(|(n, _)| n == p) {
                Some((n, id)) => {
                    if a.exists || a.quiet {
                        continue;
                    }
                    if a.hash.is_some() {
                        writeln!(out, "{}", shown(id).await?)?;
                    } else {
                        writeln!(out, "{} {n}", shown(id).await?)?;
                    }
                }
                None => {
                    if a.exists {
                        return Ok(2);
                    }
                    if !a.quiet {
                        fatal(ui, format!("'{p}' - not a valid ref"))?;
                    }
                    code = if a.quiet { 1 } else { 128 };
                    break;
                }
            }
        }
        return Ok(code);
    }
    let mut any = false;
    for (name, id) in &refs {
        if !a.patterns.is_empty()
            && !a
                .patterns
                .iter()
                .any(|p| name == p || name.ends_with(&format!("/{p}")))
        {
            continue;
        }
        any = true;
        if a.quiet {
            continue;
        }
        if a.hash.is_some() {
            writeln!(out, "{}", shown(id).await?)?;
        } else {
            writeln!(out, "{} {name}", shown(id).await?)?;
        }
    }
    Ok(if any { 0 } else { 1 })
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct ForEachRefArgs {
    #[arg(long)]
    format: Option<String>,
    #[arg(long)]
    sort: Vec<String>,
    #[arg(long)]
    count: Option<usize>,
    #[arg(long = "points-at")]
    points_at: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "HEAD")]
    merged: Option<String>,
    #[arg(long = "no-merged", num_args = 0..=1, require_equals = true, default_missing_value = "HEAD")]
    no_merged: Option<String>,
    #[arg(long)]
    contains: Option<String>,
    #[arg(long = "no-contains")]
    no_contains: Option<String>,
    #[arg(long)]
    exclude: Vec<String>,
    #[arg(long = "ignore-case")]
    ignore_case: bool,
    #[arg(long = "omit-empty")]
    omit_empty: bool,
    #[arg(short = 's', long)]
    shell: bool,
    #[arg(short = 'p', long)]
    perl: bool,
    #[arg(long)]
    python: bool,
    #[arg(long)]
    tcl: bool,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    color: Option<String>,
    #[arg(long)]
    stdin: bool,
    #[arg(long = "include-root-refs")]
    include_root_refs: bool,
    patterns: Vec<String>,
}

pub async fn for_each_ref(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: ForEachRefArgs = parse_or_return!(ui, "for-each-ref", args);
    let ws = cx.ws_read(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let head = repo::current(repo, ws.workspace_root(), st.head.id());
    let r = cx.resolver(&ws).await?;
    let format = a
        .format
        .clone()
        .unwrap_or_else(|| "%(objectname) %(objecttype)\t%(refname)".to_owned());
    let matches_pattern = |full: &str| -> bool {
        if a.patterns.is_empty() {
            return true;
        }
        a.patterns.iter().any(|p| {
            if p.contains(['*', '?', '[']) {
                globset::Glob::new(p)
                    .map(|g| g.compile_matcher().is_match(full))
                    .unwrap_or(false)
            } else {
                let p = p.trim_end_matches('/');
                full == p || full.starts_with(&format!("{p}/"))
            }
        })
    };
    let resolve = async |s: &Option<String>| -> Result<Option<CommitId>, repo::RevError> {
        match s {
            Some(s) => Ok(Some(r.commit_of(s).await?.id().clone())),
            None => Ok(None),
        }
    };
    let (points, merged, no_merged, contains, no_contains) = match (
        resolve(&a.points_at).await,
        resolve(&a.merged).await,
        resolve(&a.no_merged).await,
        resolve(&a.contains).await,
        resolve(&a.no_contains).await,
    ) {
        (Ok(p), Ok(m), Ok(nm), Ok(c), Ok(nc)) => (p, m, nm, c, nc),
        _ => return fatal(ui, "malformed object name"),
    };
    let mut rows = Vec::new();
    for (kind, name, id) in all_refs(repo, false) {
        let full = full_ref(kind, &name);
        if !matches_pattern(&full)
            || a.exclude
                .iter()
                .any(|e| full.starts_with(e.trim_end_matches('/')))
        {
            continue;
        }
        if points.as_ref().is_some_and(|p| *p != id) {
            continue;
        }
        if let Some(m) = &merged
            && !repo::is_ancestor(repo, &id, m).await?
        {
            continue;
        }
        if let Some(m) = &no_merged
            && repo::is_ancestor(repo, &id, m).await?
        {
            continue;
        }
        if let Some(c) = &contains
            && !repo::is_ancestor(repo, c, &id).await?
        {
            continue;
        }
        if let Some(c) = &no_contains
            && repo::is_ancestor(repo, c, &id).await?
        {
            continue;
        }
        let c = repo.store().get_commit_async(&id).await?;
        rows.push((kind, name, c));
    }
    for key in a.sort.iter().rev() {
        let (desc, key) = match key.strip_prefix('-') {
            Some(k) => (true, k),
            None => (false, key.as_str()),
        };
        rows.sort_by(|x, y| {
            let o = match key {
                "committerdate" | "creatordate" => {
                    x.2.committer()
                        .timestamp
                        .timestamp
                        .cmp(&y.2.committer().timestamp.timestamp)
                }
                "authordate" => {
                    x.2.author()
                        .timestamp
                        .timestamp
                        .cmp(&y.2.author().timestamp.timestamp)
                }
                "objectname" => x.2.id().cmp(y.2.id()),
                _ => full_ref(x.0, &x.1).cmp(&full_ref(y.0, &y.1)),
            };
            if desc { o.reverse() } else { o }
        });
    }
    if let Some(n) = a.count {
        rows.truncate(n);
    }
    let mut out = ui.stdout();
    for (kind, name, c) in &rows {
        let is_head = *kind == RefKind::Branch && head.branch() == Some(name.as_str());
        let line = ref_format(repo, &format, *kind, name, c, is_head).await?;
        if a.omit_empty && line.is_empty() {
            continue;
        }
        writeln!(out, "{line}")?;
    }
    Ok(0)
}

pub async fn symbolic_ref(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let quiet = args.iter().any(|a| a == "-q" || a == "--quiet");
    let short = args.iter().any(|a| a == "--short");
    let names: Vec<&String> = args.iter().filter(|a| !a.starts_with('-')).collect();
    match names.as_slice() {
        [name] => {
            let ws = cx.ws_read(ui).await?;
            let repo = ws.repo().as_ref();
            let st = repo::state(&ws).await?;
            let value = match name.as_str() {
                "HEAD" => match repo::current(repo, ws.workspace_root(), st.head.id()) {
                    Head::Branch(b) => Some(if short { b } else { format!("refs/heads/{b}") }),
                    Head::Detached => None,
                },
                "refs/remotes/origin/HEAD" | "origin/HEAD" => Some(if short {
                    format!("{}/{}", repo::ORIGIN, repo::MAIN)
                } else {
                    format!("refs/remotes/{}/{}", repo::ORIGIN, repo::MAIN)
                }),
                _ => None,
            };
            match value {
                Some(v) => {
                    writeln!(ui.stdout(), "{v}")?;
                    Ok(0)
                }
                None if quiet => Ok(1),
                None => fatal(ui, format!("ref {name} is not a symbolic ref")),
            }
        }
        [name, target] if name.as_str() == "HEAD" => {
            let Some(branch) = target.strip_prefix("refs/heads/") else {
                return fatal(
                    ui,
                    format!("Refusing to point HEAD outside of refs/heads/: {target}"),
                );
            };
            change::switch(ui, cx, &[branch.to_owned()]).await
        }
        _ => {
            writeln!(ui.stderr(), "usage: git symbolic-ref [-q] [--short] <name>")?;
            Ok(129)
        }
    }
}

// ---------------------------------------------------------------------------------------------
// merge-base, describe, name-rev

pub async fn merge_base(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let all = args.iter().any(|a| a == "-a" || a == "--all");
    let is_ancestor = args.iter().any(|a| a == "--is-ancestor");
    let independent = args.iter().any(|a| a == "--independent");
    let octopus = args.iter().any(|a| a == "--octopus");
    let fork_point = args.iter().any(|a| a == "--fork-point");
    let specs: Vec<&String> = args.iter().filter(|a| !a.starts_with('-')).collect();
    let ws = cx.ws_read(ui).await?;
    let repo = ws.repo().as_ref();
    let r = cx.resolver(&ws).await?;
    let mut ids = Vec::new();
    for s in &specs {
        match r.commit_of(s).await {
            Ok(c) => ids.push(c.id().clone()),
            Err(e) => return rev_error(ui, s, e),
        }
    }
    if fork_point && ids.len() == 1 {
        let st = repo::state(&ws).await?;
        ids.push(st.head.id().clone());
    }
    if is_ancestor {
        let [x, y] = ids.as_slice() else {
            return fatal(ui, "--is-ancestor takes exactly two commits");
        };
        return Ok(if repo::is_ancestor(repo, x, y).await? {
            0
        } else {
            1
        });
    }
    if independent {
        let heads = repo.index().heads(&mut ids.iter()).await?;
        for id in ids.iter().filter(|i| heads.contains(i)) {
            writeln!(ui.stdout(), "{}", id.hex())?;
        }
        return Ok(0);
    }
    if ids.len() < 2 {
        writeln!(
            ui.stderr(),
            "usage: git merge-base [-a | --all] <commit> <commit>..."
        )?;
        return Ok(129);
    }
    let bases = if octopus || ids.len() > 2 {
        match inspect::merge_base(repo, &ids).await? {
            Some(c) => vec![c.id().clone()],
            None => vec![],
        }
    } else {
        let mut b = repo.index().common_ancestors(&ids[..1], &ids[1..]).await?;
        b.retain(|id| id != repo.store().root_commit_id());
        b.sort();
        b
    };
    if bases.is_empty() {
        return Ok(1);
    }
    let mut out = ui.stdout();
    for b in bases.iter().take(if all { usize::MAX } else { 1 }) {
        writeln!(out, "{}", b.hex())?;
    }
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct DescribeArgs {
    #[arg(long)]
    tags: bool,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    always: bool,
    #[arg(long)]
    long: bool,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    abbrev: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "-dirty")]
    dirty: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "-broken")]
    broken: Option<String>,
    #[arg(long = "match")]
    match_: Vec<String>,
    #[arg(long)]
    exclude: Vec<String>,
    #[arg(long = "first-parent")]
    first_parent: bool,
    #[arg(long = "exact-match")]
    exact_match: bool,
    #[arg(long)]
    contains: bool,
    #[arg(long)]
    candidates: Option<usize>,
    #[arg(long)]
    debug: bool,
    commits: Vec<String>,
}

/// `git describe`: the nearest tag (or, with `--all`, any ref) a commit has behind it, and how far:
/// `<tag>-<n>-g<abbrev>`; `--always` falls back to the abbreviated commit, and `--dirty` marks a
/// working tree with changes.
pub async fn describe(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: DescribeArgs = parse_or_return!(ui, "describe", args);
    let ws = if a.dirty.is_some() || a.broken.is_some() {
        cx.ws(ui).await?
    } else {
        cx.ws_read(ui).await?
    };
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let r = cx.resolver(&ws).await?;
    let specs = if a.commits.is_empty() {
        vec!["HEAD".to_owned()]
    } else {
        a.commits.clone()
    };
    let mut names: HashMap<CommitId, Vec<String>> = HashMap::new();
    for (kind, name, id) in all_refs(repo, a.all) {
        let shown = match kind {
            RefKind::Tag => name.clone(),
            RefKind::Branch if a.all => format!("heads/{name}"),
            RefKind::Remote if a.all => format!("remotes/{}/{name}", repo::ORIGIN),
            _ => continue,
        };
        if !a.match_.is_empty()
            && !a
                .match_
                .iter()
                .any(|p| globset::Glob::new(p).is_ok_and(|g| g.compile_matcher().is_match(&name)))
        {
            continue;
        }
        if a.exclude
            .iter()
            .any(|p| globset::Glob::new(p).is_ok_and(|g| g.compile_matcher().is_match(&name)))
        {
            continue;
        }
        names.entry(id).or_default().push(shown);
    }
    let abbrev_len: Option<usize> = a.abbrev.as_deref().and_then(|v| v.parse().ok());
    let dirty = if a.dirty.is_some() || a.broken.is_some() {
        st.wc.tree().tree_ids() != st.wc.parent_tree(repo).await?.tree_ids()
    } else {
        false
    };
    let mut out = ui.stdout();
    for spec in &specs {
        let c = match r.commit_of(spec).await {
            Ok(c) => c,
            Err(e) => return rev_error(ui, spec, e),
        };
        let expression = if a.first_parent {
            RevsetExpression::commits(vec![c.id().clone()]).first_ancestors()
        } else {
            RevsetExpression::commits(vec![c.id().clone()]).ancestors()
        };
        let mut found: Option<(String, CommitId)> = None;
        let mut stream = expression.evaluate(repo).map_err(internal_error)?.stream();
        while let Some(id) = stream.next().await {
            let id = id.map_err(internal_error)?;
            if let Some(n) = names.get(&id) {
                let mut n = n.clone();
                n.sort();
                found = Some((n[0].clone(), id));
                break;
            }
            if a.exact_match {
                break;
            }
        }
        let short = match abbrev_len {
            Some(0) => String::new(),
            Some(n) => c.id().hex()[..n.clamp(4, 64)].to_owned(),
            None => repo::short(repo, c.id()).await?,
        };
        let mut text = match found {
            Some((name, id)) => {
                let n = if id == *c.id() {
                    0
                } else {
                    repo::count_range(repo, &id, c.id()).await?
                };
                if n == 0 && !a.long || abbrev_len == Some(0) {
                    name
                } else {
                    format!("{name}-{n}-g{short}")
                }
            }
            None if a.always => short,
            None => {
                if a.exact_match {
                    return fatal(ui, format!("no tag exactly matches '{}'", c.id().hex()));
                }
                return fatal(ui, "No names found, cannot describe anything.");
            }
        };
        if spec == "HEAD" && a.commits.is_empty() && dirty {
            text.push_str(
                a.dirty
                    .as_deref()
                    .or(a.broken.as_deref())
                    .unwrap_or("-dirty"),
            );
        }
        writeln!(out, "{text}")?;
    }
    Ok(0)
}

pub async fn name_rev(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let name_only = args.iter().any(|a| a == "--name-only");
    let tags_only = args.iter().any(|a| a == "--tags");
    let always = args.iter().any(|a| a == "--always");
    let no_undefined = args.iter().any(|a| a == "--no-undefined");
    let specs: Vec<&String> = args.iter().filter(|a| !a.starts_with('-')).collect();
    let ws = cx.ws_read(ui).await?;
    let repo = ws.repo().as_ref();
    let r = cx.resolver(&ws).await?;
    let refs: Vec<(String, CommitId)> = all_refs(repo, true)
        .into_iter()
        .filter(|(k, _, _)| !tags_only || *k == RefKind::Tag)
        .map(|(k, n, id)| {
            let shown = match k {
                RefKind::Tag => format!("tags/{n}"),
                RefKind::Remote => format!("remotes/{}/{n}", repo::ORIGIN),
                RefKind::Branch => n,
            };
            (shown, id)
        })
        .collect();
    let mut out = ui.stdout();
    for spec in specs {
        let c = match r.commit_of(spec).await {
            Ok(c) => c,
            Err(e) => return rev_error(ui, spec, e),
        };
        let mut best: Option<(usize, String)> = None;
        for (name, id) in &refs {
            if !repo::is_ancestor(repo, c.id(), id).await? {
                continue;
            }
            // Walk the ref's first parents to the commit.
            let mut n = 0;
            let mut cur = repo.store().get_commit_async(id).await?;
            let mut hit = false;
            while n < 10_000 {
                if cur.id() == c.id() {
                    hit = true;
                    break;
                }
                let Some(p) = cur.parent_ids().first().cloned() else {
                    break;
                };
                if p == *repo.store().root_commit_id() {
                    break;
                }
                cur = repo.store().get_commit_async(&p).await?;
                n += 1;
            }
            if hit && best.as_ref().is_none_or(|(bn, _)| n < *bn) {
                best = Some((
                    n,
                    if n == 0 {
                        name.clone()
                    } else {
                        format!("{name}~{n}")
                    },
                ));
            }
        }
        let name = match best {
            Some((_, n)) => n,
            None if always => repo::short(repo, c.id()).await?,
            None if no_undefined => {
                return fatal(ui, format!("cannot describe '{}'", c.id().hex()));
            }
            None => "undefined".to_owned(),
        };
        if name_only {
            writeln!(out, "{name}")?;
        } else {
            writeln!(out, "{spec} {name}")?;
        }
    }
    Ok(0)
}

// ---------------------------------------------------------------------------------------------
// config

/// The git configuration an ok workspace answers: who you are (jj's `user.name` and
/// `user.email`), the remote, and the branch's upstream. Other keys are unset.
async fn config_values(cx: &Cx<'_>, ws: &WorkspaceCommandHelper) -> Vec<(String, String)> {
    let s = cx.command.settings();
    let mut v = vec![
        ("user.name".to_owned(), s.user_name().to_owned()),
        ("user.email".to_owned(), s.user_email().to_owned()),
        ("core.bare".to_owned(), "false".to_owned()),
    ];
    if let Ok(url) = super::remote::origin_url(cx.command, ws) {
        v.push((format!("remote.{}.url", repo::ORIGIN), url));
        v.push((
            format!("remote.{}.fetch", repo::ORIGIN),
            format!("+refs/heads/*:refs/remotes/{}/*", repo::ORIGIN),
        ));
    }
    for (name, _) in repo::branches(ws.repo().as_ref()) {
        v.push((format!("branch.{name}.remote"), repo::ORIGIN.to_owned()));
        v.push((
            format!("branch.{name}.merge"),
            format!("refs/heads/{}", repo::MAIN),
        ));
    }
    if !v.iter().any(|(k, _)| k == "branch.main.remote") {
        v.push(("branch.main.remote".to_owned(), repo::ORIGIN.to_owned()));
        v.push((
            "branch.main.merge".to_owned(),
            format!("refs/heads/{}", repo::MAIN),
        ));
    }
    for c in &cx.globals.config {
        let (k, val) = c.split_once('=').unwrap_or((c, "true"));
        v.push((k.to_ascii_lowercase(), val.to_owned()));
    }
    v
}

/// `git config`: reads what an ok workspace answers (`--get`, `--get-all`, `--get-regexp`,
/// `--list`, `git config <key>`); `user.name` and `user.email` are written to jj's configuration
/// (`--global`: the user's; else the repository's); other keys are accepted and not kept.
pub async fn config(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let mut global = false;
    let mut list = false;
    let mut get_all = false;
    let mut get_regexp = false;
    let mut unset = false;
    let mut null = false;
    let mut name_only = false;
    let mut words: Vec<String> = Vec::new();
    let mut default: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        i += 1;
        match a {
            "--global" | "--system" => global = true,
            "--local" | "--worktree" | "--includes" | "--no-includes" | "--show-origin"
            | "--show-scope" | "--bool" | "--int" | "--path" | "--bool-or-int"
            | "--expiry-date" | "--get" => {}
            "--file" | "-f" | "--blob" | "--type" => i += 1,
            "-l" | "--list" => list = true,
            "--get-all" => get_all = true,
            "--get-regexp" => get_regexp = true,
            "--unset" | "--unset-all" | "--remove-section" => unset = true,
            "-z" | "--null" => null = true,
            "--name-only" => name_only = true,
            "--add" | "--replace-all" => {}
            "--default" => {
                default = args.get(i).cloned();
                i += 1;
            }
            "-e" | "--edit" => {
                return fatal(ui, "ok's configuration is jj's: `jj config edit --user`");
            }
            _ if a.starts_with("--type=")
                || a.starts_with("--file=")
                || a.starts_with("--blob=") => {}
            _ if a.starts_with("--default=") => default = Some(a["--default=".len()..].to_owned()),
            "get" | "list" | "set" | "unset" if words.is_empty() => match a {
                "list" => list = true,
                "unset" => unset = true,
                _ => {}
            },
            _ => words.push(a.to_owned()),
        }
    }
    let ws = cx.ws_read(ui).await?;
    let values = config_values(cx, &ws).await;
    let term = if null { '\0' } else { '\n' };
    let mut out = ui.stdout();
    if list {
        for (k, v) in &values {
            if name_only {
                write!(out, "{k}{term}")?;
            } else {
                write!(out, "{k}={v}{term}")?;
            }
        }
        return Ok(0);
    }
    if get_regexp {
        let Some(p) = words.first() else {
            return Ok(129);
        };
        let Ok(re) = regex::Regex::new(&p.to_ascii_lowercase()) else {
            return error(ui, format!("invalid key pattern: {p}"), 6);
        };
        let mut any = false;
        for (k, v) in &values {
            if re.is_match(k) {
                any = true;
                if name_only {
                    write!(out, "{k}{term}")?;
                } else {
                    write!(out, "{k} {v}{term}")?;
                }
            }
        }
        return Ok(if any { 0 } else { 1 });
    }
    match words.as_slice() {
        [key] if !unset => {
            let k = key.to_ascii_lowercase();
            let found: Vec<&String> = values
                .iter()
                .filter(|(n, _)| *n == k)
                .map(|(_, v)| v)
                .collect();
            match (found.last(), &default) {
                (Some(v), _) if !get_all => write!(out, "{v}{term}")?,
                (Some(_), _) => {
                    for v in found {
                        write!(out, "{v}{term}")?;
                    }
                }
                (None, Some(d)) => write!(out, "{d}{term}")?,
                (None, None) => return Ok(1),
            }
            Ok(0)
        }
        [key, value, ..] if !unset => {
            let k = key.to_ascii_lowercase();
            if k == "user.name" || k == "user.email" {
                drop(out);
                let mut cmd = std::process::Command::new(std::env::current_exe()?);
                cmd.args([
                    "--no-pager",
                    "config",
                    "set",
                    if global { "--user" } else { "--repo" },
                    &k,
                    value,
                ])
                .current_dir(cx.command.cwd());
                let status = cmd.status()?;
                return Ok(if status.success() { 0 } else { 4 });
            }
            writeln!(
                ui.stderr(),
                "warning: ok keeps no git configuration: '{key}' is not kept"
            )?;
            Ok(0)
        }
        [_key] => Ok(0),
        _ => {
            writeln!(ui.stderr(), "usage: git config [<options>]")?;
            Ok(129)
        }
    }
}

// ---------------------------------------------------------------------------------------------
// update-index, submodule, worktree, help

/// `git update-index`: the index is the working tree, so a refresh has nothing to do; `--chmod`
/// sets the executable bit on the files, which the next snapshot records.
pub async fn update_index(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let mut chmod: Option<bool> = None;
    let mut paths = Vec::new();
    for a in args {
        match a.as_str() {
            "--chmod=+x" => chmod = Some(true),
            "--chmod=-x" => chmod = Some(false),
            _ if a.starts_with('-') => {}
            _ => paths.push(a.clone()),
        }
    }
    if let Some(exec) = chmod {
        let mut add_args = vec![
            format!("--chmod={}", if exec { "+x" } else { "-x" }),
            "-q".to_owned(),
        ];
        add_args.extend(paths);
        return change::add(ui, cx, &add_args).await;
    }
    Ok(0)
}

/// `git submodule`: ok repositories have none, so every query answers nothing and every update
/// has nothing to do.
pub async fn submodule(ui: &mut Ui, _cx: &Cx<'_>, args: &[String]) -> Code {
    match args
        .iter()
        .find(|a| !a.starts_with('-'))
        .map(String::as_str)
    {
        Some("add") => {
            fatal(ui, "ok repositories have no submodules")?;
            hint(ui, "vendor the code into the repository instead")?;
            Ok(128)
        }
        _ => Ok(0),
    }
}

/// `git worktree`: an ok workspace is its own clone (`git clone <repository> <dir>` makes
/// another, isolated from this one).
pub async fn worktree(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    match args.first().map(String::as_str) {
        Some("list") | None => {
            let ws = cx.ws_read(ui).await?;
            let st = repo::state(&ws).await?;
            let repo = ws.repo().as_ref();
            let head = repo::current(repo, ws.workspace_root(), st.head.id());
            let porcelain = args.iter().any(|a| a == "--porcelain");
            let mut out = ui.stdout();
            if porcelain {
                writeln!(out, "worktree {}", ws.workspace_root().display())?;
                writeln!(out, "HEAD {}", st.head.id().hex())?;
                match head {
                    Head::Branch(b) => writeln!(out, "branch refs/heads/{b}")?,
                    Head::Detached => writeln!(out, "detached")?,
                }
                writeln!(out)?;
            } else {
                let place = match head {
                    Head::Branch(b) => format!("[{b}]"),
                    Head::Detached => "(detached HEAD)".to_owned(),
                };
                writeln!(
                    out,
                    "{}  {} {place}",
                    ws.workspace_root().display(),
                    repo::short(repo, st.head.id()).await?
                )?;
            }
            Ok(0)
        }
        Some("prune") | Some("repair") => Ok(0),
        Some(_) => {
            fatal(
                ui,
                "an ok workspace is a clone of its own: there are no worktrees to add",
            )?;
            hint(
                ui,
                "`git clone <repository> <directory>` makes another workspace, isolated from this one",
            )?;
            Ok(128)
        }
    }
}

/// `git help ok`: how git works in an ok workspace.
pub async fn help(ui: &mut Ui, _cx: &Cx<'_>, _args: &[String]) -> Code {
    write!(
        ui.stdout(),
        r#"git in an ok workspace

In an ok workspace (a directory `git clone <repository>` or `jj clone` made), git's commands
are ok's: this git is ok's jj. Everywhere else, git is git.

The model
  HEAD           the working change's parent: your last commit
  index, tree    one and the same: the working change. ok tracks every file the ignore
                 files (.ignore, .gitignore) do not name, so `git add` has nothing to do,
                 and `git commit` commits every change (or the paths it is given)
  branches       local names for commits (jj's bookmarks), seen by this workspace alone
  origin/main    ok's main; every branch's upstream
  push           uploads HEAD and its unlanded ancestors as ChangeLists, one per commit
                 (-o r=<reviewer>, -o ready, -o wip; or --reviewer, --ready, --wip)
  pull           `jj sync`: main brought up to date, landed commits retired, the rest
                 rebased onto main. ok rebases always: a ChangeList is one change on one
                 parent
  fetch          origin/main brought up to date; `git fetch origin cl/<n>` brings in a
                 ChangeList as origin/cl/<n>
  conflicts      kept in the commit that has them, never stopping a command halfway:
                 resolve them in the working tree and `git commit` (or --continue) folds
                 the resolution in; --abort undoes the command
  stash          commits set aside on the HEAD they were made at, in ok like any change

What ok has no use for — the index's plumbing, reflogs, submodules, worktrees, interactive
rebases — says so, and names what to use instead. `jj` is always there underneath:
`jj log`, `jj op log`, and `jj undo` see and undo everything git did.
"#
    )?;
    Ok(0)
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn args() {
        BranchArgs::command().debug_assert();
        TagArgs::command().debug_assert();
        ShowRefArgs::command().debug_assert();
        ForEachRefArgs::command().debug_assert();
        DescribeArgs::command().debug_assert();
    }

    #[test]
    fn ref_names() {
        assert!(check_ref_name("feature/x").is_ok());
        assert!(check_ref_name("fix-1.2").is_ok());
        assert!(check_ref_name("bad name").is_err());
        assert!(check_ref_name("a..b").is_err());
        assert!(check_ref_name("-x").is_err());
        assert!(check_ref_name("x.lock").is_err());
        assert!(check_ref_name("HEAD").is_err());
    }

    #[test]
    fn full_refs() {
        assert_eq!(full_ref(RefKind::Branch, "x"), "refs/heads/x");
        assert_eq!(
            full_ref(RefKind::Remote, "main"),
            "refs/remotes/origin/main"
        );
        assert_eq!(full_ref(RefKind::Tag, "v1"), "refs/tags/v1");
    }
}
