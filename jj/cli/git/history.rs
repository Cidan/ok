//! git's commands that make history from other history: `merge`, `rebase`, `cherry-pick`, and
//! `revert`. They never stop halfway, as jj never does: a conflict is recorded in the commit
//! that has it, the command says where, and exits 1. `git commit` (or `--continue`) then folds
//! the resolution into that commit, and `--abort` undoes the command's operation.

use std::io::Write as _;

use futures::TryStreamExt as _;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::internal_error;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::commit::Commit;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo::MutableRepo;
use jj_lib::repo::Repo;
use jj_lib::revset::RevsetExpression;

use super::Code;
use super::Cx;
use super::change;
use super::change::Carried;
use super::change::Carry;
use super::error;
use super::fatal;
use super::fmt;
use super::hint;
use super::inspect;
use super::parse_or_return;
use super::repo;
use super::repo::Head;
use super::rev_error;

/// Undoes the most recent operation of a command (its description starting with one of
/// `prefixes`), past the snapshots taken since: git's `--abort`. None when the last thing done
/// was something else.
async fn abort(ui: &mut Ui, cx: &Cx<'_>, prefixes: &[&str], what: &str) -> Code {
    let mut ws = cx.ws(ui).await?;
    let mut op = ws.repo().operation().clone();
    let target = loop {
        let desc = op.metadata().description.clone();
        if prefixes.iter().any(|p| desc.starts_with(p)) {
            break op;
        }
        if desc != "snapshot working copy" {
            return fatal(ui, format!("There is no {what} to abort."));
        }
        let parents = op.parents().await.map_err(internal_error)?;
        let [parent] = parents.as_slice() else {
            return fatal(ui, format!("There is no {what} to abort."));
        };
        op = parent.clone();
    };
    let parents = target.parents().await.map_err(internal_error)?;
    let [parent] = parents.as_slice() else {
        return fatal(ui, format!("There is no {what} to abort."));
    };
    let mut tx = ws.start_transaction();
    let loader = tx.base_repo().loader().clone();
    let at = loader.load_at(&target).await.map_err(internal_error)?;
    let before = loader.load_at(parent).await.map_err(internal_error)?;
    tx.repo_mut().merge(&at, &before).await?;
    tx.finish(ui, format!("git {what} --abort")).await?;
    Ok(0)
}

/// `--continue`: the working tree's resolution folded into HEAD, if HEAD has conflicts.
async fn continue_(ui: &mut Ui, cx: &Cx<'_>, what: &str) -> Code {
    let ws = cx.ws(ui).await?;
    let st = repo::state(&ws).await?;
    drop(ws);
    if st.unborn || !st.head.has_conflict() {
        return fatal(ui, format!("no {what} in progress"));
    }
    change::commit(ui, cx, &["--no-edit".to_owned()]).await
}

/// Says where the conflicts of `commit` are, as git does.
async fn report_conflicts(
    ui: &Ui,
    cx: &Cx<'_>,
    ws: &WorkspaceCommandHelper,
    commit: &Commit,
) -> Result<bool, CommandError> {
    let converter = inspect::cwd_converter(cx, ws);
    let mut any = false;
    let mut out = ui.stdout();
    for (path, _) in commit.tree().conflicts() {
        any = true;
        writeln!(
            out,
            "CONFLICT (content): Merge conflict in {}",
            converter.format_file_path(&path)
        )?;
    }
    Ok(any)
}

/// A 3-way merge of trees: `ours` with what changed from `base` to `theirs`.
async fn apply(
    ours: MergedTree,
    base: MergedTree,
    theirs: MergedTree,
    labels: [&str; 3],
) -> Result<MergedTree, CommandError> {
    Ok(MergedTree::merge(Merge::from_vec(vec![
        (ours, labels[0].to_owned()),
        (base, labels[1].to_owned()),
        (theirs, labels[2].to_owned()),
    ]))
    .await?)
}

/// The working change carried onto the new HEAD, or the refusal git gives when its changes are
/// in the way (`None`: the transaction is to be dropped).
async fn carry_onto(
    repo: &mut MutableRepo,
    wc: &Commit,
    head: &Commit,
) -> Result<Result<(), Vec<jj_lib::repo_path::RepoPathBuf>>, CommandError> {
    match change::carry(repo, wc, head, Carry::Keep).await? {
        Carried::Moved(_) => Ok(Ok(())),
        Carried::Conflicts(p) => Ok(Err(p)),
    }
}

fn refuse(
    ui: &Ui,
    cx: &Cx<'_>,
    ws: &WorkspaceCommandHelper,
    paths: &[jj_lib::repo_path::RepoPathBuf],
) -> Code {
    let converter = inspect::cwd_converter(cx, ws);
    let mut e = ui.stderr();
    writeln!(
        e,
        "error: Your local changes to the following files would be overwritten by merge:"
    )?;
    for p in paths {
        writeln!(e, "\t{}", converter.format_file_path(p))?;
    }
    writeln!(
        e,
        "Please commit your changes or stash them before you merge.\nAborting"
    )?;
    Ok(1)
}

// ---------------------------------------------------------------------------------------------
// merge

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct MergeArgs {
    #[arg(long = "ff")]
    ff: bool,
    #[arg(long = "no-ff")]
    no_ff: bool,
    #[arg(long = "ff-only")]
    ff_only: bool,
    #[arg(long)]
    squash: bool,
    #[arg(long = "no-squash")]
    no_squash: bool,
    #[arg(long)]
    commit: bool,
    #[arg(long = "no-commit")]
    no_commit: bool,
    #[arg(short = 'm')]
    message: Vec<String>,
    #[arg(short = 'F', long = "file")]
    file: Option<String>,
    #[arg(short = 'e', long)]
    edit: bool,
    #[arg(long = "no-edit")]
    no_edit: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(long)]
    stat: bool,
    #[arg(short = 'n', long = "no-stat")]
    no_stat: bool,
    #[arg(long)]
    log: Option<Option<String>>,
    #[arg(long = "no-log")]
    no_log: bool,
    #[arg(short = 's', long)]
    strategy: Option<String>,
    #[arg(short = 'X', long = "strategy-option")]
    strategy_option: Vec<String>,
    #[arg(long = "allow-unrelated-histories")]
    allow_unrelated_histories: bool,
    #[arg(long)]
    abort: bool,
    #[arg(long = "continue")]
    continue_: bool,
    #[arg(long)]
    quit: bool,
    #[arg(long)]
    autostash: bool,
    #[arg(long = "no-autostash")]
    no_autostash: bool,
    #[arg(long)]
    signoff: bool,
    #[arg(long = "no-verify")]
    no_verify: bool,
    #[arg(long)]
    progress: bool,
    #[arg(long = "into-name")]
    into_name: Option<String>,
    commits: Vec<String>,
}

/// The message git gives a merge commit: `Merge branch 'x'`, `Merge commit 'abc'`,
/// `Merge remote-tracking branch 'origin/main'`, and `into <branch>` unless it is main.
fn merge_message(names: &[String], head: &Head) -> String {
    let describe = |n: &str| -> (&'static str, String) {
        if n.starts_with("origin/") || n.starts_with("refs/remotes/") {
            (
                "remote-tracking branch",
                n.trim_start_matches("refs/remotes/").to_owned(),
            )
        } else if n.bytes().all(|b| b.is_ascii_hexdigit()) || n.contains(['~', '^']) {
            ("commit", n.to_owned())
        } else {
            ("branch", n.to_owned())
        }
    };
    let mut msg = if names.len() == 1 {
        let (kind, n) = describe(&names[0]);
        format!("Merge {kind} '{n}'")
    } else {
        let quoted: Vec<String> = names
            .iter()
            .map(|n| format!("'{}'", describe(n).1))
            .collect();
        let (last, rest) = quoted.split_last().unwrap();
        format!("Merge branches {} and {last}", rest.join(", "))
    };
    if let Head::Branch(b) = head
        && b != repo::MAIN
    {
        msg.push_str(&format!(" into {b}"));
    }
    msg.push('\n');
    msg
}

/// `git merge`: a fast-forward when HEAD is behind; otherwise a merge commit on HEAD and the
/// commits named, conflicts and all. ok uploads no merge commits — a ChangeList is one change on
/// one parent — so a merge of `origin/main` is better a `git rebase` (or `git pull`), which the
/// command says.
pub async fn merge(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: MergeArgs = parse_or_return!(ui, "merge", args);
    if a.abort || a.quit {
        if a.quit {
            return Ok(0);
        }
        return abort(ui, cx, &["git merge"], "merge").await;
    }
    if a.continue_ {
        return continue_(ui, cx, "merge").await;
    }
    let mut ws = cx.ws(ui).await?;
    let b = change::before(&ws).await?;
    let r = cx.resolver(&ws).await?;
    let names = if a.commits.is_empty() {
        vec![format!("{}/{}", repo::ORIGIN, repo::MAIN)]
    } else {
        a.commits.clone()
    };
    let mut theirs = Vec::new();
    for n in &names {
        match r.commit_of(n).await {
            Ok(c) => theirs.push(c),
            Err(e) => return rev_error(ui, n, e),
        }
    }
    drop(r);
    let repo = ws.repo().clone();
    let head = b.st.head.clone();
    let wc = b.st.wc.clone();
    let mut new: Vec<Commit> = Vec::new();
    for t in &theirs {
        if !repo::is_ancestor(repo.as_ref(), t.id(), head.id()).await? || b.st.unborn {
            new.push(t.clone());
        }
    }
    if new.is_empty() {
        writeln!(ui.stdout(), "Already up to date.")?;
        return Ok(0);
    }
    let ff = new.len() == 1
        && (b.st.unborn || repo::is_ancestor(repo.as_ref(), head.id(), new[0].id()).await?);
    if a.ff_only && !ff {
        return fatal(ui, "Not possible to fast-forward, aborting.");
    }
    if a.squash {
        let base =
            match inspect::merge_base(repo.as_ref(), &[head.id().clone(), new[0].id().clone()])
                .await?
            {
                Some(c) => c.tree(),
                None => repo.store().empty_merged_tree(),
            };
        let tree = apply(
            wc.tree(),
            base,
            new[0].tree(),
            ["the working copy", "the merge base", &names[0]],
        )
        .await?;
        let mut tx = ws.start_transaction();
        tx.repo_mut()
            .rewrite_commit(&wc)
            .set_tree(tree)
            .write()
            .await?;
        tx.repo_mut().rebase_descendants().await?;
        tx.finish(ui, format!("git merge --squash {}", names.join(" ")))
            .await?;
        let st = repo::state(&ws).await?;
        if report_conflicts(ui, cx, &ws, &st.wc).await? {
            writeln!(ui.stdout(), "Squash commit -- not updating HEAD")?;
            writeln!(
                ui.stdout(),
                "Automatic merge failed; fix conflicts and then commit the result."
            )?;
            return Ok(1);
        }
        writeln!(ui.stdout(), "Squash commit -- not updating HEAD")?;
        return Ok(0);
    }
    if ff && !a.no_ff {
        let target = new[0].clone();
        let mut tx = ws.start_transaction();
        if let Err(paths) = carry_onto(tx.repo_mut(), &wc, &target).await? {
            drop(tx);
            return refuse(ui, cx, &ws, &paths);
        }
        change::advance(tx.repo_mut(), &b.head, head.id(), target.id());
        let (from_short, to_short) = (
            repo::short(tx.repo(), head.id()).await?,
            repo::short(tx.repo(), target.id()).await?,
        );
        tx.finish(ui, format!("git merge {}", names.join(" ")))
            .await?;
        change::record(&ws, &b, &b.head, target.id());
        if !a.quiet {
            writeln!(ui.stdout(), "Updating {from_short}..{to_short}")?;
            writeln!(ui.stdout(), "Fast-forward")?;
            let show = inspect::DiffShow::from_flags(
                &inspect::DiffFlags {
                    stat: Some(String::new()),
                    summary: true,
                    ..Default::default()
                },
                false,
                false,
            )
            .unwrap();
            let repo = ws.repo().as_ref();
            let copies = repo::copy_records(
                repo,
                &[head.id().clone()],
                target.id(),
                &jj_lib::matchers::EverythingMatcher,
            )
            .await?;
            let mut out = ui.stdout_formatter();
            inspect::show_diff(
                ui,
                cx,
                &ws,
                repo,
                out.as_mut(),
                (&head.tree(), &target.tree()),
                &copies,
                &jj_lib::matchers::EverythingMatcher,
                &show,
            )
            .await?;
        }
        return Ok(0);
    }
    let mut parents = vec![head.clone()];
    parents.extend(new.iter().cloned());
    let tree = jj_lib::rewrite::merge_commit_trees(repo.as_ref(), &parents).await?;
    let message = if !a.message.is_empty() {
        change::cleanup(
            &jj_cli::description_util::join_message_paragraphs(&a.message),
            false,
        )
    } else if let Some(f) = &a.file {
        match std::fs::read_to_string(cx.command.cwd().join(f)) {
            Ok(t) => change::cleanup(&t, false),
            Err(e) => return fatal(ui, format!("could not read '{f}': {e}")),
        }
    } else {
        merge_message(&names, &b.head)
    };
    let message = if a.edit {
        change::edit_message(cx, &message, &[])?
    } else {
        message
    };
    let mut tx = ws.start_transaction();
    if a.no_commit {
        // The merge waits in the working change, a commit of both parents: `git commit` makes it.
        let base_tree = wc.parent_tree(tx.repo()).await?;
        let carried = apply(
            tree,
            base_tree,
            wc.tree(),
            ["the merge", "HEAD", "the working copy"],
        )
        .await?;
        let ids: Vec<CommitId> = parents.iter().map(|c| c.id().clone()).collect();
        let pending = tx
            .repo_mut()
            .rewrite_commit(&wc)
            .set_parents(ids)
            .set_tree(carried)
            .set_description(message)
            .write()
            .await?;
        tx.repo_mut().rebase_descendants().await?;
        tx.finish(ui, format!("git merge --no-commit {}", names.join(" ")))
            .await?;
        if report_conflicts(ui, cx, &ws, &pending).await? {
            writeln!(
                ui.stdout(),
                "Automatic merge failed; fix conflicts and then commit the result."
            )?;
            return Ok(1);
        }
        writeln!(
            ui.stdout(),
            "Automatic merge went well; stopped before committing as requested"
        )?;
        return Ok(0);
    }
    let ids: Vec<CommitId> = parents.iter().map(|c| c.id().clone()).collect();
    let merged = tx
        .repo_mut()
        .new_commit(ids, tree)
        .set_description(message)
        .write()
        .await?;
    if let Err(paths) = carry_onto(tx.repo_mut(), &wc, &merged).await? {
        drop(tx);
        return refuse(ui, cx, &ws, &paths);
    }
    change::advance(tx.repo_mut(), &b.head, head.id(), merged.id());
    tx.finish(ui, format!("git merge {}", names.join(" ")))
        .await?;
    change::record(&ws, &b, &b.head, merged.id());
    let conflicted = report_conflicts(ui, cx, &ws, &merged).await?;
    if conflicted {
        writeln!(
            ui.stdout(),
            "Automatic merge failed; fix conflicts and then commit the result."
        )?;
    } else if !a.quiet {
        writeln!(ui.stdout(), "Merge made by the 'ok' strategy.")?;
    }
    hint(
        ui,
        "ok uploads no merge commits (a ChangeList is one change on one parent): `git rebase origin/main` brings main in a way `git push` can upload",
    )?;
    Ok(if conflicted { 1 } else { 0 })
}

// ---------------------------------------------------------------------------------------------
// rebase

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct RebaseArgs {
    #[arg(long)]
    onto: Option<String>,
    #[arg(long = "keep-base")]
    keep_base: bool,
    #[arg(long = "continue")]
    continue_: bool,
    #[arg(long)]
    abort: bool,
    #[arg(long)]
    skip: bool,
    #[arg(long)]
    quit: bool,
    #[arg(long = "edit-todo")]
    edit_todo: bool,
    #[arg(long = "show-current-patch")]
    show_current_patch: bool,
    #[arg(short = 'i', long)]
    interactive: bool,
    #[arg(short = 'x', long)]
    exec: Vec<String>,
    #[arg(long)]
    root: bool,
    #[arg(long)]
    autosquash: bool,
    #[arg(long = "no-autosquash")]
    no_autosquash: bool,
    #[arg(long)]
    autostash: bool,
    #[arg(long = "no-autostash")]
    no_autostash: bool,
    #[arg(long = "update-refs")]
    update_refs: bool,
    #[arg(long = "no-update-refs")]
    no_update_refs: bool,
    #[arg(short = 'f', long = "force-rebase", visible_alias = "no-ff")]
    force_rebase: bool,
    #[arg(long = "fork-point")]
    fork_point: bool,
    #[arg(long = "no-fork-point")]
    no_fork_point: bool,
    #[arg(short = 'r', long = "rebase-merges", num_args = 0..=1, require_equals = true)]
    rebase_merges: Option<String>,
    #[arg(long = "no-rebase-merges")]
    no_rebase_merges: bool,
    #[arg(short = 'm', long)]
    merge: bool,
    #[arg(long)]
    apply: bool,
    #[arg(short = 's', long)]
    strategy: Option<String>,
    #[arg(short = 'X', long = "strategy-option")]
    strategy_option: Vec<String>,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(long)]
    stat: bool,
    #[arg(short = 'n', long = "no-stat")]
    no_stat: bool,
    #[arg(long = "no-verify")]
    no_verify: bool,
    #[arg(long)]
    verify: bool,
    #[arg(long)]
    signoff: bool,
    #[arg(long = "committer-date-is-author-date")]
    committer_date_is_author_date: bool,
    #[arg(long = "ignore-date", visible_alias = "reset-author-date")]
    ignore_date: bool,
    #[arg(long = "empty")]
    empty: Option<String>,
    #[arg(long = "keep-empty")]
    keep_empty: bool,
    #[arg(long = "no-keep-empty")]
    no_keep_empty: bool,
    #[arg(long = "reapply-cherry-picks")]
    reapply_cherry_picks: bool,
    #[arg(long = "no-reapply-cherry-picks")]
    no_reapply_cherry_picks: bool,
    #[arg(long = "allow-empty-message")]
    allow_empty_message: bool,
    #[arg(short = 'C')]
    context: Option<String>,
    #[arg(long = "ignore-whitespace")]
    ignore_whitespace: bool,
    #[arg(long = "whitespace")]
    whitespace: Option<String>,
    #[arg(long = "rerere-autoupdate")]
    rerere_autoupdate: bool,
    #[arg(long = "no-rerere-autoupdate")]
    no_rerere_autoupdate: bool,
    #[arg(short = 'S', long = "gpg-sign", num_args = 0..=1, require_equals = true)]
    gpg_sign: Option<String>,
    #[arg(long = "no-gpg-sign")]
    no_gpg_sign: bool,
    upstream: Option<String>,
    branch: Option<String>,
}

/// `git rebase [<upstream>] [<branch>] [--onto <newbase>]`: HEAD's commits since `<upstream>`
/// (`origin/main` by default) moved onto `<newbase>` (`<upstream>` by default), and the branch,
/// the working change, and what else was built on them with them. Conflicts stay in the commits
/// that have them, said, and the command exits 1.
pub async fn rebase(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: RebaseArgs = parse_or_return!(ui, "rebase", args);
    if a.abort || a.quit || a.skip {
        if a.quit {
            return Ok(0);
        }
        return abort(ui, cx, &["git rebase", "git pull"], "rebase").await;
    }
    if a.continue_ {
        return continue_(ui, cx, "rebase").await;
    }
    if a.interactive || a.edit_todo || !a.exec.is_empty() {
        fatal(
            ui,
            "an interactive rebase is not available in ok workspaces",
        )?;
        hint(
            ui,
            "jj rewrites history in place: `jj squash`, `jj split`, `jj describe`, `jj rebase -r`, and `jj abandon`",
        )?;
        return Ok(128);
    }
    if a.show_current_patch {
        return fatal(ui, "no rebase in progress");
    }
    if let Some(branch) = &a.branch {
        let code = change::switch(ui, cx, std::slice::from_ref(branch)).await?;
        if code != 0 {
            return Ok(code);
        }
    }
    let mut ws = cx.ws(ui).await?;
    let b = change::before(&ws).await?;
    if b.st.unborn {
        return fatal(ui, "your current branch has no commits yet");
    }
    let r = cx.resolver(&ws).await?;
    let upstream_spec = a
        .upstream
        .clone()
        .unwrap_or_else(|| format!("{}/{}", repo::ORIGIN, repo::MAIN));
    let upstream = match r.commit_of(&upstream_spec).await {
        Ok(c) => c,
        Err(e) => return rev_error(ui, &upstream_spec, e),
    };
    let onto = match &a.onto {
        Some(o) => match r.commit_of(o).await {
            Ok(c) => c,
            Err(e) => return rev_error(ui, o, e),
        },
        None if a.keep_base => {
            match inspect::merge_base(r.repo, &[upstream.id().clone(), b.st.head.id().clone()])
                .await?
            {
                Some(c) => c,
                None => return fatal(ui, "no merge base"),
            }
        }
        None => upstream.clone(),
    };
    drop(r);
    let repo = ws.repo().clone();
    let head = b.st.head.clone();
    let protected: Vec<CommitId> = repo::remote_branches(repo.as_ref())
        .into_iter()
        .map(|(_, id)| id)
        .collect();
    let range = if a.root {
        RevsetExpression::commits(vec![head.id().clone()]).ancestors()
    } else {
        RevsetExpression::commits(vec![upstream.id().clone()])
            .range(&RevsetExpression::commits(vec![head.id().clone()]))
    }
    .minus(&RevsetExpression::commits(protected).ancestors())
    .minus(&RevsetExpression::root());
    let roots: Vec<CommitId> = range
        .roots()
        .evaluate(repo.as_ref())
        .map_err(internal_error)?
        .stream()
        .try_collect()
        .await
        .map_err(internal_error)?;
    let place = match &b.head {
        Head::Branch(br) => format!("refs/heads/{br}"),
        Head::Detached => "HEAD".to_owned(),
    };
    if roots.is_empty() {
        if repo::is_ancestor(repo.as_ref(), onto.id(), head.id()).await? {
            writeln!(
                ui.stdout(),
                "Current branch {} is up to date.",
                b.head.branch().unwrap_or("HEAD")
            )?;
            return Ok(0);
        }
        // Behind only: a fast-forward.
        let wc = b.st.wc.clone();
        let mut tx = ws.start_transaction();
        if let Err(paths) = carry_onto(tx.repo_mut(), &wc, &onto).await? {
            drop(tx);
            return refuse(ui, cx, &ws, &paths);
        }
        change::advance(tx.repo_mut(), &b.head, head.id(), onto.id());
        tx.finish(ui, format!("git rebase {upstream_spec}")).await?;
        change::record(&ws, &b, &b.head, onto.id());
        if !a.quiet {
            writeln!(ui.stderr(), "Successfully rebased and updated {place}.")?;
        }
        return Ok(0);
    }
    let mut already = true;
    for root in &roots {
        let c = repo.store().get_commit_async(root).await?;
        if c.parent_ids() != [onto.id().clone()] {
            already = false;
        }
    }
    if already && !a.force_rebase {
        writeln!(
            ui.stdout(),
            "Current branch {} is up to date.",
            b.head.branch().unwrap_or("HEAD")
        )?;
        return Ok(0);
    }
    let mut tx = ws.start_transaction();
    for root in &roots {
        let c = tx.repo().store().get_commit_async(root).await?;
        jj_lib::rewrite::rebase_commit(tx.repo_mut(), c, vec![onto.id().clone()]).await?;
    }
    tx.repo_mut().rebase_descendants().await?;
    tx.finish(ui, format!("git rebase {upstream_spec}")).await?;
    let st = repo::state(&ws).await?;
    change::record(&ws, &b, &b.head, st.head.id());
    // What the rebase left conflicted.
    let moved: Vec<CommitId> = RevsetExpression::commits(vec![onto.id().clone()])
        .range(&RevsetExpression::commits(vec![st.head.id().clone()]))
        .evaluate(ws.repo().as_ref())
        .map_err(internal_error)?
        .stream()
        .try_collect()
        .await
        .map_err(internal_error)?;
    let mut conflicted = Vec::new();
    for id in moved.iter().rev() {
        let c = ws.repo().store().get_commit_async(id).await?;
        if c.has_conflict() {
            conflicted.push(c);
        }
    }
    if conflicted.is_empty() {
        if !a.quiet {
            writeln!(ui.stderr(), "Successfully rebased and updated {place}.")?;
        }
        return Ok(0);
    }
    for c in &conflicted {
        report_conflicts(ui, cx, &ws, c).await?;
        let short = repo::short(ws.repo().as_ref(), c.id()).await?;
        writeln!(
            ui.stderr(),
            "error: {short} ({}) has conflicts",
            fmt::subject(c.description())
        )?;
    }
    hint(
        ui,
        "The rebase is done, its conflicts kept in the commits named. Resolve HEAD's in the working tree and `git commit` (or `git rebase --continue`); an earlier commit's with `jj new <commit>`, then `jj squash`. `git rebase --abort` undoes the rebase.",
    )?;
    Ok(1)
}

// ---------------------------------------------------------------------------------------------
// cherry-pick, revert

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct PickArgs {
    #[arg(short = 'x')]
    x: bool,
    #[arg(short = 'n', long = "no-commit")]
    no_commit: bool,
    #[arg(short = 'e', long)]
    edit: bool,
    #[arg(long = "no-edit")]
    no_edit: bool,
    #[arg(short = 'm', long = "mainline")]
    mainline: Option<usize>,
    #[arg(long)]
    ff: bool,
    #[arg(long = "allow-empty")]
    allow_empty: bool,
    #[arg(long = "allow-empty-message")]
    allow_empty_message: bool,
    #[arg(long = "keep-redundant-commits")]
    keep_redundant_commits: bool,
    #[arg(long)]
    empty: Option<String>,
    #[arg(short = 's', long)]
    signoff: bool,
    #[arg(long)]
    strategy: Option<String>,
    #[arg(short = 'X', long = "strategy-option")]
    strategy_option: Vec<String>,
    #[arg(long = "rerere-autoupdate")]
    rerere_autoupdate: bool,
    #[arg(long = "no-rerere-autoupdate")]
    no_rerere_autoupdate: bool,
    #[arg(short = 'S', long = "gpg-sign", num_args = 0..=1, require_equals = true)]
    gpg_sign: Option<String>,
    #[arg(long = "no-gpg-sign")]
    no_gpg_sign: bool,
    #[arg(long)]
    reference: bool,
    #[arg(long)]
    abort: bool,
    #[arg(long = "continue")]
    continue_: bool,
    #[arg(long)]
    skip: bool,
    #[arg(long)]
    quit: bool,
    commits: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Pick {
    CherryPick,
    Revert,
}

impl Pick {
    fn name(self) -> &'static str {
        match self {
            Pick::CherryPick => "cherry-pick",
            Pick::Revert => "revert",
        }
    }
}

pub async fn cherry_pick(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    pick(ui, cx, args, Pick::CherryPick).await
}

pub async fn revert(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    pick(ui, cx, args, Pick::Revert).await
}

/// `git cherry-pick` and `git revert`: each commit's change (or its inverse) applied on HEAD as
/// a new commit, the branch moving along; `-n` applies them to the working change instead. A
/// conflict stops the run at the commit that has it.
async fn pick(ui: &mut Ui, cx: &Cx<'_>, args: &[String], kind: Pick) -> Code {
    let a: PickArgs = parse_or_return!(ui, kind.name(), args);
    if a.abort || a.skip || a.quit {
        if a.quit {
            return Ok(0);
        }
        return abort(ui, cx, &[&format!("git {}", kind.name())], kind.name()).await;
    }
    if a.continue_ {
        return continue_(ui, cx, kind.name()).await;
    }
    if a.commits.is_empty() {
        writeln!(
            ui.stderr(),
            "usage: git {} [<options>] <commit-ish>...",
            kind.name()
        )?;
        return Ok(129);
    }
    let mut ws = cx.ws(ui).await?;
    let b = change::before(&ws).await?;
    let r = cx.resolver(&ws).await?;
    // Ranges pick oldest first; single commits in the order given.
    let mut picks: Vec<Commit> = Vec::new();
    for spec in &a.commits {
        if spec.contains("..") || spec.starts_with('^') {
            let w = match repo::walk(&r, std::slice::from_ref(spec), false).await {
                Ok(w) => w,
                Err((s, e)) => return rev_error(ui, &s, e),
            };
            let mut ids: Vec<CommitId> = w
                .expression(r.repo)
                .evaluate(r.repo)
                .map_err(internal_error)?
                .stream()
                .try_collect()
                .await
                .map_err(internal_error)?;
            ids.reverse();
            for id in ids {
                picks.push(r.repo.store().get_commit_async(&id).await?);
            }
        } else {
            match r.commit_of(spec).await {
                Ok(c) => picks.push(c),
                Err(e) => return rev_error(ui, spec, e),
            }
        }
    }
    drop(r);
    let wc = b.st.wc.clone();
    let mut tx = ws.start_transaction();
    let mut head = b.st.head.clone();
    let mut working = wc.tree();
    let mut made: Vec<Commit> = Vec::new();
    let mut stopped: Option<Commit> = None;
    for c in &picks {
        let parents = c.parents().await?;
        let parent_tree = if c.parent_ids().len() > 1 {
            let Some(m) = a.mainline else {
                drop(tx);
                return error(
                    ui,
                    format!(
                        "commit {} is a merge but no -m option was given.",
                        c.id().hex()
                    ),
                    128,
                );
            };
            match parents.get(m.wrapping_sub(1)) {
                Some(p) => p.tree(),
                None => {
                    drop(tx);
                    return error(
                        ui,
                        format!("commit {} does not have parent {m}", c.id().hex()),
                        128,
                    );
                }
            }
        } else {
            c.parent_tree(tx.repo()).await?
        };
        let label = format!(
            "{} {}",
            repo::short_hex(&c.id().hex()),
            fmt::subject(c.description())
        );
        let (base, theirs) = match kind {
            Pick::CherryPick => (parent_tree, c.tree()),
            Pick::Revert => (c.tree(), parent_tree),
        };
        let ours = if a.no_commit {
            working.clone()
        } else {
            head.tree()
        };
        let tree = apply(ours, base, theirs, ["HEAD", "base", &label]).await?;
        if a.no_commit {
            working = tree;
            continue;
        }
        if tree.tree_ids() == head.tree().tree_ids() && !a.allow_empty && !a.keep_redundant_commits
        {
            writeln!(
                ui.stderr(),
                "The previous {} is now empty; skipping {}",
                kind.name(),
                repo::short_hex(&c.id().hex())
            )?;
            continue;
        }
        let mut description = match kind {
            Pick::CherryPick => c.description().to_owned(),
            Pick::Revert => {
                let subject = fmt::subject(c.description());
                if a.reference {
                    format!(
                        "Revert {} ({subject})\n\nThis reverts commit {}.\n",
                        repo::short_hex(&c.id().hex()),
                        c.id().hex()
                    )
                } else {
                    format!(
                        "Revert \"{subject}\"\n\nThis reverts commit {}.\n",
                        c.id().hex()
                    )
                }
            }
        };
        if kind == Pick::CherryPick && a.x {
            description = format!(
                "{}\n\n(cherry picked from commit {})\n",
                description.trim_end(),
                c.id().hex()
            );
        }
        if a.signoff {
            let s = cx.command.settings();
            description = format!(
                "{}\n\nSigned-off-by: {} <{}>\n",
                description.trim_end(),
                s.user_name(),
                s.user_email()
            );
        }
        if a.edit {
            description = change::edit_message(cx, &description, &[])?;
        }
        let mut builder = tx
            .repo_mut()
            .new_commit(vec![head.id().clone()], tree)
            .set_description(description);
        if kind == Pick::CherryPick {
            builder = builder.set_author(c.author().clone());
        }
        let new = builder.write().await?;
        head = new.clone();
        made.push(new.clone());
        if new.has_conflict() {
            stopped = Some(c.clone());
            break;
        }
    }
    if a.no_commit {
        tx.repo_mut()
            .rewrite_commit(&wc)
            .set_tree(working)
            .write()
            .await?;
        tx.repo_mut().rebase_descendants().await?;
        tx.finish(ui, format!("git {} --no-commit", kind.name()))
            .await?;
        let st = repo::state(&ws).await?;
        return Ok(if report_conflicts(ui, cx, &ws, &st.wc).await? {
            1
        } else {
            0
        });
    }
    if made.is_empty() {
        return Ok(0);
    }
    if let Err(paths) = carry_onto(tx.repo_mut(), &wc, &head).await? {
        drop(tx);
        return refuse(ui, cx, &ws, &paths);
    }
    change::advance(tx.repo_mut(), &b.head, b.st.head.id(), head.id());
    tx.finish(ui, format!("git {} {}", kind.name(), a.commits.join(" ")))
        .await?;
    change::record(&ws, &b, &b.head, head.id());
    for c in &made {
        if !c.has_conflict() {
            change::report_commit(ui, cx, &ws, c, &b.head).await?;
        }
    }
    if let Some(c) = stopped {
        let last = made.last().unwrap();
        report_conflicts(ui, cx, &ws, last).await?;
        writeln!(
            ui.stderr(),
            "error: could not {} {}... {}",
            if kind == Pick::Revert {
                "revert"
            } else {
                "apply"
            },
            repo::short_hex(&c.id().hex()),
            fmt::subject(c.description())
        )?;
        hint(
            ui,
            format!(
                "The commit is made with its conflicts. Resolve them in the working tree, then `git commit` (or `git {0} --continue`); `git {0} --abort` undoes it.",
                kind.name()
            ),
        )?;
        return Ok(1);
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn args() {
        MergeArgs::command().debug_assert();
        RebaseArgs::command().debug_assert();
        PickArgs::command().debug_assert();
    }

    #[test]
    fn merge_messages() {
        assert_eq!(
            merge_message(&["feat".to_owned()], &Head::Branch("main".to_owned())),
            "Merge branch 'feat'\n"
        );
        assert_eq!(
            merge_message(
                &["origin/main".to_owned()],
                &Head::Branch("feat".to_owned())
            ),
            "Merge remote-tracking branch 'origin/main' into feat\n"
        );
        assert_eq!(
            merge_message(
                &["a".to_owned(), "b".to_owned(), "c".to_owned()],
                &Head::Detached
            ),
            "Merge branches 'a', 'b' and 'c'\n"
        );
    }
}
