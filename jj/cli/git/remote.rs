//! git's commands that talk to ok: `clone` (a virtual workspace, as `jj clone` makes), `push`
//! (ChangeLists, as `jj cl` uploads), `pull` (`jj sync`), `fetch` (main alone, or a CL), and
//! `remote` (ok, as `origin`).

use std::io::Write as _;

use jj_cli::cli_util::CommandHelper;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::Repo as _;
use ok_store::pb;

use super::Code;
use super::Cx;
use super::fatal;
use super::front;
use super::hint;
use super::parse_or_return;
use super::repo;
use super::rev_error;

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct CloneArgs {
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(long)]
    progress: bool,
    #[arg(long = "no-progress")]
    no_progress: bool,
    #[arg(short = 'n', long = "no-checkout")]
    no_checkout: bool,
    #[arg(long)]
    bare: bool,
    #[arg(long)]
    mirror: bool,
    #[arg(short = 'l', long)]
    local: bool,
    #[arg(long = "no-local")]
    no_local: bool,
    #[arg(long = "no-hardlinks")]
    no_hardlinks: bool,
    #[arg(short = 's', long)]
    shared: bool,
    #[arg(long = "recurse-submodules", visible_alias = "recursive", num_args = 0..=1, require_equals = true, default_missing_value = ".")]
    recurse_submodules: Option<String>,
    #[arg(long = "shallow-submodules")]
    shallow_submodules: bool,
    #[arg(long = "no-shallow-submodules")]
    no_shallow_submodules: bool,
    #[arg(long = "remote-submodules")]
    remote_submodules: bool,
    #[arg(long = "no-remote-submodules")]
    no_remote_submodules: bool,
    #[arg(long)]
    depth: Option<String>,
    #[arg(long = "shallow-since")]
    shallow_since: Option<String>,
    #[arg(long = "shallow-exclude")]
    shallow_exclude: Vec<String>,
    #[arg(long = "single-branch")]
    single_branch: bool,
    #[arg(long = "no-single-branch")]
    no_single_branch: bool,
    #[arg(long = "no-tags")]
    no_tags: bool,
    #[arg(long)]
    tags: bool,
    #[arg(short = 'o', long)]
    origin: Option<String>,
    #[arg(short = 'b', long)]
    branch: Option<String>,
    #[arg(long)]
    revision: Option<String>,
    #[arg(short = 'u', long = "upload-pack")]
    upload_pack: Option<String>,
    #[arg(long)]
    template: Option<String>,
    #[arg(short = 'c', long = "config")]
    config: Vec<String>,
    #[arg(long)]
    reference: Vec<String>,
    #[arg(long = "reference-if-able")]
    reference_if_able: Vec<String>,
    #[arg(long)]
    dissociate: bool,
    #[arg(long = "separate-git-dir")]
    separate_git_dir: Option<String>,
    #[arg(long)]
    filter: Option<String>,
    #[arg(long = "also-filter-submodules")]
    also_filter_submodules: bool,
    #[arg(long)]
    sparse: bool,
    #[arg(short = 'j', long)]
    jobs: Option<String>,
    #[arg(long = "bundle-uri")]
    bundle_uri: Option<String>,
    #[arg(long = "server-option")]
    server_option: Vec<String>,
    #[arg(long = "reject-shallow")]
    reject_shallow: bool,
    #[arg(long = "no-reject-shallow")]
    no_reject_shallow: bool,
    #[arg(long = "ref-format")]
    ref_format: Option<String>,
    /// ok: a real checkout, every file on disk, instead of a virtual workspace
    #[arg(long)]
    real: bool,
    /// ok: the workspace's name
    #[arg(long)]
    workspace: Option<String>,
    /// ok: attach an existing workspace here
    #[arg(long)]
    attach: Option<String>,
    repository: String,
    directory: Option<String>,
}

/// The ok repository a clone's source names, if it names one: a repository's name (`app`,
/// `acme/app`), or a URL on the ok this machine reaches (its endpoint, or its web address).
/// A path on disk, another host, or `user@host:path` is git's.
fn ok_repository(source: &str, hosts: &[String]) -> Option<String> {
    if std::path::Path::new(source).exists() {
        return None;
    }
    if let Some((scheme, rest)) = source.split_once("://") {
        if !matches!(scheme, "http" | "https") {
            return None;
        }
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let host = authority
            .rsplit('@')
            .next()
            .unwrap_or(authority)
            .to_ascii_lowercase();
        if !hosts.contains(&host) {
            return None;
        }
        let name = path
            .trim_end_matches('/')
            .trim_end_matches(".git")
            .trim_end_matches('/')
            .to_owned();
        return is_repository_name(&name).then_some(name);
    }
    if source.contains(':') || source.contains('@') {
        return None;
    }
    is_repository_name(source).then(|| source.to_owned())
}

/// A repository's name as ok names one: one or two `/`-separated parts of lowercase letters,
/// digits, `.`, `_`, and `-`, starting and ending with a letter or digit.
fn is_repository_name(name: &str) -> bool {
    let parts: Vec<&str> = name.split('/').collect();
    parts.len() <= 2
        && parts.iter().all(|p| {
            let b = p.as_bytes();
            !b.is_empty()
                && b.len() <= 64
                && b.iter().all(|c| {
                    c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'_' | b'-')
                })
                && b[0].is_ascii_alphanumeric()
                && b[b.len() - 1].is_ascii_alphanumeric()
        })
}

/// The hosts this machine reaches ok at: its endpoint's and its web address's.
fn ok_hosts(command: &CommandHelper) -> Vec<String> {
    ["ok.endpoint", "ok.web-url"]
        .iter()
        .filter_map(|k| command.settings().get_string(*k).ok())
        .filter_map(|url| {
            let rest = url.split_once("://")?.1;
            let authority = rest.split('/').next()?;
            Some(authority.to_ascii_lowercase())
        })
        .collect()
}

/// Hands this command, as given, to git; returns only if git could not be run.
fn to_git(cx: &Cx<'_>, name: &str, args: &[String]) -> Code {
    let mut all: Vec<std::ffi::OsString> = cx.globals.forwarded.iter().map(Into::into).collect();
    all.push(name.into());
    all.extend(args.iter().map(Into::into));
    front::run_git(&all, None);
    Ok(128)
}

pub async fn clone(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    // Options git has that ok does not know mean a clone of something that is not ok's.
    let Ok(a) = <CloneArgs as clap::Parser>::try_parse_from(
        std::iter::once("git clone".to_owned()).chain(args.iter().cloned()),
    ) else {
        return to_git(cx, "clone", args);
    };
    let Some(name) = ok_repository(&a.repository, &ok_hosts(cx.command)) else {
        return to_git(cx, "clone", args);
    };
    if a.bare || a.mirror || a.separate_git_dir.is_some() {
        return fatal(
            ui,
            "an ok workspace is never bare: it is a working copy, on ok",
        );
    }
    if let Some(b) = a.branch.as_deref().or(a.revision.as_deref())
        && b != repo::MAIN
        && b != "refs/heads/main"
    {
        fatal(
            ui,
            format!("Remote branch {b} not found in upstream origin"),
        )?;
        hint(
            ui,
            "a repository on ok has one branch, main; others' work is ChangeLists (`git fetch origin cl/<n>`)",
        )?;
        return Ok(128);
    }
    let workspace = a.workspace.map(WorkspaceNameBuf::from);
    let attach = a.attach.map(WorkspaceNameBuf::from);
    // Boxed: `jj git-compat` is itself one of the commands `commands::run` runs.
    Box::pin(crate::commands::run(
        ui,
        cx.command,
        crate::commands::Command::Clone(crate::commands::CloneArgs::new(
            name,
            a.directory,
            workspace,
            a.real,
            attach,
        )),
    ))
    .await?;
    Ok(0)
}

/// The ok a workspace's repository is on, as git's remote URL: its web address and name.
pub fn origin_url(
    command: &CommandHelper,
    ws: &WorkspaceCommandHelper,
) -> Result<String, CommandError> {
    let shared = ok_cl::shared(ws.repo().as_ref()).map_err(crate::changelists::cl_error)?;
    let web = crate::commands::web_url(command, &shared.config.endpoint);
    Ok(format!(
        "{}/{}",
        web.trim_end_matches('/'),
        shared.config.repository
    ))
}

/// Whether a remote git names is ok: `origin`, or the repository's URL.
fn is_origin(command: &CommandHelper, ws: &WorkspaceCommandHelper, remote: &str) -> bool {
    remote == repo::ORIGIN
        || origin_url(command, ws).is_ok_and(|u| {
            u.trim_end_matches('/') == remote.trim_end_matches('/').trim_end_matches(".git")
        })
}

fn not_origin(ui: &Ui, remote: &str) -> Code {
    fatal(
        ui,
        format!("'{remote}' does not appear to be a git repository"),
    )?;
    hint(ui, "an ok workspace has one remote, origin: ok")?;
    Ok(128)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct PushArgs {
    #[arg(short = 'u', long = "set-upstream")]
    set_upstream: bool,
    #[arg(short = 'f', long)]
    force: bool,
    #[arg(long = "force-with-lease", num_args = 0..=1, require_equals = true, default_missing_value = "")]
    force_with_lease: Option<String>,
    #[arg(long = "no-force-with-lease")]
    no_force_with_lease: bool,
    #[arg(long = "force-if-includes")]
    force_if_includes: bool,
    #[arg(short = 'n', long = "dry-run")]
    dry_run: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(long)]
    progress: bool,
    #[arg(long = "no-progress")]
    no_progress: bool,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    branches: bool,
    #[arg(long)]
    mirror: bool,
    #[arg(long)]
    tags: bool,
    #[arg(long = "follow-tags")]
    follow_tags: bool,
    #[arg(short = 'd', long)]
    delete: bool,
    #[arg(long)]
    prune: bool,
    #[arg(short = 'o', long = "push-option")]
    push_option: Vec<String>,
    #[arg(long = "no-verify")]
    no_verify: bool,
    #[arg(long)]
    verify: bool,
    #[arg(long)]
    atomic: bool,
    #[arg(long = "no-atomic")]
    no_atomic: bool,
    #[arg(long)]
    porcelain: bool,
    #[arg(long = "recurse-submodules")]
    recurse_submodules: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    signed: Option<String>,
    #[arg(long = "no-signed")]
    no_signed: bool,
    #[arg(long)]
    thin: bool,
    #[arg(long = "no-thin")]
    no_thin: bool,
    #[arg(long)]
    repo: Option<String>,
    #[arg(long = "receive-pack", visible_alias = "exec")]
    receive_pack: Option<String>,
    #[arg(short = '4', long = "ipv4")]
    ipv4: bool,
    #[arg(short = '6', long = "ipv6")]
    ipv6: bool,
    /// ok: send the ChangeLists for review
    #[arg(long, conflicts_with = "wip")]
    ready: bool,
    /// ok: return the ChangeLists to draft
    #[arg(long)]
    wip: bool,
    /// ok: assign a reviewer (can be repeated)
    #[arg(long, value_name = "HANDLE")]
    reviewer: Vec<String>,
    repository: Option<String>,
    refspecs: Vec<String>,
}

/// What a push asks of review, from git's push options (`-o r=ada`, `-o ready`) and Gerrit's
/// `refs/for/main%r=ada,ready`.
#[derive(Default)]
struct ReviewOptions {
    reviewers: Vec<String>,
    ready: bool,
    wip: bool,
}

impl ReviewOptions {
    fn add(&mut self, option: &str) {
        let option = option.trim();
        if let Some(r) = option
            .strip_prefix("r=")
            .or_else(|| option.strip_prefix("reviewer="))
        {
            self.reviewers.push(r.to_owned());
        } else if option == "ready" {
            self.ready = true;
        } else if matches!(option, "wip" | "draft" | "private") {
            self.wip = true;
        }
    }
}

/// `git push`: HEAD (or the refspecs' sources) and its unlanded ancestors uploaded as
/// ChangeLists, one per commit, as `jj cl` uploads them. Where to is always main: a refspec's
/// destination only carries Gerrit's options.
pub async fn push(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: PushArgs = parse_or_return!(ui, "push", args);
    if a.mirror || a.prune || a.delete {
        fatal(ui, "ok deletes nothing by a push")?;
        hint(ui, "abandon a ChangeList with `jj cl abandon <n>`")?;
        return Ok(128);
    }
    let ws = cx.ws(ui).await?;
    let repo = ws.repo().as_ref();
    if let Some(remote) = a.repository.as_deref().or(a.repo.as_deref())
        && !is_origin(cx.command, &ws, remote)
    {
        return not_origin(ui, remote);
    }
    let mut options = ReviewOptions {
        reviewers: a.reviewer.clone(),
        ready: a.ready,
        wip: a.wip,
    };
    for o in &a.push_option {
        options.add(o);
    }
    let r = cx.resolver(&ws).await?;
    let mut revisions: Vec<CommitId> = Vec::new();
    if a.all || a.branches {
        revisions.extend(repo::branches(repo).into_iter().filter_map(|(_, id)| id));
    }
    let refspecs = if a.refspecs.is_empty() && revisions.is_empty() {
        vec!["HEAD".to_owned()]
    } else {
        a.refspecs.clone()
    };
    for spec in &refspecs {
        let spec = spec.strip_prefix('+').unwrap_or(spec);
        let (src, dst) = spec.split_once(':').unwrap_or((spec, ""));
        if src.is_empty() {
            fatal(ui, format!("ok deletes nothing by a push: '{spec}'"))?;
            hint(ui, "abandon a ChangeList with `jj cl abandon <n>`")?;
            return Ok(128);
        }
        if let Some((_, opts)) = dst.split_once('%') {
            for o in opts.split(',') {
                options.add(o);
            }
        }
        match r.commit_of(src).await {
            Ok(c) => revisions.push(c.id().clone()),
            Err(repo::RevError::Unborn) => {
                return super::error(ui, format!("src refspec {src} does not match any"), 1);
            }
            Err(e) => return rev_error(ui, src, e),
        }
    }
    let readiness = match (options.ready, options.wip) {
        (true, _) => pb::upload_request::Readiness::Ready,
        (_, true) => pb::upload_request::Readiness::Wip,
        _ => pb::upload_request::Readiness::Unchanged,
    };
    use crate::changelists::Upload;
    match crate::changelists::upload_changes(
        ui,
        cx.command,
        &ws,
        revisions,
        readiness,
        options.reviewers,
        a.dry_run,
    )
    .await?
    {
        Upload::Nothing | Upload::NoNewChanges => {
            if !a.quiet {
                writeln!(ui.stderr(), "Everything up-to-date")?;
            }
        }
        Upload::Done => {}
    }
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct PullArgs {
    #[arg(short = 'r', long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    rebase: Option<String>,
    #[arg(long = "no-rebase")]
    no_rebase: bool,
    #[arg(long = "ff-only")]
    ff_only: bool,
    #[arg(long)]
    ff: bool,
    #[arg(long = "no-ff")]
    no_ff: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(long)]
    autostash: bool,
    #[arg(long = "no-autostash")]
    no_autostash: bool,
    #[arg(long)]
    all: bool,
    #[arg(short = 'p', long)]
    prune: bool,
    #[arg(short = 't', long)]
    tags: bool,
    #[arg(long = "no-tags")]
    no_tags: bool,
    #[arg(long)]
    depth: Option<String>,
    #[arg(long)]
    unshallow: bool,
    #[arg(long)]
    commit: bool,
    #[arg(long = "no-commit")]
    no_commit: bool,
    #[arg(short = 'e', long)]
    edit: bool,
    #[arg(long = "no-edit")]
    no_edit: bool,
    #[arg(long)]
    squash: bool,
    #[arg(long = "no-squash")]
    no_squash: bool,
    #[arg(long)]
    stat: bool,
    #[arg(short = 'n', long = "no-stat")]
    no_stat: bool,
    #[arg(long)]
    verify: bool,
    #[arg(long = "no-verify")]
    no_verify: bool,
    #[arg(short = 's', long)]
    strategy: Option<String>,
    #[arg(short = 'X', long = "strategy-option")]
    strategy_option: Vec<String>,
    #[arg(long = "recurse-submodules", num_args = 0..=1, require_equals = true)]
    recurse_submodules: Option<String>,
    #[arg(long = "no-recurse-submodules")]
    no_recurse_submodules: bool,
    #[arg(short = 'j', long)]
    jobs: Option<String>,
    #[arg(long = "set-upstream")]
    set_upstream: bool,
    #[arg(long)]
    progress: bool,
    /// ok: in a fork, first bring it its upstream's newer work
    #[arg(long)]
    upstream: bool,
    repository: Option<String>,
    refspecs: Vec<String>,
}

/// `git pull`: `jj sync` — main brought up to date, landed changes retired, and the rest
/// rebased onto main. ok rebases always: a ChangeList is one change on one parent, so a merge
/// could not be uploaded.
pub async fn pull(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: PullArgs = parse_or_return!(ui, "pull", args);
    {
        let ws = cx.ws_read(ui).await?;
        if let Some(remote) = &a.repository
            && !is_origin(cx.command, &ws, remote)
        {
            return not_origin(ui, remote);
        }
    }
    for spec in &a.refspecs {
        let src = spec.split(':').next().unwrap_or(spec);
        let src = src.strip_prefix("refs/heads/").unwrap_or(src);
        if src != repo::MAIN && src != "HEAD" {
            fatal(ui, format!("couldn't find remote ref {spec}"))?;
            hint(
                ui,
                "a repository on ok has one branch, main; `git fetch origin cl/<n>` brings in a ChangeList",
            )?;
            return Ok(128);
        }
    }
    crate::changelists::sync(
        ui,
        cx.command,
        crate::changelists::SyncArgs::new(a.upstream),
        !a.quiet,
    )
    .await?;
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct FetchArgs {
    #[arg(long)]
    all: bool,
    #[arg(short = 'p', long)]
    prune: bool,
    #[arg(short = 'P', long = "prune-tags")]
    prune_tags: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(short = 't', long)]
    tags: bool,
    #[arg(short = 'n', long = "no-tags")]
    no_tags: bool,
    #[arg(long)]
    depth: Option<String>,
    #[arg(long)]
    deepen: Option<String>,
    #[arg(long)]
    unshallow: bool,
    #[arg(long = "shallow-since")]
    shallow_since: Option<String>,
    #[arg(long = "shallow-exclude")]
    shallow_exclude: Vec<String>,
    #[arg(short = 'f', long)]
    force: bool,
    #[arg(short = 'a', long)]
    append: bool,
    #[arg(long = "dry-run")]
    dry_run: bool,
    #[arg(short = 'j', long)]
    jobs: Option<String>,
    #[arg(short = 'm', long)]
    multiple: bool,
    #[arg(long = "recurse-submodules", num_args = 0..=1, require_equals = true)]
    recurse_submodules: Option<String>,
    #[arg(long = "no-recurse-submodules")]
    no_recurse_submodules: bool,
    #[arg(long)]
    progress: bool,
    #[arg(short = 'u', long = "update-head-ok")]
    update_head_ok: bool,
    #[arg(short = 'k', long)]
    keep: bool,
    #[arg(long)]
    filter: Option<String>,
    #[arg(long)]
    refetch: bool,
    #[arg(long = "set-upstream")]
    set_upstream: bool,
    #[arg(long = "write-fetch-head")]
    write_fetch_head: bool,
    #[arg(long = "no-write-fetch-head")]
    no_write_fetch_head: bool,
    #[arg(long = "upload-pack")]
    upload_pack: Option<String>,
    #[arg(long)]
    atomic: bool,
    #[arg(long = "no-show-forced-updates")]
    no_show_forced_updates: bool,
    #[arg(long = "show-forced-updates")]
    show_forced_updates: bool,
    repository: Option<String>,
    refspecs: Vec<String>,
}

/// The CL a refspec names: `cl/<n>`, `refs/cl/<n>`, or Gerrit's `refs/changes/<nn>/<n>/<ps>`.
fn cl_number(spec: &str) -> Option<i64> {
    let src = spec
        .trim_start_matches('+')
        .split(':')
        .next()
        .unwrap_or(spec);
    if let Some(n) = src
        .strip_prefix("refs/cl/")
        .or_else(|| src.strip_prefix("cl/"))
        .or_else(|| src.strip_prefix("origin/cl/"))
    {
        return n.parse().ok();
    }
    let rest = src.strip_prefix("refs/changes/")?;
    let mut parts = rest.split('/');
    let _shard = parts.next()?;
    parts.next()?.parse().ok()
}

/// `git fetch`: `origin/main` (`main@ok`) brought up to date, and nothing of yours moved; with a
/// `cl/<n>` refspec, that ChangeList brought in as `origin/cl/<n>` (`jj cl get`).
pub async fn fetch(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: FetchArgs = parse_or_return!(ui, "fetch", args);
    let mut ws = cx.ws(ui).await?;
    if let Some(remote) = &a.repository
        && !a.all
        && !is_origin(cx.command, &ws, remote)
    {
        return not_origin(ui, remote);
    }
    let mut cls = Vec::new();
    for spec in &a.refspecs {
        if let Some(n) = cl_number(spec) {
            cls.push(n);
            continue;
        }
        let src = spec
            .trim_start_matches('+')
            .split(':')
            .next()
            .unwrap_or(spec);
        let src = src.strip_prefix("refs/heads/").unwrap_or(src);
        if src != repo::MAIN && src != "*" && src != "HEAD" {
            fatal(ui, format!("couldn't find remote ref {spec}"))?;
            return Ok(128);
        }
    }
    let url = origin_url(cx.command, &ws)?;
    if a.dry_run {
        return Ok(0);
    }
    let immutable = crate::changelists::immutable_resolver(cx.command, &ws);
    let mut tx = ws.start_transaction();
    let fetched = ok_cl::fetch(tx.repo_mut())
        .await
        .map_err(crate::changelists::cl_error)?;
    let root = tx.repo().store().root_commit_id().clone();
    let short7 = |id: &CommitId| repo::short_hex(&id.hex()).to_owned();
    let mut lines = Vec::new();
    match &fetched.old_main {
        Some(old) if *old == fetched.main => {}
        Some(old) if *old != root => lines.push(format!(
            "   {}..{}  {:<10} -> {}/{}",
            short7(old),
            short7(&fetched.main),
            repo::MAIN,
            repo::ORIGIN,
            repo::MAIN
        )),
        _ if fetched.main != root => lines.push(format!(
            " * [new branch]      {:<10} -> {}/{}",
            repo::MAIN,
            repo::ORIGIN,
            repo::MAIN
        )),
        _ => {}
    }
    for n in cls {
        ok_cl::get(tx.repo_mut(), n, &immutable)
            .await
            .map_err(crate::changelists::cl_error)?;
        let name = ok_cl::cl_bookmark(n);
        lines.push(format!(
            " * [new ref]         {name:<10} -> {}/{name}",
            repo::ORIGIN
        ));
    }
    tx.finish(ui, "git fetch").await?;
    if !a.quiet && !lines.is_empty() {
        writeln!(ui.stderr(), "From {url}")?;
        for l in lines {
            writeln!(ui.stderr(), "{l}")?;
        }
    }
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct RemoteArgs {
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

/// `git remote`: one, `origin`, which is ok.
pub async fn remote(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: RemoteArgs = parse_or_return!(ui, "remote", args);
    let ws = cx.ws_read(ui).await?;
    let url = origin_url(cx.command, &ws)?;
    let words: Vec<&str> = a.args.iter().map(String::as_str).collect();
    let named = |rest: &[&str]| -> String {
        rest.iter()
            .find(|w| !w.starts_with('-'))
            .copied()
            .unwrap_or(repo::ORIGIN)
            .to_owned()
    };
    match words.as_slice() {
        [] => {
            let mut out = ui.stdout();
            if a.verbose {
                writeln!(out, "{}\t{url} (fetch)", repo::ORIGIN)?;
                writeln!(out, "{}\t{url} (push)", repo::ORIGIN)?;
            } else {
                writeln!(out, "{}", repo::ORIGIN)?;
            }
            Ok(0)
        }
        ["get-url", rest @ ..] => {
            let name = named(rest);
            if name != repo::ORIGIN {
                return super::error(ui, format!("No such remote '{name}'"), 2);
            }
            writeln!(ui.stdout(), "{url}")?;
            Ok(0)
        }
        ["show", rest @ ..] => {
            let name = named(rest);
            if name != repo::ORIGIN {
                return super::error(ui, format!("No such remote '{name}'"), 2);
            }
            let mut out = ui.stdout();
            writeln!(out, "* remote {}", repo::ORIGIN)?;
            writeln!(out, "  Fetch URL: {url}")?;
            writeln!(out, "  Push  URL: {url}")?;
            writeln!(out, "  HEAD branch: {}", repo::MAIN)?;
            writeln!(out, "  Remote branch:")?;
            writeln!(out, "    {} tracked", repo::MAIN)?;
            Ok(0)
        }
        ["prune", ..] | ["update", ..] | ["set-head", ..] | ["set-branches", ..] => Ok(0),
        [other, ..] => {
            fatal(
                ui,
                format!(
                    "'git remote {other}' is not available: an ok workspace has one remote, origin"
                ),
            )?;
            Ok(128)
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn args() {
        CloneArgs::command().debug_assert();
        PushArgs::command().debug_assert();
        PullArgs::command().debug_assert();
        FetchArgs::command().debug_assert();
        RemoteArgs::command().debug_assert();
    }

    #[test]
    fn clone_sources() {
        let hosts = vec!["ok.example.com".to_owned()];
        assert_eq!(ok_repository("app", &hosts), Some("app".to_owned()));
        assert_eq!(
            ok_repository("acme/app", &hosts),
            Some("acme/app".to_owned())
        );
        assert_eq!(
            ok_repository("https://ok.example.com/acme/app.git", &hosts),
            Some("acme/app".to_owned())
        );
        assert_eq!(
            ok_repository("https://OK.example.com/app/", &hosts),
            Some("app".to_owned())
        );
        assert_eq!(ok_repository("https://github.com/acme/app", &hosts), None);
        assert_eq!(ok_repository("git@github.com:acme/app.git", &hosts), None);
        assert_eq!(ok_repository("ssh://ok.example.com/app", &hosts), None);
        assert_eq!(ok_repository("Upper", &hosts), None);
        assert_eq!(ok_repository("a/b/c", &hosts), None);
        assert_eq!(ok_repository("/", &hosts), None);
    }

    #[test]
    fn cl_refspecs() {
        assert_eq!(cl_number("cl/42"), Some(42));
        assert_eq!(cl_number("refs/cl/7:refs/remotes/origin/cl/7"), Some(7));
        assert_eq!(cl_number("refs/changes/42/42/3"), Some(42));
        assert_eq!(cl_number("main"), None);
    }
}
