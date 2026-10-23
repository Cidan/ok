//! ok's ChangeList lifecycle from the command line (docs/changelists.md, "From the
//! command line"): `jj cl status` (and `-f`, following it), review, comments, submit, abandon,
//! restore, rollback, a CL's checks and their logs, and its deployments — over ok control's
//! ChangeLists service (`jj/review`). Uploading stays `jj cl` itself.

use std::collections::HashMap;
use std::io::Write as _;
use std::time::Duration;

use futures::TryStreamExt as _;
use jj_cli::cli_util::CommandHelper;
use jj_cli::cli_util::RevisionArg;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::user_error;
use jj_cli::command_error::user_error_with_message;
use jj_cli::ui::Ui;
use jj_lib::backend::ChangeId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo::Repo as _;
use ok_review::Control;
use ok_review::FollowEvent;
use ok_review::Followed;
use ok_review::pb;
use ok_review::render;
use ok_review::render::Outcome;
use ok_review::render::Until;
use tonic::Code;
use tonic::Status;

/// What a person names a CL by: its number, or a revset of changes that have CLs.
#[derive(clap::Args, Clone, Debug)]
pub struct Target {
    /// The CLs: numbers, or revsets of changes uploaded as CLs (default: `@`, or `@-` when `@`
    /// has no description)
    #[arg(value_name = "CL_OR_REVSET")]
    cls: Vec<String>,
}

/// The one CL a command acts on, by option, beside the IDs it takes.
#[derive(clap::Args, Clone, Debug)]
pub struct OneCl {
    /// The CL: a number, or a revset of one change uploaded as a CL (default: `@`, or `@-` when
    /// `@` has no description)
    #[arg(long, short, value_name = "CL_OR_REVSET")]
    cl: Option<String>,
}

/// Show where CLs stand, or follow them (-f) as they move
///
/// Without -f, prints everything about each CL: its review, its pre, its submission's post, its
/// deployments, and — when something failed — what, with its log's last lines.
///
/// With -f, prints a line each time a CL moves, and exits once it is settled: finished, failed,
/// or waiting on a person. --until waits for a milestone instead (and implies -f). The exit
/// code says how it ended: 0 reached (deployed, or the milestone), 1 something failed or the CL
/// was abandoned, 2 it waits on a person — its author, a reviewer, or an admin — 3 --timeout
/// passed, 4 ok could not be asked about it.
///
/// --json prints ok control's own JSON — what its REST API answers — one object per line.
#[derive(clap::Args, Clone, Debug)]
pub struct StatusArgs {
    #[command(flatten)]
    target: Target,
    /// Follow the CLs as they move, printing a line for each change, until they are settled
    #[arg(long, short)]
    follow: bool,
    /// Follow until this milestone (implies --follow)
    #[arg(long, value_enum)]
    until: Option<Until>,
    /// Give up following after this long: 90s, 30m, 2h
    #[arg(long, value_parser = parse_duration)]
    timeout: Option<Duration>,
    /// Print ok control's JSON, one object per line
    #[arg(long)]
    json: bool,
}

/// Send draft CLs for review, assigning reviewers
#[derive(clap::Args, Clone, Debug)]
pub struct ReadyArgs {
    #[command(flatten)]
    target: Target,
    /// Assign a reviewer, by handle or <handle>@example.com (can be repeated)
    #[arg(long, short, value_name = "HANDLE")]
    reviewer: Vec<String>,
    /// A message to post with it
    #[arg(long, short)]
    message: Option<String>,
}

/// Return open CLs to draft: out of review, their reviewers and grants kept
#[derive(clap::Args, Clone, Debug)]
pub struct DraftArgs {
    #[command(flatten)]
    target: Target,
}

/// Assign reviewers to CLs, or unassign them
#[derive(clap::Args, Clone, Debug)]
pub struct ReviewersArgs {
    #[command(flatten)]
    target: Target,
    /// Assign a reviewer, by handle or <handle>@example.com (can be repeated)
    #[arg(long, value_name = "HANDLE")]
    add: Vec<String>,
    /// Unassign a reviewer (can be repeated)
    #[arg(long, value_name = "HANDLE")]
    remove: Vec<String>,
    /// A message to post with those assigned
    #[arg(long, short)]
    message: Option<String>,
}

/// Review CLs: grant or revoke your LGTM and Approval, and say something
///
/// An LGTM is anyone's but the author's (unless every OWNERS governing the CL sets allowself);
/// an Approval counts for each owner set that names you.
#[derive(clap::Args, Clone, Debug)]
pub struct ReviewArgs {
    #[command(flatten)]
    target: Target,
    /// Grant your LGTM
    #[arg(long, conflicts_with = "no_lgtm")]
    lgtm: bool,
    /// Revoke your LGTM
    #[arg(long)]
    no_lgtm: bool,
    /// Grant your Approval
    #[arg(long, conflicts_with = "no_approve")]
    approve: bool,
    /// Revoke your Approval
    #[arg(long)]
    no_approve: bool,
    /// A message to post with it
    #[arg(long, short)]
    message: Option<String>,
}

/// Comment on a CL: on the CL, a file, or a line, or in reply to a thread
#[derive(clap::Args, Clone, Debug)]
pub struct CommentArgs {
    #[command(flatten)]
    cl: OneCl,
    /// What to say
    #[arg(long, short)]
    message: String,
    /// The file to comment on (default: the CL itself)
    #[arg(long, conflicts_with = "reply")]
    file: Option<String>,
    /// The line of the file, 1-based (default: the whole file)
    #[arg(long, requires = "file")]
    line: Option<i32>,
    /// The line is on the base's side of the diff, not the patchset's
    #[arg(long, requires = "line")]
    base: bool,
    /// Reply to this thread
    #[arg(long, value_name = "THREAD")]
    reply: Option<i64>,
}

/// Resolve review threads (or reopen them)
#[derive(clap::Args, Clone, Debug)]
pub struct ResolveArgs {
    #[command(flatten)]
    cl: OneCl,
    /// The threads, by ID (`jj cl status` lists the unresolved ones)
    #[arg(required = true, value_name = "THREAD")]
    threads: Vec<i64>,
    /// Reopen them instead
    #[arg(long)]
    reopen: bool,
}

/// Submit CLs: hand them to the landing queue, which lands them once post passes
///
/// With -f (or --until), follows them after: by default until they are deployed.
#[derive(clap::Args, Clone, Debug)]
pub struct SubmitArgs {
    #[command(flatten)]
    target: Target,
    /// Submit each with its unsubmitted ancestors, bottom first
    #[arg(long)]
    stack: bool,
    /// Stop the CLs' submissions instead: nothing of them lands
    #[arg(long, conflicts_with_all = ["stack", "follow", "until"])]
    cancel: bool,
    /// Follow them once submitted, until deployed
    #[arg(long, short)]
    follow: bool,
    /// Follow them until this milestone (implies --follow)
    #[arg(long, value_enum)]
    until: Option<Until>,
    /// Give up following after this long: 90s, 30m, 2h
    #[arg(long, value_parser = parse_duration)]
    timeout: Option<Duration>,
}

/// Abandon CLs without landing them (restore reopens them)
#[derive(clap::Args, Clone, Debug)]
pub struct AbandonArgs {
    #[command(flatten)]
    target: Target,
    /// Why
    #[arg(long, short)]
    message: Option<String>,
}

/// Reopen abandoned CLs
#[derive(clap::Args, Clone, Debug)]
pub struct RestoreArgs {
    #[command(flatten)]
    target: Target,
}

/// Roll back a landed CL: a new CL undoing it (and those landed above it in its stack)
#[derive(clap::Args, Clone, Debug)]
pub struct RollbackArgs {
    #[command(flatten)]
    target: Target,
    /// Why
    #[arg(long, short)]
    message: Option<String>,
}

/// Rerun a CL's checks: the runs named, or every one of its pre that failed
#[derive(clap::Args, Clone, Debug)]
pub struct RerunArgs {
    #[command(flatten)]
    cl: OneCl,
    /// The runs, by ID (default: every failed check of its current patchset's pre)
    #[arg(value_name = "RUN")]
    runs: Vec<i64>,
}

/// Run pre by hand on CLs: a draft, which runs none until it is sent for review, or a CL under
/// its stack's top, whose pre runs there — over everything under it
#[derive(clap::Args, Clone, Debug)]
pub struct PreArgs {
    #[command(flatten)]
    target: Target,
}

/// Cancel runs of a CL's checks
#[derive(clap::Args, Clone, Debug)]
pub struct CancelArgs {
    #[command(flatten)]
    cl: OneCl,
    /// The runs, by ID
    #[arg(required = true, value_name = "RUN")]
    runs: Vec<i64>,
}

/// Print a run's log
#[derive(clap::Args, Clone, Debug)]
pub struct LogArgs {
    #[command(flatten)]
    cl: OneCl,
    /// The run, by ID
    run: i64,
    /// Keep printing what it writes until it finishes
    #[arg(long, short)]
    follow: bool,
    /// Only its last bytes
    #[arg(long, value_name = "BYTES")]
    tail: Option<i64>,
}

/// Act on a deployment of a landed CL: retry it, halt it, or allow its migration's hazards
#[derive(clap::Args, Clone, Debug)]
pub struct DeploymentArgs {
    #[command(flatten)]
    cl: OneCl,
    #[arg(value_enum)]
    action: DeploymentAction,
    /// The deployment, by ID (`jj cl status` lists them)
    deployment: i64,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum DeploymentAction {
    /// Run a deployment that failed, was halted, or was canceled again
    Retry,
    /// Stop it: one waiting is canceled, one running rolls back
    Halt,
    /// Let it apply a migration that deletes data (a repository admin)
    Allow,
}

/// `90s`, `30m`, `2h`, or seconds.
fn parse_duration(s: &str) -> Result<Duration, String> {
    let (n, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = n
        .parse()
        .map_err(|_| format!("{s:?} is not a duration (90s, 30m, 2h)"))?;
    let secs = match unit {
        "" | "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        _ => return Err(format!("{s:?} is not a duration (90s, 30m, 2h)")),
    };
    Ok(Duration::from_secs(secs))
}

/// A refusal or failure from ok control, as the user should see it.
fn control_error(s: Status) -> CommandError {
    match s.code() {
        Code::Unauthenticated => {
            user_error_with_message("ok did not accept this machine's credential", s)
                .hinted("Run `jj auth login`")
        }
        Code::NotFound
        | Code::InvalidArgument
        | Code::FailedPrecondition
        | Code::PermissionDenied => user_error(s.message().to_owned()),
        _ => user_error_with_message("ok control", s),
    }
}

/// The CLs a command names, in the workspace's repository, and a connection to ok control.
async fn resolve(
    ui: &mut Ui,
    command: &CommandHelper,
    targets: &[String],
) -> Result<(Control, Vec<pb::ClRef>), CommandError> {
    let workspace_command = command.workspace_helper(ui).await?;
    let repo = workspace_command.repo().as_ref();
    let shared = ok_cl::shared(repo).map_err(|e| user_error(e.to_string()))?;
    let repository = shared.config.repository.clone();
    let mut numbers = Vec::new();
    let mut revsets = Vec::new();
    for t in targets {
        match t.parse::<i64>() {
            Ok(n) if n > 0 => numbers.push(n),
            _ => revsets.push(RevisionArg::from(t.clone())),
        }
    }
    if targets.is_empty() {
        let Some(wc) = workspace_command.get_wc_commit_id() else {
            return Err(
                user_error("This workspace has no working-copy commit").hinted("Name the CL")
            );
        };
        let wc = workspace_command
            .repo()
            .store()
            .get_commit_async(wc)
            .await?;
        if wc.description().trim().is_empty() && wc.parent_ids().len() == 1 {
            revsets.push(RevisionArg::from("@-".to_owned()));
        } else {
            revsets.push(RevisionArg::from("@".to_owned()));
        }
    }
    if !revsets.is_empty() {
        let commits: Vec<jj_lib::commit::Commit> = workspace_command
            .parse_union_revsets(ui, &revsets)?
            .evaluate_to_commits()?
            .try_collect()
            .await?;
        let mut changes: Vec<ChangeId> = commits.iter().map(|c| c.change_id().clone()).collect();
        changes.sort();
        changes.dedup();
        let cls = ok_store::changelists::lookup_changes(&shared, &changes)
            .await
            .map_err(control_error)?;
        for change in &changes {
            let Some(cl) = cls
                .iter()
                .find(|cl| cl.change_id.as_slice() == change.as_bytes())
            else {
                return Err(user_error(format!(
                    "Change {} has no CL",
                    jj_cli::cli_util::short_change_hash(change)
                ))
                .hinted("Upload it with jj cl"));
            };
            numbers.push(cl.number);
        }
    }
    numbers.sort_unstable();
    numbers.dedup();
    let endpoint = crate::commands::web_url(command, &shared.config.endpoint);
    let control = Control::connect(&endpoint, &repository)
        .await
        .map_err(control_error)?;
    let refs = numbers
        .into_iter()
        .map(|number| pb::ClRef {
            repository: repository.clone(),
            number,
        })
        .collect();
    Ok((control, refs))
}

/// The one CL a command acts on.
async fn resolve_one(
    ui: &mut Ui,
    command: &CommandHelper,
    cl: &OneCl,
) -> Result<(Control, pb::ClRef), CommandError> {
    let targets: Vec<String> = cl.cl.iter().cloned().collect();
    let (control, mut refs) = resolve(ui, command, &targets).await?;
    if refs.len() != 1 {
        return Err(user_error(format!(
            "That names {} CLs; name one",
            refs.len()
        )));
    }
    Ok((control, refs.remove(0)))
}

/// Ends `jj cl status` when it cannot ask ok about the CLs — they do not resolve, or ok does
/// not answer — with the exit code that says so, rather than jj's own.
fn unreachable_cl(ui: &mut Ui, e: &CommandError) -> ! {
    let _ = writeln!(ui.stderr(), "Error: {}", e.error);
    for h in &e.hints {
        if let jj_cli::command_error::ErrorHint::PlainText(t) = h {
            let _ = writeln!(ui.stderr(), "Hint: {t}");
        }
    }
    exit(ui, render::UNREACHABLE)
}

/// Ends the process with code, once what was printed is out.
fn exit(ui: &mut Ui, code: i32) -> ! {
    let _ = ui.stdout().flush();
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    std::process::exit(code)
}

/// Prints where each CL stands after an action, one line each.
fn report(ui: &mut Ui, statuses: &[pb::ClStatus]) -> Result<(), CommandError> {
    for st in statuses {
        writeln!(ui.stdout(), "{}", render::line(st))?;
        if let Some(cl) = &st.cl {
            writeln!(ui.stdout(), "  {}", cl.url)?;
        }
    }
    Ok(())
}

pub async fn run_status(
    ui: &mut Ui,
    command: &CommandHelper,
    args: StatusArgs,
) -> Result<(), CommandError> {
    let (control, cls) = match resolve(ui, command, &args.target.cls).await {
        Ok(r) => r,
        Err(e) => unreachable_cl(ui, &e),
    };
    if args.follow || args.until.is_some() {
        follow(
            ui,
            &control,
            cls,
            args.until.unwrap_or(Until::Settled),
            args.json,
            args.timeout,
        )
        .await
    }
    for cl in cls {
        if args.json {
            let mut rx = control.rest(
                "Status",
                serde_json::json!({ "repository": cl.repository, "number": cl.number }),
            );
            while let Some(ev) = rx.recv().await {
                match ev {
                    ok_review::Event::Item(line) => writeln!(ui.stdout(), "{line}")?,
                    ok_review::Event::End => break,
                    ok_review::Event::Failed(s) => {
                        writeln!(ui.stderr(), "cl/{}: {}", cl.number, s.message())?;
                        exit(ui, render::UNREACHABLE)
                    }
                }
            }
            continue;
        }
        match control.status(cl.clone()).await {
            Ok(st) => write!(ui.stdout(), "{}", render::full(&st))?,
            Err(s) => {
                writeln!(ui.stderr(), "cl/{}: {}", cl.number, s.message())?;
                exit(ui, render::UNREACHABLE)
            }
        }
    }
    Ok(())
}

/// Follows CLs until each is where until asks, or settles short of it, and exits with how they
/// ended: the worst of them (a failure, then waiting on a person, then reached).
async fn follow(
    ui: &mut Ui,
    control: &Control,
    cls: Vec<pb::ClRef>,
    until: Until,
    json: bool,
    timeout: Option<Duration>,
) -> ! {
    let total = cls.len();
    let mut rx = control.follow(cls, json, timeout);
    let mut last: HashMap<i64, String> = HashMap::new();
    let mut done: HashMap<i64, Outcome> = HashMap::new();
    while done.len() < total {
        let Some(ev) = rx.recv().await else { break };
        let (n, what) = match ev {
            FollowEvent::TimedOut => {
                let _ = writeln!(
                    ui.stderr(),
                    "timed out following: {} of {total} CLs settled",
                    done.len()
                );
                exit(ui, render::TIMED_OUT)
            }
            FollowEvent::Cl(n, what) => (n, what),
        };
        if done.contains_key(&n) {
            continue;
        }
        let (point, failure) = match what {
            Followed::Status(st) => {
                let l = render::line(&st);
                if last.get(&n) != Some(&l) {
                    let _ = writeln!(ui.stdout(), "{l}");
                    last.insert(n, l);
                }
                (render::Point::from(&*st), render::failure(&st))
            }
            Followed::Json(line, point) => {
                let _ = writeln!(ui.stdout(), "{line}");
                (point, String::new())
            }
            Followed::Failed(s) => {
                let _ = writeln!(ui.stderr(), "cl/{n}: {}", s.message());
                exit(ui, render::UNREACHABLE)
            }
        };
        if let Some(o) = render::outcome(point, until) {
            if o == Outcome::Failed && !failure.is_empty() {
                let _ = write!(ui.stdout(), "{failure}");
            }
            done.insert(n, o);
        }
    }
    let worst = done.values().copied().max().unwrap_or(Outcome::Reached);
    exit(ui, worst.code())
}

pub async fn run_ready(
    ui: &mut Ui,
    command: &CommandHelper,
    args: ReadyArgs,
) -> Result<(), CommandError> {
    let (control, cls) = resolve(ui, command, &args.target.cls).await?;
    let mut out = Vec::new();
    // Highest first: a stack's CLs are numbered from the bottom up, and pre runs at its top in
    // review, so the top is in review before any CL under it.
    for cl in cls.into_iter().rev() {
        let req = pb::ReviewersRequest {
            cl: Some(cl),
            reviewers: args.reviewer.clone(),
            message: args.message.clone().unwrap_or_default(),
        };
        out.push(control.request_review(req).await.map_err(control_error)?);
    }
    out.reverse();
    report(ui, &out)
}

pub async fn run_draft(
    ui: &mut Ui,
    command: &CommandHelper,
    args: DraftArgs,
) -> Result<(), CommandError> {
    let (control, cls) = resolve(ui, command, &args.target.cls).await?;
    let mut out = Vec::new();
    for cl in cls {
        out.push(control.return_to_draft(cl).await.map_err(control_error)?);
    }
    report(ui, &out)
}

pub async fn run_reviewers(
    ui: &mut Ui,
    command: &CommandHelper,
    args: ReviewersArgs,
) -> Result<(), CommandError> {
    if args.add.is_empty() && args.remove.is_empty() {
        return Err(
            user_error("Nothing to change").hinted("Assign with --add, unassign with --remove")
        );
    }
    let (control, cls) = resolve(ui, command, &args.target.cls).await?;
    let mut out = Vec::new();
    for cl in cls {
        let mut st = None;
        if !args.add.is_empty() {
            let req = pb::ReviewersRequest {
                cl: Some(cl.clone()),
                reviewers: args.add.clone(),
                message: args.message.clone().unwrap_or_default(),
            };
            st = Some(control.add_reviewers(req).await.map_err(control_error)?);
        }
        if !args.remove.is_empty() {
            let req = pb::ReviewersRequest {
                cl: Some(cl.clone()),
                reviewers: args.remove.clone(),
                message: String::new(),
            };
            st = Some(control.remove_reviewers(req).await.map_err(control_error)?);
        }
        out.extend(st);
    }
    report(ui, &out)
}

pub async fn run_review(
    ui: &mut Ui,
    command: &CommandHelper,
    args: ReviewArgs,
) -> Result<(), CommandError> {
    let mut grants = Vec::new();
    let mut grant = |label: pb::Label, granted: bool| {
        grants.push(pb::reply_request::Grant {
            label: label as i32,
            granted,
        })
    };
    if args.lgtm || args.no_lgtm {
        grant(pb::Label::Lgtm, args.lgtm);
    }
    if args.approve || args.no_approve {
        grant(pb::Label::Approval, args.approve);
    }
    if grants.is_empty() && args.message.is_none() {
        return Err(user_error("Nothing to say").hinted("Grant with --lgtm or --approve, revoke with --no-lgtm or --no-approve, or say something with -m"));
    }
    let (control, cls) = resolve(ui, command, &args.target.cls).await?;
    let mut out = Vec::new();
    for cl in cls {
        let req = pb::ReplyRequest {
            cl: Some(cl),
            grants: grants.clone(),
            message: args.message.clone().unwrap_or_default(),
        };
        out.push(control.reply(req).await.map_err(control_error)?);
    }
    report(ui, &out)
}

pub async fn run_comment(
    ui: &mut Ui,
    command: &CommandHelper,
    args: CommentArgs,
) -> Result<(), CommandError> {
    let (control, cl) = resolve_one(ui, command, &args.cl).await?;
    let side = if args.base {
        pb::comment_request::Side::Base
    } else {
        pb::comment_request::Side::Patchset
    };
    let req = pb::CommentRequest {
        cl: Some(cl),
        body: args.message,
        reply_to: args.reply.unwrap_or_default(),
        file: args.file.unwrap_or_default(),
        line: args.line.unwrap_or_default(),
        side: side as i32,
        patchset: 0,
    };
    let st = control.comment(req).await.map_err(control_error)?;
    report(ui, &[st])
}

pub async fn run_resolve(
    ui: &mut Ui,
    command: &CommandHelper,
    args: ResolveArgs,
) -> Result<(), CommandError> {
    let (control, cl) = resolve_one(ui, command, &args.cl).await?;
    let mut last = None;
    for thread in args.threads {
        let req = pb::ResolveRequest {
            cl: Some(cl.clone()),
            thread,
            resolved: !args.reopen,
        };
        last = Some(control.resolve(req).await.map_err(control_error)?);
    }
    report(ui, &last.into_iter().collect::<Vec<_>>())
}

pub async fn run_submit(
    ui: &mut Ui,
    command: &CommandHelper,
    args: SubmitArgs,
) -> Result<(), CommandError> {
    let (control, cls) = resolve(ui, command, &args.target.cls).await?;
    let mut out = Vec::new();
    for cl in &cls {
        let st = if args.cancel {
            control.cancel_submit(cl.clone()).await
        } else {
            control
                .submit(pb::SubmitRequest {
                    cl: Some(cl.clone()),
                    stack: args.stack,
                })
                .await
        };
        out.push(st.map_err(control_error)?);
    }
    report(ui, &out)?;
    if args.follow || args.until.is_some() {
        follow(
            ui,
            &control,
            cls,
            args.until.unwrap_or(Until::Deployed),
            false,
            args.timeout,
        )
        .await
    }
    Ok(())
}

pub async fn run_abandon(
    ui: &mut Ui,
    command: &CommandHelper,
    args: AbandonArgs,
) -> Result<(), CommandError> {
    let (control, cls) = resolve(ui, command, &args.target.cls).await?;
    let mut out = Vec::new();
    for cl in cls {
        out.push(
            control
                .abandon(pb::AbandonRequest {
                    cl: Some(cl),
                    reason: args.message.clone().unwrap_or_default(),
                })
                .await
                .map_err(control_error)?,
        );
    }
    report(ui, &out)
}

pub async fn run_restore(
    ui: &mut Ui,
    command: &CommandHelper,
    args: RestoreArgs,
) -> Result<(), CommandError> {
    let (control, cls) = resolve(ui, command, &args.target.cls).await?;
    let mut out = Vec::new();
    for cl in cls {
        out.push(control.restore(cl).await.map_err(control_error)?);
    }
    report(ui, &out)
}

pub async fn run_rollback(
    ui: &mut Ui,
    command: &CommandHelper,
    args: RollbackArgs,
) -> Result<(), CommandError> {
    let (control, cls) = resolve(ui, command, &args.target.cls).await?;
    let mut out = Vec::new();
    for cl in cls {
        out.push(
            control
                .rollback(pb::RollbackRequest {
                    cl: Some(cl),
                    reason: args.message.clone().unwrap_or_default(),
                })
                .await
                .map_err(control_error)?,
        );
    }
    report(ui, &out)
}

pub async fn run_rerun(
    ui: &mut Ui,
    command: &CommandHelper,
    args: RerunArgs,
) -> Result<(), CommandError> {
    let (control, cl) = resolve_one(ui, command, &args.cl).await?;
    let failed = args.runs.is_empty();
    let st = control
        .rerun(pb::RunsRequest {
            cl: Some(cl),
            runs: args.runs,
            failed,
        })
        .await
        .map_err(control_error)?;
    report(ui, &[st])
}

pub async fn run_pre(
    ui: &mut Ui,
    command: &CommandHelper,
    args: PreArgs,
) -> Result<(), CommandError> {
    let (control, cls) = resolve(ui, command, &args.target.cls).await?;
    let mut out = Vec::new();
    for cl in cls {
        out.push(control.run_pre(cl).await.map_err(control_error)?);
    }
    report(ui, &out)
}

pub async fn run_cancel(
    ui: &mut Ui,
    command: &CommandHelper,
    args: CancelArgs,
) -> Result<(), CommandError> {
    let (control, cl) = resolve_one(ui, command, &args.cl).await?;
    let st = control
        .cancel_runs(pb::RunsRequest {
            cl: Some(cl),
            runs: args.runs,
            failed: false,
        })
        .await
        .map_err(control_error)?;
    report(ui, &[st])
}

pub async fn run_log(
    ui: &mut Ui,
    command: &CommandHelper,
    args: LogArgs,
) -> Result<(), CommandError> {
    let (control, cl) = resolve_one(ui, command, &args.cl).await?;
    let offset = args.tail.map_or(0, |b| -b.abs());
    let mut rx = control.read_log(pb::ReadLogRequest {
        cl: Some(cl),
        run: args.run,
        offset,
        follow: args.follow,
    });
    while let Some(ev) = rx.recv().await {
        match ev {
            ok_review::Event::Item(chunk) => {
                ui.stdout().write_all(chunk.data.as_bytes())?;
                if chunk.end {
                    break;
                }
            }
            ok_review::Event::End => break,
            ok_review::Event::Failed(s) => return Err(control_error(s)),
        }
    }
    Ok(())
}

pub async fn run_deployment(
    ui: &mut Ui,
    command: &CommandHelper,
    args: DeploymentArgs,
) -> Result<(), CommandError> {
    let (control, cl) = resolve_one(ui, command, &args.cl).await?;
    let req = pb::DeploymentRequest {
        cl: Some(cl),
        deployment: args.deployment,
    };
    let st = match args.action {
        DeploymentAction::Retry => control.retry_deployment(req).await,
        DeploymentAction::Halt => control.halt_deployment(req).await,
        DeploymentAction::Allow => control.allow_hazards(req).await,
    };
    report(ui, &[st.map_err(control_error)?])
}
