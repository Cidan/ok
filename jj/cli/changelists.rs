//! ok's ChangeList commands (docs/scm.md, "Commands"): `jj cl`, `jj cl get`, and
//! `jj sync`, over the workflow in `jj/cl`, and the rest of a CL's lifecycle — status,
//! review, submit, and the rest — in `lifecycle.rs`.

use std::io::Write as _;
use std::sync::Arc;

use futures::TryStreamExt as _;
use jj_cli::cli_util::CommandHelper;
use jj_cli::cli_util::RevisionArg;
use jj_cli::cli_util::short_change_hash;
use jj_cli::cli_util::short_commit_hash;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::internal_error;
use jj_cli::command_error::user_error;
use jj_cli::command_error::user_error_with_message;
use jj_cli::revset_util::RevsetExpressionEvaluator;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::commit::Commit;
use jj_lib::id_prefix::IdPrefixContext;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo::Repo;
use jj_lib::revset::ResolvedRevsetExpression;
use jj_lib::revset::RevsetExpression;
use ok_cl::ClError;
use ok_cl::Followed;
use ok_cl::NotUploadable;
use ok_store::pb;
use tonic::Code;
use tonic::Status;

/// Upload changes to ok as ChangeLists
///
/// Uploads the change and every mutable ancestor — everything between it and main (or a
/// CL you brought in with `jj cl get`), bottom first — one CL per change. A change without
/// a CL gets a new one, a draft unless `--ready`; a change whose CL has an older patchset
/// gets a new patchset. Review and submit with the subcommands below (`jj cl status`,
/// `jj cl review`, `jj cl submit`, …), or on ok.
#[derive(clap::Args, Clone, Debug)]
#[command(args_conflicts_with_subcommands = true)]
pub struct ClArgs {
    #[command(subcommand)]
    command: Option<ClCommand>,
    /// The changes to upload, with their mutable ancestors (default: `@`, or `@-` when `@`
    /// has no description)
    #[arg(long = "revision", short, value_name = "REVSETS")]
    revisions: Vec<RevisionArg>,
    /// Send the CLs for review (a new CL starts open instead of as a draft)
    #[arg(long, conflicts_with = "wip")]
    ready: bool,
    /// Return the CLs to draft
    #[arg(long)]
    wip: bool,
    /// Assign a reviewer, by handle or <handle>@example.com (can be repeated)
    #[arg(long, value_name = "HANDLE")]
    reviewer: Vec<String>,
}

#[derive(clap::Subcommand, Clone, Debug)]
enum ClCommand {
    Get(GetArgs),
    Status(crate::lifecycle::StatusArgs),
    Ready(crate::lifecycle::ReadyArgs),
    Draft(crate::lifecycle::DraftArgs),
    Reviewers(crate::lifecycle::ReviewersArgs),
    Review(crate::lifecycle::ReviewArgs),
    Comment(crate::lifecycle::CommentArgs),
    Resolve(crate::lifecycle::ResolveArgs),
    Submit(crate::lifecycle::SubmitArgs),
    Abandon(crate::lifecycle::AbandonArgs),
    Restore(crate::lifecycle::RestoreArgs),
    Rollback(crate::lifecycle::RollbackArgs),
    Rerun(crate::lifecycle::RerunArgs),
    Cancel(crate::lifecycle::CancelArgs),
    Log(crate::lifecycle::LogArgs),
    Deployment(crate::lifecycle::DeploymentArgs),
}

/// Bring someone else's CL into your view as `cl/<n>@ok`
///
/// The CL's current patchset becomes the immutable remote bookmark `cl/<n>@ok`. Build on
/// it with `jj new cl/<n>@ok`; your changes upload as CLs stacked on it, and `jj sync`
/// moves it (and them) to each new patchset, and onto main once it lands.
#[derive(clap::Args, Clone, Debug)]
struct GetArgs {
    /// The CL's number
    number: i64,
}

/// Bring your repository up to date with ok
///
/// Moves `main@ok` to ok's main, retires your changes whose CLs landed (keeping any
/// edits you made after the patchset that landed), moves each `cl/<n>@ok` to its CL's
/// current patchset, and rebases the rest of your changes onto main. Conflicts become
/// jj conflicts; changes the rebase empties are abandoned.
#[derive(clap::Args, Clone, Debug)]
pub struct SyncArgs {}

/// The web page of CL `number` in the repository of `repo`.
fn cl_url(command: &CommandHelper, repo: &dyn Repo, number: i64) -> Result<String, CommandError> {
    let shared = ok_cl::shared(repo).map_err(cl_error)?;
    let web = crate::commands::web_url(command, &shared.config.endpoint);
    Ok(format!(
        "{}/{}/cl/{number}",
        web.trim_end_matches('/'),
        shared.config.repository
    ))
}

/// A workflow failure, as the user should see it.
fn cl_error(e: ClError) -> CommandError {
    match e {
        ClError::NotOk => user_error(e.to_string())
            .hinted("ok's commands work in a repository made by jj clone or jj create"),
        ClError::Scm(s) => scm_error(s),
        ClError::Landed { number, commit } => user_error(format!(
            "CL {number} has landed on main as {}",
            short_commit_hash(&commit)
        ))
        .hinted("jj sync brings main up to date"),
        ClError::AlreadyInView { number, commit } => user_error(format!(
            "CL {number}'s change is already in your view, as {}",
            short_commit_hash(&commit)
        ))
        .hinted("Your own changes upload as CLs with jj cl; there is nothing to get"),
        ClError::Publish(e) => user_error_with_message("could not publish to ok", e),
        other => internal_error(other),
    }
}

fn scm_error(s: Status) -> CommandError {
    match s.code() {
        Code::Unauthenticated => {
            user_error_with_message("ok did not accept this machine's credential", s)
                .hinted("Run `jj auth login`")
        }
        Code::NotFound | Code::InvalidArgument | Code::FailedPrecondition => {
            user_error(s.message().to_owned())
        }
        _ => user_error_with_message("ok", s),
    }
}

pub async fn run_cl(
    ui: &mut Ui,
    command: &CommandHelper,
    args: ClArgs,
) -> Result<(), CommandError> {
    use crate::lifecycle as l;
    match args.command {
        Some(ClCommand::Get(a)) => return run_get(ui, command, a).await,
        Some(ClCommand::Status(a)) => return l::run_status(ui, command, a).await,
        Some(ClCommand::Ready(a)) => return l::run_ready(ui, command, a).await,
        Some(ClCommand::Draft(a)) => return l::run_draft(ui, command, a).await,
        Some(ClCommand::Reviewers(a)) => return l::run_reviewers(ui, command, a).await,
        Some(ClCommand::Review(a)) => return l::run_review(ui, command, a).await,
        Some(ClCommand::Comment(a)) => return l::run_comment(ui, command, a).await,
        Some(ClCommand::Resolve(a)) => return l::run_resolve(ui, command, a).await,
        Some(ClCommand::Submit(a)) => return l::run_submit(ui, command, a).await,
        Some(ClCommand::Abandon(a)) => return l::run_abandon(ui, command, a).await,
        Some(ClCommand::Restore(a)) => return l::run_restore(ui, command, a).await,
        Some(ClCommand::Rollback(a)) => return l::run_rollback(ui, command, a).await,
        Some(ClCommand::Rerun(a)) => return l::run_rerun(ui, command, a).await,
        Some(ClCommand::Cancel(a)) => return l::run_cancel(ui, command, a).await,
        Some(ClCommand::Log(a)) => return l::run_log(ui, command, a).await,
        Some(ClCommand::Deployment(a)) => return l::run_deployment(ui, command, a).await,
        None => {}
    }
    let workspace_command = command.workspace_helper(ui).await?;
    let revisions: Vec<CommitId> = if args.revisions.is_empty() {
        let Some(wc) = workspace_command.get_wc_commit_id() else {
            return Err(user_error("This workspace has no working-copy commit")
                .hinted("Name the changes to upload with -r"));
        };
        let wc = workspace_command
            .repo()
            .store()
            .get_commit_async(wc)
            .await?;
        if wc.description().trim().is_empty() {
            if wc.parent_ids().len() != 1 {
                return Err(user_error(
                    "@ has no description and is a merge, so there is no default change to upload",
                )
                .hinted("Name the changes to upload with -r"));
            }
            writeln!(ui.status(), "@ has no description; uploading @-")?;
            wc.parent_ids().to_vec()
        } else {
            vec![wc.id().clone()]
        }
    } else {
        workspace_command
            .parse_union_revsets(ui, &args.revisions)?
            .evaluate_to_commit_ids()?
            .try_collect()
            .await?
    };
    let mut to_upload: Vec<Commit> = workspace_command
        .attach_revset_evaluator(
            workspace_command
                .env()
                .immutable_expression()
                .range(&RevsetExpression::commits(revisions)),
        )
        .evaluate_to_commits()?
        .try_collect()
        .await?;
    to_upload.reverse();
    if to_upload.is_empty() {
        writeln!(
            ui.status(),
            "Nothing to upload: those changes are already on main (or immutable)"
        )?;
        return Ok(());
    }
    let repo = workspace_command.repo().as_ref();
    if let Some((c, why)) = ok_cl::check_uploadable(repo, &to_upload)
        .await
        .map_err(cl_error)?
    {
        let change = short_change_hash(c.change_id());
        return Err(match why {
            NotUploadable::Merge => user_error(format!(
                "Change {change} is a merge; a CL is one change on one parent"
            ))
            .hinted("Rebase it onto a single parent"),
            NotUploadable::Conflicted => {
                user_error(format!("Change {change} has unresolved conflicts"))
                    .hinted("Resolve them with jj resolve, then upload again")
            }
            NotUploadable::NoDescription => {
                user_error(format!("Change {change} has no description"))
                    .hinted(format!("Describe it with jj describe {change}"))
            }
            NotUploadable::Empty => user_error(format!("Change {change} changes nothing")).hinted(
                format!("Abandon it with jj abandon {change}, or upload its parent"),
            ),
        });
    }
    let readiness = match (args.ready, args.wip) {
        (true, _) => pb::upload_request::Readiness::Ready,
        (_, true) => pb::upload_request::Readiness::Wip,
        _ => pb::upload_request::Readiness::Unchanged,
    };
    let response = match ok_cl::upload(repo, &to_upload, readiness, args.reviewer).await {
        Ok(r) => r,
        Err(ClError::Scm(s)) => return Err(upload_error(&to_upload, s)),
        Err(e) => return Err(cl_error(e)),
    };
    let mut formatter = ui.stdout_formatter();
    for (commit, result) in to_upload.iter().zip(&response.results) {
        let url = cl_url(command, repo, result.number)?;
        let what = match pb::upload_response::result::Outcome::try_from(result.outcome) {
            Ok(pb::upload_response::result::Outcome::Created) => {
                format!("CL {} created", result.number)
            }
            Ok(pb::upload_response::result::Outcome::NewPatchset) => {
                format!("CL {} patchset {}", result.number, result.patchset)
            }
            _ => format!("CL {} unchanged", result.number),
        };
        write!(formatter, "{what}  {url}\n    ")?;
        workspace_command.write_commit_summary(formatter.as_mut(), commit)?;
        writeln!(formatter)?;
    }
    Ok(())
}

/// An upload the SCM refused, naming the change as the user knows it.
fn upload_error(commits: &[Commit], s: Status) -> CommandError {
    let Some(rej) = ok_store::changelists::upload_rejection(&s) else {
        return scm_error(s);
    };
    if rej.reason == pb::upload_rejection::Reason::NoNewChanges as i32 {
        return user_error(
            "Nothing new to upload: every change is already its CL's current patchset",
        )
        .hinted("To send a CL for review without uploading, jj cl ready; to assign reviewers, jj cl reviewers --add");
    }
    let change = commits
        .iter()
        .find(|c| c.id().as_bytes() == rej.commit_id.as_slice())
        .map(|c| format!("Change {}", short_change_hash(c.change_id())));
    let mut message = match change {
        Some(change) => format!("{change} cannot be uploaded: {}", rej.message),
        None => format!("Cannot upload: {}", rej.message),
    };
    for p in &rej.problems {
        message.push_str("\n  ");
        message.push_str(p);
    }
    user_error(message)
}

/// The user's immutable set (`::immutable_heads()`), resolvable against any repository
/// state of a transaction.
fn immutable_resolver(
    command: &CommandHelper,
    workspace_command: &jj_cli::cli_util::WorkspaceCommandHelper,
) -> impl Fn(&dyn Repo) -> Result<Arc<ResolvedRevsetExpression>, ClError> + use<> {
    let expression = workspace_command.env().immutable_expression();
    let extensions = command.revset_extensions().clone();
    let id_prefix_context = IdPrefixContext::new(extensions.clone());
    move |repo: &dyn Repo| {
        RevsetExpressionEvaluator::new(
            repo,
            extensions.clone(),
            &id_prefix_context,
            expression.clone(),
        )
        .resolve()
        .map_err(ClError::from)
    }
}

async fn run_get(ui: &mut Ui, command: &CommandHelper, args: GetArgs) -> Result<(), CommandError> {
    let mut workspace_command = command.workspace_helper(ui).await?;
    let immutable = immutable_resolver(command, &workspace_command);
    let mut tx = workspace_command.start_transaction();
    let got = ok_cl::get(tx.repo_mut(), args.number, &immutable)
        .await
        .map_err(cl_error)?;
    let url = cl_url(command, tx.repo(), args.number)?;
    if let Some(mut formatter) = ui.status_formatter() {
        write!(
            formatter,
            "CL {} patchset {} is {}@{}: ",
            args.number,
            got.change_list.current_patchset,
            ok_cl::cl_bookmark(args.number),
            ok_cl::REMOTE
        )?;
        tx.write_commit_summary(formatter.as_mut(), &got.commit)?;
        writeln!(formatter, "\n  {url}")?;
        if got.change_list.status == pb::ChangeListStatus::Abandoned as i32 {
            writeln!(formatter, "  CL {} is abandoned", args.number)?;
        }
    }
    tx.finish(ui, format!("get CL {}", args.number)).await
}

fn changes(n: usize) -> String {
    if n == 1 {
        "1 change".to_owned()
    } else {
        format!("{n} changes")
    }
}

pub async fn run_sync(
    ui: &mut Ui,
    command: &CommandHelper,
    _args: SyncArgs,
) -> Result<(), CommandError> {
    let mut workspace_command = command.workspace_helper(ui).await?;
    let immutable = immutable_resolver(command, &workspace_command);
    let mut tx = workspace_command.start_transaction();
    let report = ok_cl::sync(tx.repo_mut(), &immutable)
        .await
        .map_err(cl_error)?;
    if let Some(mut formatter) = ui.status_formatter() {
        if let Some(main) = &report.main
            && report.old_main.as_ref() != Some(main)
        {
            write!(formatter, "main is at ")?;
            let commit = tx.repo().store().get_commit_async(main).await?;
            tx.write_commit_summary(formatter.as_mut(), &commit)?;
            writeln!(formatter, " ({} new)", report.fetched)?;
        }
        for r in &report.retired {
            let change = short_change_hash(r.local.change_id());
            match &r.kept {
                None => writeln!(
                    formatter,
                    "CL {} landed as {}: retired change {change}",
                    r.number,
                    short_commit_hash(&r.landed)
                )?,
                Some(kept) => writeln!(
                    formatter,
                    "CL {} landed as {}: your later edits to {change} are now change {}",
                    r.number,
                    short_commit_hash(&r.landed),
                    short_change_hash(kept.change_id())
                )?,
            }
        }
        for f in &report.followed {
            match f {
                Followed::Moved { number, patchset } => writeln!(
                    formatter,
                    "{}@{} moved to patchset {patchset}",
                    ok_cl::cl_bookmark(*number),
                    ok_cl::REMOTE
                )?,
                Followed::Landed { number, commit } => writeln!(
                    formatter,
                    "CL {number} landed as {}; {}@{} is gone",
                    short_commit_hash(commit),
                    ok_cl::cl_bookmark(*number),
                    ok_cl::REMOTE
                )?,
            }
        }
        if report.rebased > 0 {
            writeln!(formatter, "Rebased {}", changes(report.rebased))?;
        }
        if report.emptied > 0 {
            writeln!(
                formatter,
                "Abandoned {} the rebase emptied",
                changes(report.emptied)
            )?;
        }
        for (number, commit) in &report.abandoned {
            writeln!(
                formatter,
                "CL {number} is abandoned; change {} is left as it is (restore the CL on ok, or jj abandon {})",
                short_change_hash(commit.change_id()),
                short_change_hash(commit.change_id())
            )?;
        }
    }
    tx.finish(ui, "sync with ok").await
}
