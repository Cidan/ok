//! `jj lock`, `jj unlock`, and `jj locks` (docs/scm.md, "Locks"): locking paths of the
//! repository so that no one else's CL changing them lands while you work on them.

use std::io::Write as _;

use jj_cli::cli_util::CommandHelper;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::user_error;
use jj_cli::command_error::user_error_with_message;
use jj_cli::ui::Ui;
use jj_lib::repo::Repo as _;
use jj_lib::repo_path::RepoPathBuf;
use ok_store::pb;
use tonic::Code;
use tonic::Status;

/// Lock paths of the repository: no one else's CL that changes them lands while you hold them
///
/// A path is a file, or a directory and everything under it, from the repository's root
/// (`//art/hero.uasset`) or from the working directory. A lock is yours for 18 hours from when you
/// take it — lock it again to start them over — until you unlock it, or a CL of yours that changes
/// it lands. Uploading is never refused: someone else's CL is, at submit.
#[derive(clap::Args, Clone, Debug)]
pub struct LockArgs {
    /// The paths
    #[arg(value_name = "PATH", required = true)]
    paths: Vec<String>,
}

/// Unlock paths you hold
///
/// `--break` ends whoever's locks of the paths: a repository administrator's, in an emergency.
#[derive(clap::Args, Clone, Debug)]
pub struct UnlockArgs {
    /// The paths
    #[arg(value_name = "PATH", required = true)]
    paths: Vec<String>,
    /// End anyone's locks of the paths (a repository administrator's)
    #[arg(long = "break")]
    break_: bool,
}

/// List the repository's locks: of the paths given, and everything under them, or all
#[derive(clap::Args, Clone, Debug)]
pub struct LocksArgs {
    /// The paths
    #[arg(value_name = "PATH")]
    paths: Vec<String>,
}

pub async fn run_lock(
    ui: &mut Ui,
    command: &CommandHelper,
    args: LockArgs,
) -> Result<(), CommandError> {
    let ws = command.workspace_helper_no_snapshot(ui).await?;
    let paths = repo_paths(&ws, &args.paths)?;
    let locks = ok_store::locks::lock(&shared(&ws)?, paths)
        .await
        .map_err(lock_error)?;
    let mut out = ui.status();
    for l in &locks {
        writeln!(out, "Locked {} until {}", l.path, when(l))?;
    }
    Ok(())
}

pub async fn run_unlock(
    ui: &mut Ui,
    command: &CommandHelper,
    args: UnlockArgs,
) -> Result<(), CommandError> {
    let ws = command.workspace_helper_no_snapshot(ui).await?;
    let paths = repo_paths(&ws, &args.paths)?;
    let shared = shared(&ws)?;
    let mut out = ui.status();
    if args.break_ {
        let broken = ok_store::locks::break_locks(&shared, paths)
            .await
            .map_err(lock_error)?;
        if broken.is_empty() {
            writeln!(out, "No one held a lock of those paths")?;
        }
        for l in &broken {
            writeln!(out, "Broke {}'s lock of {}", l.holder, l.path)?;
        }
        return Ok(());
    }
    let unlocked = ok_store::locks::unlock(&shared, paths)
        .await
        .map_err(lock_error)?;
    if unlocked.is_empty() {
        writeln!(out, "You held no lock of those paths")?;
    }
    for p in &unlocked {
        writeln!(out, "Unlocked {p}")?;
    }
    Ok(())
}

pub async fn run_locks(
    ui: &mut Ui,
    command: &CommandHelper,
    args: LocksArgs,
) -> Result<(), CommandError> {
    let ws = command.workspace_helper_no_snapshot(ui).await?;
    let shared = shared(&ws)?;
    let prefixes = if args.paths.is_empty() {
        vec![String::new()]
    } else {
        repo_paths(&ws, &args.paths)?
    };
    let mut locks: Vec<pb::FileLock> = Vec::new();
    for p in prefixes {
        for l in ok_store::locks::list(&shared, p)
            .await
            .map_err(lock_error)?
        {
            if !locks.iter().any(|k| k.path == l.path) {
                locks.push(l);
            }
        }
    }
    locks.sort_by(|a, b| a.path.cmp(&b.path));
    let mut out = ui.stdout();
    for l in &locks {
        writeln!(out, "{}\t{}\tuntil {}", l.path, l.holder, when(l))?;
    }
    Ok(())
}

/// Paths as a person writes them — from the repository's root (`//a/b`) or the working directory —
/// as the SCM names them.
fn repo_paths(ws: &WorkspaceCommandHelper, args: &[String]) -> Result<Vec<String>, CommandError> {
    args.iter()
        .map(|arg| {
            let path = match arg.strip_prefix("//") {
                Some(p) => RepoPathBuf::from_internal_string(p.trim_end_matches('/'))
                    .map_err(|e| user_error(format!("{arg} is no path: {e}")))?,
                None => ws.parse_file_path(arg)?,
            };
            if path.is_root() {
                return Err(user_error(format!(
                    "{arg} is the repository's root, which is no path to lock"
                )));
            }
            Ok(path.as_internal_file_string().to_owned())
        })
        .collect()
}

fn shared(ws: &WorkspaceCommandHelper) -> Result<std::sync::Arc<ok_store::Shared>, CommandError> {
    Ok(ws
        .repo()
        .store()
        .backend_impl::<ok_store::OkBackend>()
        .ok_or_else(|| user_error("this is not a repository on ok"))?
        .shared()
        .clone())
}

/// A lock's end, in local time.
fn when(l: &pb::FileLock) -> String {
    l.expires_at
        .as_ref()
        .and_then(|t| chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32))
        .map(|d| {
            d.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_default()
}

fn lock_error(s: Status) -> CommandError {
    if let Some(r) = ok_store::locks::rejection(&s) {
        let mut e = user_error(r.message);
        if let Some(h) = r.held {
            e = e.hinted(format!(
                "{} unlocks it with jj unlock; a repository administrator may break it with jj unlock --break",
                h.holder
            ));
        }
        return e;
    }
    match s.code() {
        Code::Unauthenticated => {
            user_error_with_message("ok did not accept this machine's credential", s)
                .hinted("Run `jj auth login`")
        }
        Code::NotFound
        | Code::InvalidArgument
        | Code::PermissionDenied
        | Code::FailedPrecondition => user_error(s.message().to_owned()),
        _ => user_error_with_message("ok", s),
    }
}
