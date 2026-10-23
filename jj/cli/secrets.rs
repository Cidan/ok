//! `jj secret` (docs/secrets.md, "Unsealing", "Clients"): unsealing a repository's secrets, listing
//! one's unseals, and reading and writing a secret from a workspace whose files are its pointers —
//! a real checkout, or a virtual one on macOS.

use std::io::Read as _;
use std::io::Write as _;
use std::time::Duration;
use std::time::Instant;

use futures::AsyncReadExt as _;
use jj_cli::cli_util::CommandHelper;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::user_error;
use jj_cli::command_error::user_error_with_message;
use jj_cli::ui::Ui;
use jj_lib::backend::TreeValue;
use jj_lib::repo::Repo as _;
use jj_lib::repo_path::RepoPathBuf;
use ok_secret::SecretBuf;
use ok_store::secrets::UnsealState;
use tonic::Code;
use tonic::Status;

/// Use a repository's secrets
///
/// A secret's path is from the repository's root (`//secret/prod/db-password`) or from the
/// working directory. Reading a secret uses an unseal, good for one read for ten minutes; a folder
/// may ask for a passkey to unseal its secrets.
#[derive(clap::Subcommand, Clone, Debug)]
pub enum SecretCommand {
    Unseal(UnsealArgs),
    Status(StatusArgs),
    Cat(CatArgs),
    Write(WriteArgs),
}

/// Unseal secrets, each for one read
///
/// Where a folder asks for a passkey, approve on the page this prints (and opens); the command
/// waits until you do.
#[derive(clap::Args, Clone, Debug)]
pub struct UnsealArgs {
    /// The secrets
    #[arg(value_name = "PATH", required = true)]
    paths: Vec<String>,
    /// Print the approval page's address without opening a browser
    #[arg(long)]
    no_browser: bool,
}

/// List your unused unseals in this repository
#[derive(clap::Args, Clone, Debug)]
pub struct StatusArgs {}

/// Print a secret's value, as the working-copy commit's pointer names it
///
/// It unseals the secret and reads it at once. In a virtual workspace on Linux, the secret's file
/// reads as its value already: open it after `jj secret unseal`.
#[derive(clap::Args, Clone, Debug)]
pub struct CatArgs {
    /// The secret
    #[arg(value_name = "PATH")]
    path: String,
    /// Print the approval page's address without opening a browser
    #[arg(long)]
    no_browser: bool,
}

/// Keep a new value of a secret, from standard input, and write its pointer to the working copy
///
/// The value is the input's bytes exactly: `printf %s 'value' | jj secret write //secret/x`. Its
/// pointer is in the working copy, to upload as a CL, which a writer of its folder may land.
#[derive(clap::Args, Clone, Debug)]
pub struct WriteArgs {
    /// The secret
    #[arg(value_name = "PATH")]
    path: String,
    /// Read the value from this file instead of standard input
    #[arg(long, value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
    file: Option<std::path::PathBuf>,
}

fn secret_error(s: Status) -> CommandError {
    match s.code() {
        Code::Unauthenticated => {
            user_error_with_message("ok did not accept this machine's credential", s)
                .hinted("Run `jj auth login`")
        }
        Code::NotFound
        | Code::InvalidArgument
        | Code::FailedPrecondition
        | Code::PermissionDenied => user_error(s.message().to_owned()),
        _ => user_error_with_message("ok secrets", s),
    }
}

/// A secret's path, as a person writes it: from the repository's root (`//secret/x`) or from the
/// working directory.
fn secret_path(ws: &WorkspaceCommandHelper, arg: &str) -> Result<RepoPathBuf, CommandError> {
    let path = match arg.strip_prefix("//") {
        Some(p) => RepoPathBuf::from_internal_string(p)
            .map_err(|e| user_error(format!("{arg} is no path: {e}")))?,
        None => ws.parse_file_path(arg)?,
    };
    if !ok_secret::valid_path(path.as_internal_file_string()) {
        return Err(user_error(format!(
            "{arg} is no secret's path: a file under //{}/, neither an {} nor an OWNERS file",
            ok_secret::DIR,
            ok_secret::ACL_FILE
        )));
    }
    Ok(path)
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

/// Opens a URL in the user's browser, when there is a display to show it on.
fn open_browser(url: &str) -> bool {
    let cmd = if cfg!(target_os = "macos") {
        "open"
    } else if std::env::var_os("DISPLAY").is_some() || std::env::var_os("WAYLAND_DISPLAY").is_some()
    {
        "xdg-open"
    } else {
        return false;
    };
    std::process::Command::new(cmd)
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .is_ok()
}

/// How often an unseal waiting on a passkey asks again.
const POLL: Duration = Duration::from_secs(2);

/// Unseals each path once: at once, or once the person approves with a passkey on the page this
/// shows, waiting for them.
async fn unseal(
    ui: &mut Ui,
    shared: &std::sync::Arc<ok_store::Shared>,
    paths: Vec<String>,
    no_browser: bool,
) -> Result<(), CommandError> {
    let first = ok_store::shared::run({
        let (shared, paths) = (shared.clone(), paths.clone());
        async move { shared.unseal(&paths, "").await }
    })
    .await
    .map_err(secret_error)?;
    match first.state() {
        UnsealState::Granted => return Ok(()),
        UnsealState::Approve => {}
        s => return Err(user_error(format!("ok secrets answered the unseal {s:?}"))),
    }
    let mut shown = format!("To unseal, approve with your passkey at\n{}\n", first.url);
    if !no_browser && open_browser(&first.url) {
        shown.push_str("(your browser is open there)\n");
    }
    write!(ui.stderr(), "{shown}")?;
    let expires = first.expires_at.map(|t| t.seconds).unwrap_or_default();
    let approval = first.approval_id.clone();
    let (shared, started) = (shared.clone(), Instant::now());
    ok_store::shared::run(async move {
        loop {
            tokio::time::sleep(POLL).await;
            let r = shared.unseal(&paths, &approval).await?;
            match r.state() {
                UnsealState::Granted => return Ok(()),
                UnsealState::Waiting => {}
                s => {
                    return Err(Status::internal(format!(
                        "ok secrets answered the unseal {s:?}"
                    )));
                }
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64);
            if expires > 0 && now > expires || started.elapsed() > Duration::from_secs(600) {
                return Err(Status::deadline_exceeded(
                    "the approval expired before it was given: run the command again",
                ));
            }
        }
    })
    .await
    .map_err(secret_error)
}

pub async fn run(
    ui: &mut Ui,
    command: &CommandHelper,
    cmd: SecretCommand,
) -> Result<(), CommandError> {
    match cmd {
        SecretCommand::Unseal(args) => {
            let ws = command.workspace_helper(ui).await?;
            let mut paths = Vec::new();
            for a in &args.paths {
                paths.push(secret_path(&ws, a)?.as_internal_file_string().to_owned());
            }
            let shared = shared(&ws)?;
            unseal(ui, &shared, paths.clone(), args.no_browser).await?;
            for p in paths {
                writeln!(ui.status(), "Unsealed //{p} for one read, for ten minutes")?;
            }
            Ok(())
        }
        SecretCommand::Status(_) => {
            let ws = command.workspace_helper(ui).await?;
            let shared = shared(&ws)?;
            let unseals = ok_store::shared::run(async move { shared.unseals().await })
                .await
                .map_err(secret_error)?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64);
            let mut out = ui.stdout();
            if unseals.is_empty() {
                writeln!(out, "No unseals")?;
            }
            for u in unseals {
                let left = u.expires_at.map_or(0, |t| (t.seconds - now).max(0));
                writeln!(
                    out,
                    "//{}  expires in {}m{:02}s",
                    u.path,
                    left / 60,
                    left % 60
                )?;
            }
            Ok(())
        }
        SecretCommand::Cat(args) => {
            let ws = command.workspace_helper(ui).await?;
            let path = secret_path(&ws, &args.path)?;
            let Some(wc) = ws.get_wc_commit_id() else {
                return Err(user_error("this workspace has no working-copy commit"));
            };
            let store = ws.repo().store().clone();
            let commit = store.get_commit_async(wc).await?;
            let value = commit.tree().path_value(&path).await?;
            let id = match value.into_resolved() {
                Ok(Some(TreeValue::File { id, .. })) => id,
                Ok(_) => {
                    return Err(user_error(format!(
                        "@ has no secret at {}",
                        path.as_internal_file_string()
                    )));
                }
                Err(_) => {
                    return Err(user_error(format!(
                        "{} is conflicted in @: resolve it to one pointer first",
                        path.as_internal_file_string()
                    )));
                }
            };
            let mut held = Vec::new();
            store
                .read_file(&path, &id)
                .await?
                .take(ok_secret::POINTER_LEN as u64 + 1)
                .read_to_end(&mut held)
                .await
                .map_err(|e| user_error_with_message("reading the pointer", e))?;
            let digest = ok_secret::parse_pointer(&held).ok_or_else(|| {
                user_error(format!(
                    "{} in @ holds no pointer",
                    path.as_internal_file_string()
                ))
            })?;
            let shared = shared(&ws)?;
            let p = path.as_internal_file_string().to_owned();
            unseal(ui, &shared, vec![p.clone()], args.no_browser).await?;
            let value: SecretBuf =
                ok_store::shared::run(async move { shared.read_secret(&p, &digest).await })
                    .await
                    .map_err(secret_error)?;
            let mut out = ui.stdout();
            out.write_all(value.as_slice())?;
            out.flush()?;
            Ok(())
        }
        SecretCommand::Write(args) => {
            let ws = command.workspace_helper(ui).await?;
            let path = secret_path(&ws, &args.path)?;
            let mut value = Vec::new();
            let limit = ok_secret::MAX_VALUE as u64 + 1;
            match &args.file {
                Some(f) => std::fs::File::open(f)
                    .and_then(|f| f.take(limit).read_to_end(&mut value))
                    .map_err(|e| user_error_with_message(format!("reading {}", f.display()), e))?,
                None => std::io::stdin()
                    .take(limit)
                    .read_to_end(&mut value)
                    .map_err(|e| user_error_with_message("reading the value", e))?,
            };
            let value = SecretBuf::new(value);
            if value.len() > ok_secret::MAX_VALUE {
                return Err(user_error(format!(
                    "a secret's value is at most {} bytes",
                    ok_secret::MAX_VALUE
                )));
            }
            let shared = shared(&ws)?;
            let p = path.as_internal_file_string().to_owned();
            let digest = ok_store::shared::run({
                let p = p.clone();
                async move { shared.write_secret(&p, value.as_slice()).await }
            })
            .await
            .map_err(secret_error)?;
            let file = ws.workspace_root().join(&p);
            if let Some(dir) = file.parent() {
                std::fs::create_dir_all(dir)
                    .map_err(|e| user_error_with_message(format!("making {}", dir.display()), e))?;
            }
            std::fs::write(&file, ok_secret::pointer(&digest))
                .map_err(|e| user_error_with_message(format!("writing {}", file.display()), e))?;
            writeln!(
                ui.status(),
                "Kept a new version of //{p}: its pointer is in the working copy, to upload as a CL"
            )?;
            Ok(())
        }
    }
}
