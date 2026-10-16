//! ok's commands (docs/scm.md, "Commands"): `jj clone` and `jj create` here, and
//! the ChangeList commands `jj cl` and `jj sync` in `changelists`.

use std::fs;
use std::io::Write as _;
use std::path::Path;

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::cli_error;
use jj_cli::command_error::user_error;
use jj_cli::command_error::user_error_with_message;
use jj_cli::ui::Ui;
use jj_lib::file_util;
use jj_lib::file_util::IoResultExt as _;
use jj_lib::op_store::RefTarget;
use jj_lib::op_store::RemoteRef;
use jj_lib::op_store::RemoteRefState;
use jj_lib::ref_name::RefName;
use jj_lib::ref_name::RemoteName;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::Repo as _;
use jj_lib::repo::RepoInitError;
use jj_lib::signing::Signer;
use jj_lib::working_copy::WorkingCopyFactory as _;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::WorkspaceInitError;
use jj_lib::workspace_store::SimpleWorkspaceStore;
use jj_lib::workspace_store::WorkspaceStore as _;
use ok_proto::ok::okfs::v1 as okfs_pb;
use ok_store::RepoConfig;
use ok_store::Repository;
use tonic::Code;
use tonic::Status;

/// The remote `main` is tracked under: `main@ok` is `trunk()`.
const REMOTE: &str = "ok";

#[derive(clap::Subcommand, Clone, Debug)]
pub enum Command {
    #[command(subcommand)]
    Auth(crate::auth::AuthCommand),
    Cl(crate::changelists::ClArgs),
    Clone(CloneArgs),
    Create(CreateArgs),
    #[command(subcommand)]
    Fs(crate::fs::FsCommand),
    Sync(crate::changelists::SyncArgs),
}

/// Clone a repository from ok
///
/// The new workspace's working-copy commit is a new change on `main@ok`. The workspace is
/// yours alone and isolated from your others: its operation log, on ok, is its own, and work
/// moves between workspaces only as CLs (upload, submit, `jj sync`).
///
/// The workspace is virtual (docs/okfs.md): mounted at once, its files read from ok the
/// first time something reads them, every write kept in ok within a second. `--real` makes a
/// real checkout instead, every file written to disk. Virtual workspaces are Linux's: elsewhere
/// every clone is real.
#[derive(clap::Args, Clone, Debug)]
pub struct CloneArgs {
    /// The repository's name
    #[arg(value_name = "REPOSITORY")]
    name: String,
    /// Where to create the workspace (default: the repository's name)
    #[arg(value_hint = clap::ValueHint::DirPath)]
    destination: Option<String>,
    /// The workspace's name (default: `<host>-<directory name>`)
    #[arg(long, conflicts_with = "attach")]
    workspace: Option<WorkspaceNameBuf>,
    /// A real checkout: every file written to disk
    #[arg(long, conflicts_with = "attach")]
    real: bool,
    /// Attach an existing workspace here, at its latest checkpoint, taking it over from the
    /// machine that holds it
    #[arg(long, value_name = "WORKSPACE")]
    attach: Option<WorkspaceNameBuf>,
}

/// Create a repository on ok and clone it
///
/// The repository is created on ok first, with you as its creator, and then cloned
/// exactly as `jj clone` would: nothing is ever local-only. A name that already exists is
/// refused; use `jj clone` for that.
#[derive(clap::Args, Clone, Debug)]
pub struct CreateArgs {
    /// The new repository's name: lowercase letters, digits, `.`, `_`, and `-`
    name: String,
    /// Where to create the workspace (default: the repository's name)
    #[arg(value_hint = clap::ValueHint::DirPath)]
    destination: Option<String>,
    /// The workspace's name (default: `<host>-<directory name>`)
    #[arg(long)]
    workspace: Option<WorkspaceNameBuf>,
    /// A real checkout: every file written to disk
    #[arg(long)]
    real: bool,
}

/// How a clone lays out its workspace.
#[derive(Clone, Debug)]
pub enum Kind {
    Real,
    Virtual,
    /// A virtual workspace attaching an existing one.
    Attach(WorkspaceNameBuf),
}

pub async fn run(ui: &mut Ui, command: &CommandHelper, cmd: Command) -> Result<(), CommandError> {
    if command.global_args().at_operation.is_some() {
        return Err(cli_error("--at-op is not respected"));
    }
    match cmd {
        Command::Auth(cmd) => crate::auth::run(ui, command, cmd).await,
        Command::Cl(args) => crate::changelists::run_cl(ui, command, args).await,
        Command::Sync(args) => crate::changelists::run_sync(ui, command, args).await,
        Command::Fs(cmd) => crate::fs::run(ui, command, cmd).await,
        Command::Clone(args) => {
            let endpoint = scm_endpoint(command)?;
            let real = args.real || !VIRTUAL_WORKSPACES;
            if !real || args.attach.is_some() {
                can_mount()?;
            }
            let dest =
                Destination::prepare(command, args.destination.as_deref().unwrap_or(&args.name))?;
            let repo = ok_store::get_repository(&endpoint, &args.name)
                .await
                .map_err(|s| scm_error(&endpoint, &args.name, s))?;
            let kind = match (real, args.attach) {
                (_, Some(w)) => Kind::Attach(w),
                (true, None) => Kind::Real,
                (false, None) => Kind::Virtual,
            };
            dest.clone_into(ui, command, &endpoint, &repo, args.workspace, kind)
                .await
        }
        Command::Create(args) => {
            let endpoint = scm_endpoint(command)?;
            let real = args.real || !VIRTUAL_WORKSPACES;
            if !real {
                can_mount()?;
            }
            let dest =
                Destination::prepare(command, args.destination.as_deref().unwrap_or(&args.name))?;
            let repo = ok_store::create_repository(&endpoint, &args.name)
                .await
                .map_err(|s| scm_error(&endpoint, &args.name, s))?;
            writeln!(ui.status(), "Created repository {} on ok", repo.name)?;
            let kind = if real { Kind::Real } else { Kind::Virtual };
            dest.clone_into(ui, command, &endpoint, &repo, args.workspace, kind)
                .await
                .map_err(|e| {
                    e.hinted(format!(
                        "The repository exists on ok; `jj clone {}` clones it",
                        repo.name
                    ))
                })
        }
    }
}

/// Whether this platform has virtual workspaces: okfs serves Linux's FUSE and macOS's NFS.
/// Elsewhere a clone without `--real` is real.
const VIRTUAL_WORKSPACES: bool = cfg!(any(target_os = "linux", target_os = "macos"));

/// Refuses a virtual workspace where this machine cannot mount one, before anything is asked of
/// ok.
fn can_mount() -> Result<(), CommandError> {
    okfs::can_mount().map_err(|why| {
        user_error(format!(
            "this machine cannot mount a virtual workspace: {why}"
        ))
        .hinted("`--real` makes a real checkout, every file on disk")
    })
}

/// A failed repository call, as the user should see it.
/// The ok a new clone talks to: `ok.endpoint`, which signing in sets.
fn scm_endpoint(command: &CommandHelper) -> Result<String, CommandError> {
    command.settings().get_string("ok.endpoint").map_err(|_| {
        user_error("this machine is not signed in to ok, and ok.endpoint is not set")
            .hinted("Run `jj auth login <ok's address>`")
    })
}

/// Where a clone's CLs are shown: `ok.web-url` if it is set, else the ok it was cloned from.
pub fn web_url(command: &CommandHelper, endpoint: &str) -> String {
    command
        .settings()
        .get_string("ok.web-url")
        .unwrap_or_else(|_| endpoint.to_owned())
}

fn scm_error(endpoint: &str, name: &str, s: Status) -> CommandError {
    match s.code() {
        Code::NotFound => user_error(format!("ok has no repository named {name:?}"))
            .hinted(format!("`jj create {name}` creates it")),
        Code::AlreadyExists => {
            user_error(format!("a repository named {name:?} already exists on ok"))
                .hinted(format!("`jj clone {name}` clones it"))
        }
        Code::InvalidArgument => user_error(s.message().to_owned()),
        Code::Unauthenticated => user_error_with_message(
            format!("ok at {endpoint} did not accept this machine's credential"),
            s,
        )
        .hinted("Run `jj auth login`"),
        _ => user_error_with_message(format!("ok at {endpoint}"), s),
    }
}

/// The directory a clone goes into, checked before anything is asked of the SCM so a
/// repository is never created for a clone that cannot happen.
struct Destination {
    path: std::path::PathBuf,
    existed: bool,
}

impl Destination {
    fn prepare(command: &CommandHelper, dest: &str) -> Result<Self, CommandError> {
        let path = command.cwd().join(dest);
        let existed = path.exists();
        if existed && !file_util::is_empty_dir(&path)? {
            return Err(user_error(format!(
                "{dest} exists and is not an empty directory"
            )));
        }
        Ok(Self { path, existed })
    }

    /// Clones `repo` here, removing what it laid out if it fails.
    async fn clone_into(
        self,
        ui: &mut Ui,
        command: &CommandHelper,
        endpoint: &str,
        repo: &Repository,
        workspace: Option<WorkspaceNameBuf>,
        kind: Kind,
    ) -> Result<(), CommandError> {
        fs::create_dir_all(&self.path).context(&self.path)?;
        let path = dunce::canonicalize(&self.path).context(&self.path)?;
        let result = match kind {
            Kind::Real => clone(ui, command, &path, endpoint, repo, workspace).await,
            kind => clone_virtual(ui, command, &path, endpoint, repo, workspace, kind).await,
        };
        if result.is_err() {
            let cleaned = match fs::remove_dir_all(path.join(".jj")) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
                _ if !self.existed => fs::remove_dir(&path),
                _ => Ok(()),
            };
            if let Err(e) = cleaned {
                writeln!(
                    ui.warning_default(),
                    "Failed to clean up {}: {e}",
                    path.display()
                )
                .ok();
            }
        }
        result
    }
}

/// Makes the clone's workspace on ok, with an operation log of its own that no other workspace
/// ever reads or changes (docs/scm.md, "Decisions", 3): named `workspace`, or by default
/// `<host>-<directory name>`, numbered past the names the person has.
async fn create_workspace(
    settings: &jj_lib::settings::UserSettings,
    endpoint: &str,
    remote: &Repository,
    wc_path: &Path,
    workspace: Option<WorkspaceNameBuf>,
) -> Result<WorkspaceNameBuf, CommandError> {
    let create = async |name: &WorkspaceNameBuf| {
        ok_store::create_workspace(endpoint, remote.id, name.as_str())
            .await
            .map_err(|s| scm_error(endpoint, &remote.name, s))
    };
    if let Some(name) = workspace {
        if name.as_str().is_empty() {
            return Err(user_error("the workspace name cannot be empty"));
        }
        if !create(&name).await? {
            return Err(user_error(format!(
                "you have a workspace named {} of {} already",
                name.as_symbol(),
                remote.name
            ))
            .hinted("Name another one with --workspace, or attach it with --attach"));
        }
        return Ok(name);
    }
    let host = settings
        .operation_hostname()
        .split('.')
        .next()
        .unwrap_or_default();
    let dir = wc_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| user_error("the destination's name is not UTF-8"))?;
    let base = if host.is_empty() {
        dir.to_owned()
    } else {
        format!("{host}-{dir}")
    };
    if base.is_empty() {
        return Err(user_error("the workspace name cannot be empty"));
    }
    for n in 1.. {
        let name = WorkspaceNameBuf::from(if n == 1 {
            base.clone()
        } else {
            format!("{base}-{n}")
        });
        if create(&name).await? {
            return Ok(name);
        }
    }
    unreachable!("a free workspace name")
}

/// Refuses to attach a workspace the person does not have, or one from before each workspace had
/// an operation log of its own.
async fn attachable(
    endpoint: &str,
    remote: &Repository,
    name: &WorkspaceNameBuf,
) -> Result<(), CommandError> {
    let workspaces = ok_store::list_workspaces(endpoint, remote.id)
        .await
        .map_err(|s| scm_error(endpoint, &remote.name, s))?;
    match workspaces.iter().find(|w| w.workspace == name.as_str()) {
        None => Err(user_error(format!(
            "you have no workspace named {} of {}",
            name.as_symbol(),
            remote.name
        ))),
        Some(w) if !w.own_log => Err(user_error(format!(
            "workspace {} of {} was made before each workspace had its own operation log, and cannot be attached",
            name.as_symbol(),
            remote.name
        ))
        .hinted(format!("`jj clone {}` makes a new workspace", remote.name))),
        Some(_) => Ok(()),
    }
}

/// Lays out a jj repository in `repo_dir` with ok's stores, as workspace `name` — the
/// workspace's own, its operation log the workspace's — and loads it at the operation heads,
/// everything jj reads to load it and build its index fetched in a few calls.
async fn workspace_repo(
    settings: &jj_lib::settings::UserSettings,
    endpoint: &str,
    remote: &Repository,
    repo_dir: &Path,
    name: &WorkspaceNameBuf,
) -> Result<std::sync::Arc<jj_lib::repo::ReadonlyRepo>, CommandError> {
    fs::create_dir_all(repo_dir).context(repo_dir)?;
    let config = RepoConfig {
        endpoint: endpoint.to_owned(),
        repository_id: remote.id,
        repository: remote.name.clone(),
        dev_principal: ok_store::client::dev_principal(endpoint),
        cache: None,
        workspace: Some(name.as_str().to_owned()),
    };
    let signer = Signer::from_settings(settings).map_err(WorkspaceInitError::from)?;
    let repo = ok_store::init_repo(settings, repo_dir, config, signer)
        .await
        .map_err(|e| match e {
            RepoInitError::Backend(e) => WorkspaceInitError::Backend(e),
            RepoInitError::OpHeadsStore(e) => WorkspaceInitError::OpHeadsStore(e),
            RepoInitError::Path(e) => WorkspaceInitError::Path(e),
        })?;
    ok_store::prefetch::operation_log(&repo_dir.join("store"))
        .await
        .map_err(|s| scm_error(endpoint, &remote.name, s))?;
    Ok(repo.loader().load_at_head().await?)
}

/// A virtual clone (docs/okfs.md): a new workspace on ok, its own repository in its
/// directory of the daemon's state, the workspace mounted by the daemon, and — as a real clone
/// does — `main@ok` set and a new change checked out on it. Attaching mounts an existing
/// workspace at its latest checkpoint instead, taking it over from the machine that holds it.
async fn clone_virtual(
    ui: &mut Ui,
    command: &CommandHelper,
    wc_path: &Path,
    endpoint: &str,
    remote: &Repository,
    workspace: Option<WorkspaceNameBuf>,
    kind: Kind,
) -> Result<(), CommandError> {
    let (settings, _) = command.settings_for_new_workspace(ui, wc_path)?;
    let name = match &kind {
        Kind::Attach(name) => {
            attachable(endpoint, remote, name).await?;
            name.clone()
        }
        _ => create_workspace(&settings, endpoint, remote, wc_path, workspace).await?,
    };
    let (id, state) = okfs::workingcopy::new_workspace()?;
    let result = mount_virtual(
        ui, command, &settings, wc_path, endpoint, remote, &name, &kind, &id, &state,
    )
    .await;
    if result.is_err() {
        // The daemon unmounts it and drops its state, the repository with it; or, never
        // mounted, the state goes here.
        let paths = okfs::control::Paths::from_env();
        let forgot = okfs::client::connect(&paths).and_then(|mut c| {
            c.call(okfs_pb::request::Request::Forget(okfs_pb::Forget {
                workspace_id: id.clone(),
            }))
        });
        if forgot.is_err()
            && state.exists()
            && let Err(e) = fs::remove_dir_all(&state)
        {
            writeln!(
                ui.warning_default(),
                "Failed to remove {}: {e}",
                state.display()
            )
            .ok();
        }
    }
    result
}

/// Makes virtual workspace `id`'s repository in its state directory and mounts it at `wc_path`:
/// new, with a new change on `main@ok`, or attached at the workspace's latest checkpoint.
#[allow(clippy::too_many_arguments)]
async fn mount_virtual(
    ui: &mut Ui,
    command: &CommandHelper,
    settings: &jj_lib::settings::UserSettings,
    wc_path: &Path,
    endpoint: &str,
    remote: &Repository,
    name: &WorkspaceNameBuf,
    kind: &Kind,
    id: &str,
    state: &Path,
) -> Result<(), CommandError> {
    let repo_dir = state.join("repo");
    let repo = workspace_repo(settings, endpoint, remote, &repo_dir, name).await?;
    if let Kind::Attach(_) = kind {
        let Some(wc_commit) = repo.view().get_wc_commit_id(name).cloned() else {
            return Err(user_error(format!(
                "workspace {} of {} has no working copy",
                name.as_symbol(),
                remote.name
            )));
        };
        let commit = repo.store().get_commit_async(&wc_commit).await?;
        let (working_copy, checkpoint) = okfs::workingcopy::attach_virtual(
            id,
            repo.store().clone(),
            wc_path,
            &repo_dir,
            name,
            repo.op_id().clone(),
            &commit,
            settings,
        )?;
        let workspace = Workspace::new(
            wc_path,
            repo_dir.clone(),
            working_copy,
            repo.loader().clone(),
        )?;
        SimpleWorkspaceStore::load(&repo_dir)?
            .add(workspace.workspace_name(), workspace.workspace_root())?;
        writeln!(
            ui.status(),
            "Attached workspace {} of {} at \"{}\"{}",
            name.as_symbol(),
            remote.name,
            file_util::relative_path(command.cwd(), wc_path).display(),
            if checkpoint > 0 {
                format!(", at its checkpoint {checkpoint}")
            } else {
                String::new()
            }
        )?;
        return Ok(());
    }
    // The workspace exists in the operation its working copy starts at, as jj's own
    // initialization makes it: a clone interrupted after this is a stale working copy that `jj
    // workspace update-stale` brings up to date.
    let mut tx = repo.start_transaction();
    tx.repo_mut()
        .check_out(name.clone(), &repo.store().root_commit())
        .await?;
    let repo = tx
        .commit(format!("add workspace '{}'", name.as_symbol()))
        .await?;
    let working_copy = okfs::workingcopy::init_virtual(
        id,
        repo.store().clone(),
        wc_path,
        &repo_dir,
        name,
        repo.op_id().clone(),
        settings,
    )?;
    check_out_new(
        ui,
        command,
        wc_path,
        &repo_dir,
        remote,
        name,
        working_copy,
        repo,
    )
    .await
}

/// A new virtual workspace's first operation: `main@ok` set and a new change checked out on it,
/// as a real clone does.
#[allow(clippy::too_many_arguments)]
async fn check_out_new(
    ui: &mut Ui,
    command: &CommandHelper,
    wc_path: &Path,
    repo_dir: &Path,
    remote: &Repository,
    name: &WorkspaceNameBuf,
    working_copy: Box<dyn jj_lib::working_copy::WorkingCopy>,
    repo: std::sync::Arc<jj_lib::repo::ReadonlyRepo>,
) -> Result<(), CommandError> {
    let workspace = Workspace::new(
        wc_path,
        repo_dir.to_owned(),
        working_copy,
        repo.loader().clone(),
    )?;
    SimpleWorkspaceStore::load(repo_dir)?
        .add(workspace.workspace_name(), workspace.workspace_root())?;
    let mut helper = command.for_workable_repo(ui, workspace, repo)?;
    let mut tx = helper.start_transaction();
    let head = tx.repo().store().get_commit_async(&remote.head).await?;
    if head.id() != tx.repo().store().root_commit_id() {
        tx.repo_mut().add_head(&head).await?;
    }
    tx.repo_mut().set_remote_bookmark(
        RefName::new("main").to_remote_symbol(RemoteName::new(REMOTE)),
        RemoteRef {
            target: RefTarget::normal(remote.head.clone()),
            state: RemoteRefState::New,
        },
    );
    let wc_commit = tx
        .repo_mut()
        .new_commit(vec![remote.head.clone()], head.tree())
        .write()
        .await?;
    tx.edit(&wc_commit)?;
    tx.finish(
        ui,
        format!("clone {} into workspace {}", remote.name, name.as_symbol()),
    )
    .await?;
    writeln!(
        ui.status(),
        "Cloned {} into \"{}\" as workspace {}",
        remote.name,
        file_util::relative_path(command.cwd(), wc_path).display(),
        name.as_symbol()
    )?;
    writeln!(
        ui.status(),
        "The workspace is virtual: files are read from ok as they are used (`--real` writes them all)"
    )?;
    if okfs::client::under_systemd() && !okfs::client::lingering() {
        writeln!(
            ui.hint_default(),
            "Virtual workspaces stay mounted while you are logged in; `loginctl enable-linger` keeps them mounted always"
        )?;
    }
    Ok(())
}

/// A real clone: a new workspace on ok, its own repository in `.jj/repo`, and — in one
/// operation — `main@ok` set to the SCM's head and a new change checked out on it.
async fn clone(
    ui: &mut Ui,
    command: &CommandHelper,
    wc_path: &Path,
    endpoint: &str,
    remote: &Repository,
    workspace: Option<WorkspaceNameBuf>,
) -> Result<(), CommandError> {
    let (settings, _) = command.settings_for_new_workspace(ui, wc_path)?;
    let jj_dir = wc_path.join(".jj");
    fs::create_dir(&jj_dir).context(&jj_dir)?;
    let repo_dir = jj_dir.join("repo");
    let name = create_workspace(&settings, endpoint, remote, wc_path, workspace).await?;
    let repo = workspace_repo(&settings, endpoint, remote, &repo_dir, &name).await?;
    let store_dir = repo_dir.join("store");

    let wc_state = jj_dir.join("working_copy");
    fs::create_dir(&wc_state).context(&wc_state)?;
    let working_copy = ok_working_copy::OkWorkingCopyFactory.init_working_copy(
        repo.store().clone(),
        wc_path.to_owned(),
        wc_state.clone(),
        repo.op_id().clone(),
        name.clone(),
        &settings,
    )?;
    let type_path = wc_state.join("type");
    fs::write(&type_path, working_copy.name()).context(&type_path)?;
    let repo_dir = dunce::canonicalize(&repo_dir).context(&repo_dir)?;
    let workspace = Workspace::new(
        wc_path,
        repo_dir.clone(),
        working_copy,
        repo.loader().clone(),
    )?;
    SimpleWorkspaceStore::load(&repo_dir)?
        .add(workspace.workspace_name(), workspace.workspace_root())?;

    let mut helper = command.for_workable_repo(ui, workspace, repo)?;
    let mut tx = helper.start_transaction();
    let head = tx.repo().store().get_commit_async(&remote.head).await?;
    if head.id() != tx.repo().store().root_commit_id() {
        tx.repo_mut().add_head(&head).await?;
    }
    tx.repo_mut().set_remote_bookmark(
        RefName::new("main").to_remote_symbol(RemoteName::new(REMOTE)),
        RemoteRef {
            target: RefTarget::normal(remote.head.clone()),
            state: RemoteRefState::New,
        },
    );
    let wc_commit = tx
        .repo_mut()
        .new_commit(vec![remote.head.clone()], head.tree())
        .write()
        .await?;
    tx.edit(&wc_commit)?;
    for tree in head.tree_ids().iter() {
        ok_store::prefetch::tree(&store_dir, tree)
            .await
            .map_err(|s| scm_error(endpoint, &remote.name, s))?;
    }
    tx.finish(
        ui,
        format!("clone {} into workspace {}", remote.name, name.as_symbol()),
    )
    .await?;
    writeln!(
        ui.status(),
        "Cloned {} into \"{}\" as workspace {}",
        remote.name,
        file_util::relative_path(command.cwd(), wc_path).display(),
        name.as_symbol()
    )?;
    Ok(())
}
