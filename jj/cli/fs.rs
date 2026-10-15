//! `jj fs`: virtual workspaces' commands (docs/okfs.md, "The user's view") — the daemon,
//! its status, prefetching, checkpoints and restoring them, re-attaching, and forgetting a
//! virtual workspace.

use std::io::Write as _;

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::user_error;
use jj_cli::command_error::user_error_with_message;
use jj_cli::ui::Ui;
use jj_lib::backend::TreeId;
use jj_lib::conflict_labels::ConflictLabels;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo::Repo as _;
use ok_proto::ok::okfs::v1 as fspb;
use ok_store::OkBackend;
use ok_store::pb;
use okfs::workingcopy::OkfsWorkingCopy;

#[derive(clap::Subcommand, Clone, Debug)]
pub enum FsCommand {
    /// The virtual workspaces' daemon, and each workspace's mount, lease, and caches
    Status,
    /// Read files into the cache ahead of time
    Prefetch(PrefetchArgs),
    /// Print what changes in this workspace as it changes, until interrupted: `changed <path>`
    /// for each path written, created, moved, or removed, and `checkout` when a checkout moves
    /// the workspace to another tree (which file-system notifications do not report)
    Watch,
    /// This workspace's checkpoints, newest first
    Checkpoints(CheckpointsArgs),
    /// Make the working copy a checkpoint's tree (undo with `jj undo`)
    Restore(RestoreArgs),
    /// Take this workspace back from the machine holding it, at its latest checkpoint
    Attach,
    /// Unmount this virtual workspace and remove its local state, a stale one too (its latest
    /// state is kept as a checkpoint first; `jj clone --attach` brings the workspace back)
    Forget,
    /// Run the daemon (systemd's okfs.service runs this)
    #[command(hide = true)]
    Daemon,
}

#[derive(clap::Args, Clone, Debug)]
pub struct PrefetchArgs {
    /// Files and directories to read (default: everything)
    #[arg(value_hint = clap::ValueHint::AnyPath)]
    paths: Vec<String>,
}

#[derive(clap::Args, Clone, Debug)]
pub struct CheckpointsArgs {
    /// How many to show
    #[arg(long, short = 'n', default_value_t = 20)]
    limit: i32,
}

#[derive(clap::Args, Clone, Debug)]
pub struct RestoreArgs {
    /// The checkpoint's ID
    id: i64,
}

fn daemon_error(e: std::io::Error) -> CommandError {
    user_error_with_message("the okfs daemon", e)
}

fn call(req: fspb::request::Request) -> Result<fspb::response::Response, CommandError> {
    let paths = okfs::control::Paths::from_env();
    okfs::client::connect(&paths)
        .map_err(daemon_error)?
        .call(req)
        .map_err(daemon_error)
}

fn bytes(n: u64) -> String {
    match n {
        n if n >= 1 << 30 => format!("{:.1} GiB", n as f64 / (1u64 << 30) as f64),
        n if n >= 1 << 20 => format!("{:.1} MiB", n as f64 / (1u64 << 20) as f64),
        n if n >= 1 << 10 => format!("{:.1} KiB", n as f64 / (1u64 << 10) as f64),
        n => format!("{n} B"),
    }
}

pub async fn run(ui: &mut Ui, command: &CommandHelper, cmd: FsCommand) -> Result<(), CommandError> {
    match cmd {
        FsCommand::Daemon => {
            okfs::daemon::run(okfs::control::Paths::from_env()).map_err(daemon_error)
        }
        FsCommand::Status => {
            let fspb::response::Response::Status(s) =
                call(fspb::request::Request::Status(fspb::Status {}))?
            else {
                return Err(user_error(
                    "the okfs daemon answered Status with something else",
                ));
            };
            let mut out = ui.stdout();
            writeln!(
                out,
                "okfs daemon (pid {}){}",
                s.pid,
                if s.online {
                    ""
                } else {
                    ": offline, ok is not reachable"
                }
            )?;
            for w in s.workspaces {
                writeln!(out, "{} at {}", w.workspace_name, w.mount_point)?;
                if !w.mounted {
                    writeln!(out, "  not mounted")?;
                    continue;
                }
                writeln!(out, "  lease: {}", w.lease)?;
                writeln!(
                    out,
                    "  latest checkpoint: {}",
                    if w.latest_checkpoint > 0 {
                        w.latest_checkpoint.to_string()
                    } else {
                        "none".into()
                    }
                )?;
                writeln!(
                    out,
                    "  written: {} files, {}",
                    w.materialized_files,
                    bytes(w.overlay_bytes)
                )?;
                writeln!(
                    out,
                    "  read from ok: {} files whole, {} in all",
                    w.fetched_files,
                    bytes(w.fetched_bytes)
                )?;
                writeln!(
                    out,
                    "  directory prefetches: {} ({} files)",
                    w.prefetches, w.prefetched_files
                )?;
                write_waits(&mut out, w.waits.as_ref())?;
            }
            for p in s.presentations {
                writeln!(
                    out,
                    "repository {} at {}, presenting commit {}",
                    p.repository_id,
                    p.mount_point,
                    hex::encode(&p.commit_id)
                )?;
                writeln!(
                    out,
                    "  read from ok: {} in all; directory prefetches: {} ({} files)",
                    bytes(p.fetched_bytes),
                    p.prefetches,
                    p.prefetched_files
                )?;
                write_waits(&mut out, p.waits.as_ref())?;
            }
            if !s.calls.is_empty() {
                writeln!(out, "calls to ok since the daemon started:")?;
                let mut calls = s.calls;
                calls.sort_by(|a, b| b.calls.cmp(&a.calls).then_with(|| a.method.cmp(&b.method)));
                for c in calls {
                    writeln!(
                        out,
                        "  {}: {} ({} failed), {:.2} s in all, longest {:.2} s; {} sent, {} received",
                        c.method,
                        c.calls,
                        c.failures,
                        c.seconds,
                        c.max_seconds,
                        bytes(c.sent_bytes),
                        bytes(c.received_bytes)
                    )?;
                }
            }
            if !s.fetches.is_empty() {
                writeln!(out, "batched fetches since the daemon started:")?;
                for f in s.fetches {
                    writeln!(
                        out,
                        "  {}: {} batches of {} in all, {:.2} s",
                        f.kind, f.batches, f.keys, f.seconds
                    )?;
                }
            }
            Ok(())
        }
        FsCommand::Prefetch(args) => {
            let ws = command.workspace_helper(ui).await?;
            let id = virtual_id(&ws)?;
            let mut paths = Vec::new();
            for p in &args.paths {
                paths.push(ws.parse_file_path(p)?.as_internal_file_string().to_owned());
            }
            if paths.is_empty() {
                paths.push(String::new());
            }
            call(fspb::request::Request::Prefetch(fspb::Prefetch {
                workspace_id: id,
                paths,
            }))?;
            Ok(())
        }
        FsCommand::Watch => {
            let ws = command.workspace_helper_no_snapshot(ui).await?;
            let id = virtual_id(&ws)?;
            let paths = okfs::control::Paths::from_env();
            let mut client = okfs::client::connect(&paths).map_err(daemon_error)?;
            okfs::control::write_frame(
                client.stream(),
                &fspb::Request {
                    request: Some(fspb::request::Request::Subscribe(fspb::Subscribe {
                        workspace_id: id,
                    })),
                },
            )
            .map_err(daemon_error)?;
            loop {
                let Some(resp) = client.next_response().map_err(daemon_error)? else {
                    return Ok(());
                };
                if !resp.error.is_empty() {
                    return Err(user_error(resp.error));
                }
                let Some(fspb::response::Response::Event(e)) = resp.response else {
                    continue;
                };
                let mut out = ui.stdout();
                if e.checkout {
                    writeln!(out, "checkout")?;
                }
                for p in e.paths {
                    writeln!(out, "changed {p}")?;
                }
                out.flush()?;
            }
        }
        FsCommand::Checkpoints(args) => {
            let ws = command.workspace_helper(ui).await?;
            let name = ws.working_copy().workspace_name().to_owned();
            let shared = shared(&ws)?;
            let client = shared.client.clone();
            let req = pb::ListCheckpointsRequest {
                repository_id: client.repository_id(),
                workspace: name.as_str().to_owned(),
                before_id: 0,
                limit: args.limit,
            };
            let list = ok_store::shared::run(async move {
                client
                    .workspaces()
                    .await?
                    .list_checkpoints(req)
                    .await
                    .map(|r| r.into_inner())
            })
            .await
            .map_err(|s| user_error_with_message("listing checkpoints on ok", s))?;
            let mut out = ui.stdout();
            for c in list.checkpoints {
                let when = c
                    .created_at
                    .map(|t| {
                        chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32)
                            .map(|d| {
                                d.with_timezone(&chrono::Local)
                                    .format("%Y-%m-%d %H:%M:%S")
                                    .to_string()
                            })
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();
                writeln!(
                    out,
                    "{:>8}  {when}  {}{}",
                    c.id,
                    c.machine,
                    if c.fork {
                        "  (fork: written on a machine that had lost the workspace)"
                    } else {
                        ""
                    }
                )?;
            }
            Ok(())
        }
        FsCommand::Restore(args) => {
            let mut ws = command.workspace_helper(ui).await?;
            let shared = shared(&ws)?;
            let client = shared.client.clone();
            let req = pb::GetCheckpointRequest {
                repository_id: client.repository_id(),
                id: args.id,
            };
            let cp = ok_store::shared::run(async move {
                client
                    .workspaces()
                    .await?
                    .get_checkpoint(req)
                    .await
                    .map(|r| r.into_inner())
            })
            .await
            .map_err(|s| {
                user_error_with_message(format!("reading checkpoint {} on ok", args.id), s)
            })?;
            let store = ws.repo().store().clone();
            let ids: Vec<TreeId> = cp
                .tree_ids
                .iter()
                .map(|b| {
                    if b.is_empty() {
                        store.empty_tree_id().clone()
                    } else {
                        TreeId::new(b.clone())
                    }
                })
                .collect();
            let tree = MergedTree::new(store, Merge::from_vec(ids), ConflictLabels::unlabeled());
            let name = ws.working_copy().workspace_name().to_owned();
            let wc_id = ws
                .repo()
                .view()
                .get_wc_commit_id(&name)
                .cloned()
                .ok_or_else(|| user_error("this workspace has no working-copy commit"))?;
            let mut tx = ws.start_transaction();
            let wc = tx.repo().store().get_commit_async(&wc_id).await?;
            tx.repo_mut()
                .rewrite_commit(&wc)
                .set_tree(tree)
                .write()
                .await?;
            tx.finish(ui, format!("restore checkpoint {}", args.id))
                .await?;
            Ok(())
        }
        FsCommand::Attach => {
            let ws = command.workspace_helper_no_snapshot(ui).await?;
            let wc = virtual_wc(&ws)?;
            let id = wc.workspace_id().to_owned();
            let root = wc.root().to_owned();
            let name = ws.working_copy().workspace_name().as_str().to_owned();
            let repo_dir = ws.repo_path().to_owned();
            let tree = ws.working_copy().tree()?.clone();
            let fspb::response::Response::Mount(m) =
                call(fspb::request::Request::Mount(fspb::Mount {
                    workspace_id: id,
                    mount_point: root.display().to_string(),
                    repo_dir: repo_dir.display().to_string(),
                    workspace_name: name.clone(),
                    tree: Some(fspb::Tree {
                        ids: tree.tree_ids().iter().map(|t| t.to_bytes()).collect(),
                    }),
                    sparse: Vec::new(),
                    attach: true,
                }))?
            else {
                return Err(user_error(
                    "the okfs daemon answered Mount with something else",
                ));
            };
            writeln!(
                ui.status(),
                "Attached workspace {name}{}",
                if m.checkpoint_id > 0 {
                    format!(" at its checkpoint {}", m.checkpoint_id)
                } else {
                    String::new()
                }
            )?;
            Ok(())
        }
        FsCommand::Forget => {
            // The workspace's operation log is its own (docs/scm.md, "Decisions", 3), so
            // forgetting it here needs no operation: the daemon checkpoints what the mount holds
            // and removes it with the workspace's repository, and the workspace stays on ok. No
            // snapshot either, so a stale working copy is forgotten as readily as any.
            let ws = command.workspace_helper_no_snapshot(ui).await?;
            let id = virtual_id(&ws)?;
            let name = ws.working_copy().workspace_name().to_owned();
            call(fspb::request::Request::Forget(fspb::Forget {
                workspace_id: id,
            }))?;
            writeln!(
                ui.status(),
                "Forgot workspace {}: unmounted and its local state removed; it stays on ok at its last checkpoint (`jj clone <repository> <directory> --attach {}` brings it back)",
                name.as_symbol(),
                name.as_symbol()
            )?;
            Ok(())
        }
    }
}

/// A workspace's or presentation's reads that waited on ok.
fn write_waits(out: &mut impl std::io::Write, w: Option<&fspb::WaitStats>) -> std::io::Result<()> {
    let w = w.cloned().unwrap_or_default();
    writeln!(
        out,
        "  reads that waited on ok: {}, {:.2} s in all, longest {:.2} s",
        w.reads, w.seconds, w.max_seconds
    )
}

fn virtual_wc(
    ws: &jj_cli::cli_util::WorkspaceCommandHelper,
) -> Result<&OkfsWorkingCopy, CommandError> {
    ws.working_copy()
        .downcast_ref::<OkfsWorkingCopy>()
        .ok_or_else(|| {
            user_error("this is not a virtual workspace")
                .hinted("`jj clone` without --real makes one")
        })
}

fn virtual_id(ws: &jj_cli::cli_util::WorkspaceCommandHelper) -> Result<String, CommandError> {
    Ok(virtual_wc(ws)?.workspace_id().to_owned())
}

fn shared(
    ws: &jj_cli::cli_util::WorkspaceCommandHelper,
) -> Result<std::sync::Arc<ok_store::Shared>, CommandError> {
    Ok(ws
        .repo()
        .store()
        .backend_impl::<OkBackend>()
        .ok_or_else(|| user_error("this is not a repository on ok"))?
        .shared()
        .clone())
}
