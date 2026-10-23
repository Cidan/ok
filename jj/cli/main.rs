//! ok's jj (docs/scm.md, "The client"): upstream jj with ok's stores and commands registered, and a
//! built-in configuration layer.

mod auth;
mod changelists;
mod commands;
mod fs;
mod lifecycle;
mod secrets;

// The allocator upstream jj uses, on the platforms it uses it on (jj-cli's main.rs).
#[cfg(any(
    all(target_os = "windows", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::process::ExitCode;

use jj_cli::cli_util::BoxedAsyncCliDispatch;
use jj_cli::cli_util::CliRunner;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::user_error;
use jj_cli::ui::Ui;
use jj_lib::config::ConfigLayer;
use jj_lib::config::ConfigSource;

/// ok's defaults, under anything the user configures: trunk() is main as the SCM has it, the ok new
/// clones come from — the one this machine signed in to — snapshots of new files up to 50 MiB, the
/// largest the SCM holds, and the author identity of the person who signed in. A clone's CLs link
/// to the ok it came from unless `ok.web-url` says otherwise.
///
/// trunk() is `main@ok` whenever that exists. Like jj's own definition it falls back to root()
/// rather than failing, because immutable_heads() is built on it and must evaluate in every
/// repository state — including a clone's, before `main@ok` is set.
fn defaults() -> ConfigLayer {
    let mut text = "[revset-aliases]\n\"trunk()\" = 'latest(remote_bookmarks(exact:\"main\", exact:\"ok\") | root())'\n\n[snapshot]\nmax-new-file-size = \"50MiB\"\n".to_owned();
    if let Some(c) = ok_credential::dir()
        .ok()
        .and_then(|d| ok_credential::load(&d).ok())
    {
        text.push_str(&format!("\n[ok]\nendpoint = {:?}\n", c.server.url));
        let person = c.person.clone().unwrap_or_default();
        let name = Some(person.display_name.as_str())
            .filter(|n| !n.is_empty())
            .or(c.handle())
            .unwrap_or(&c.principal)
            .to_owned();
        text.push_str(&format!("\n[user]\nname = {name:?}\n"));
        if !person.email.is_empty() {
            text.push_str(&format!("email = {:?}\n", person.email));
        }
    }
    ConfigLayer::parse(ConfigSource::Default, &text).expect("ok's default configuration parses")
}

/// ok's working copies: the real one (jj's local working copy, honoring `.ignore`) and the
/// virtual one (okfs, docs/okfs.md).
fn working_copy_factories() -> jj_lib::workspace::WorkingCopyFactories {
    let mut f = ok_working_copy::factories();
    f.insert(
        okfs::workingcopy::NAME.to_owned(),
        Box::new(okfs::workingcopy::OkfsWorkingCopyFactory),
    );
    f
}

/// Refuses what would make a workspace reach past itself (docs/scm.md, "Decisions", 3):
/// `jj workspace add`, whose workspace would share this one's repository and operation log, and
/// `jj workspace rename`, after which this workspace's views would name a workspace its operation
/// log is not. `jj clone` makes another workspace.
async fn isolated_workspaces(
    ui: &mut Ui,
    command: &CommandHelper,
    dispatch: BoxedAsyncCliDispatch<'_>,
) -> Result<(), CommandError> {
    if let Some(("workspace", sub)) = command.matches().subcommand() {
        match sub.subcommand_name() {
            Some("add") => {
                return Err(user_error(
                    "an ok workspace is one jj workspace: `jj workspace add` would share this one's operation log",
                )
                .hinted("`jj clone <repository> <directory>` makes another workspace, isolated from this one"));
            }
            Some("rename") => {
                return Err(user_error(
                    "an ok workspace keeps the name ok knows it by: its operation log is that workspace's",
                )
                .hinted("`jj clone <repository> <directory> --workspace <name>` makes a workspace with another name"));
            }
            _ => {}
        }
    }
    dispatch.call(ui, command).await
}

fn main() -> ExitCode {
    let code = CliRunner::init()
        .version(env!("CARGO_PKG_VERSION"))
        .add_store_factories(ok_store::factories())
        .add_working_copy_factories(working_copy_factories())
        .set_workspace_loader_factory(Box::new(ok_working_copy::OkWorkspaceLoaderFactory))
        .add_extra_config(defaults())
        .add_subcommand(commands::run)
        .add_dispatch_hook(isolated_workspaces)
        .run()
        .into();
    // What the command asked ok, when OK_STATS names a file (jj/store/calls.rs).
    if let Err(e) = ok_store::calls::write_report() {
        eprintln!("Warning: writing OK_STATS: {e}");
    }
    code
}
