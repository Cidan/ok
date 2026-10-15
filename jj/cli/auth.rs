//! `jj auth`: signing this machine in to ok (docs/identity.md, "Signing in"), as `ok auth` does —
//! the same credential, in the same place, so either signs the other in.

use std::io::Write as _;

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::user_error;
use jj_cli::command_error::user_error_with_message;
use jj_cli::ui::Ui;

/// Sign this machine in to ok, or out
#[derive(clap::Subcommand, Clone, Debug)]
pub enum AuthCommand {
    Login(LoginArgs),
    Status(StatusArgs),
    Logout(LogoutArgs),
}

/// Sign this machine in to ok
///
/// Prints a code and a link: open it, sign in, and approve the code. The machine then holds a
/// credential — a key it keeps and a certificate ok renews before it expires — which every jj
/// command presents to ok.
#[derive(clap::Args, Clone, Debug)]
pub struct LoginArgs {
    /// ok's address, e.g. `https://ok.example.com` (default: `ok.endpoint`, or `$OK_SERVER`)
    #[arg(value_name = "URL")]
    server: Option<String>,
    /// What this machine is called on your account page (default: user@host)
    #[arg(long)]
    label: Option<String>,
    /// Do not open a browser
    #[arg(long)]
    no_browser: bool,
}

/// Show who this machine is signed in as
#[derive(clap::Args, Clone, Debug)]
pub struct StatusArgs {}

/// Sign this machine out, revoking its credential
#[derive(clap::Args, Clone, Debug)]
pub struct LogoutArgs {}

fn credential_error(e: ok_credential::Error) -> CommandError {
    match e {
        ok_credential::Error::NotLoggedIn => user_error("this machine is not signed in to ok")
            .hinted("Run `jj auth login <ok's address>`"),
        e => user_error_with_message("the credential", e),
    }
}

fn default_label() -> String {
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty());
    match (std::env::var("USER").ok().filter(|u| !u.is_empty()), host) {
        (Some(u), Some(h)) => format!("{u}@{h}"),
        (None, Some(h)) => h,
        _ => "a command line".to_owned(),
    }
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

pub async fn run(
    ui: &mut Ui,
    command: &CommandHelper,
    cmd: AuthCommand,
) -> Result<(), CommandError> {
    let dir = ok_credential::dir().map_err(credential_error)?;
    match cmd {
        AuthCommand::Login(args) => {
            let url = args
                .server
                .or_else(|| command.settings().get_string("ok.endpoint").ok())
                .or_else(|| std::env::var("OK_SERVER").ok().filter(|s| !s.is_empty()))
                .ok_or_else(|| {
                    user_error("name ok's address: `jj auth login https://ok.example.com`")
                })?;
            let server = ok_credential::Server::parse(&url).map_err(credential_error)?;
            let label = args.label.unwrap_or_else(default_label);
            // The login speaks gRPC and waits on timers, so it runs on the store's Tokio runtime:
            // jj's own executor is no runtime. The code to approve comes back here, to be shown
            // while the login waits for it.
            let (code_tx, code_rx) =
                futures::channel::oneshot::channel::<ok_credential::Approval>();
            let login = ok_store::shared::run({
                let dir = dir.clone();
                async move {
                    ok_credential::login(&server, &dir, &label, move |a| {
                        let _ = code_tx.send(a.clone());
                    })
                    .await
                }
            });
            let show = async {
                if let Ok(a) = code_rx.await {
                    let mut shown = format!(
                        "To sign in, open {}\nand confirm the code {}\n",
                        a.uri_complete, a.user_code
                    );
                    if !args.no_browser && open_browser(&a.uri_complete) {
                        shown.push_str("(your browser is open there)\n");
                    }
                    let _ = write!(ui.stderr(), "{shown}");
                }
            };
            let (c, ()) = futures::future::join(login, show).await;
            let c = c.map_err(credential_error)?;
            writeln!(
                ui.status(),
                "Signed in to {} as {}",
                c.server.url,
                c.principal
            )?;
        }
        AuthCommand::Status(_) => {
            let c = ok_credential::load(&dir).map_err(credential_error)?;
            let left = c
                .not_after
                .duration_since(std::time::SystemTime::now())
                .map(|d| d.as_secs() / 3600)
                .unwrap_or(0);
            writeln!(
                ui.stdout(),
                "Signed in to {} as {}; the certificate renews before it expires, in {left} hours",
                c.server.url,
                c.principal
            )?;
        }
        AuthCommand::Logout(_) => {
            let dir = dir.clone();
            ok_store::shared::run(async move { ok_credential::logout(&dir).await })
                .await
                .map_err(credential_error)?;
            writeln!(ui.status(), "Signed out")?;
        }
    }
    Ok(())
}
