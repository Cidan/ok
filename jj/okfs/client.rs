//! jj's side of the daemon: finding it, starting it, and replacing one of another build
//! (docs/okfs.md, "The daemon"). The daemon runs under a service manager, never as a copy of jj
//! that jj starts itself: where the user has a systemd user manager, as the `okfs.service` unit,
//! which keeps its mounts' connections across restarts; on macOS, as the launchd agent `ok.okfs`,
//! which restarts it at once, its NFS mounts waiting for it. Elsewhere jj only connects, and says
//! how to start one (`jj fs daemon`, under whatever supervises the machine's processes) when none
//! runs. `OKFS_DAEMON=external` (tests) only connects, to a daemon of any build.

use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use ok_proto::ok::okfs::v1 as pb;

use crate::control::Client;
use crate::control::Paths;
use crate::control::private_dir;
use crate::daemon::build_id;

/// The systemd user unit's name.
pub const UNIT: &str = "okfs.service";

/// The launchd agent's label, on macOS.
pub const LAUNCHD_LABEL: &str = "ok.okfs";

/// Whether this process is the launchd agent.
pub fn under_launchd() -> bool {
    std::env::var("XPC_SERVICE_NAME").is_ok_and(|n| n == LAUNCHD_LABEL)
}

/// Whether the daemon runs as a launchd agent here: on macOS.
fn launchd() -> bool {
    cfg!(target_os = "macos")
}

fn mode() -> String {
    std::env::var("OKFS_DAEMON").unwrap_or_default()
}

/// Whether the user has a systemd user manager to run the daemon under.
fn systemd_user() -> bool {
    std::env::var_os("XDG_RUNTIME_DIR")
        .is_some_and(|d| Path::new(&d).join("systemd/private").exists())
}

/// A connection to the daemon, starting it (or replacing one of another build) when needed.
pub fn connect(paths: &Paths) -> io::Result<Client> {
    if let Ok(mut c) = Client::connect(paths) {
        match c.call(pb::request::Request::Hello(pb::Hello {}))? {
            pb::response::Response::Hello(h) if h.version == build_id() || mode() == "external" => {
                return Ok(c);
            }
            pb::response::Response::Hello(h) => {
                drop(c);
                restart(paths, h.pid)?;
            }
            _ => {
                return Err(io::Error::other(
                    "the okfs daemon answered Hello with something else",
                ));
            }
        }
    } else {
        if mode() == "external" {
            return Err(io::Error::other(format!(
                "no okfs daemon at {} (OKFS_DAEMON=external)",
                paths.socket().display()
            )));
        }
        start(paths)?;
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut c) = Client::connect(paths)
            && c.call(pb::request::Request::Hello(pb::Hello {})).is_ok()
        {
            return Ok(c);
        }
        if Instant::now() > deadline {
            return Err(io::Error::other(format!(
                "the okfs daemon did not start (see {})",
                if systemd_user() {
                    "journalctl --user -u okfs".to_owned()
                } else if launchd() {
                    format!(
                        "{} and launchctl print {}/{LAUNCHD_LABEL}",
                        paths.state.join("daemon.log").display(),
                        launchd_domain()
                    )
                } else {
                    paths.state.join("daemon.log").display().to_string()
                }
            )));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn run(cmd: &mut Command) -> io::Result<()> {
    let out = cmd.stdin(Stdio::null()).output()?;
    if !out.status.success() {
        return Err(io::Error::other(format!(
            "{:?}: {}",
            cmd,
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(())
}

/// A value for a unit's `Environment=`, quoted as systemd reads it.
fn unit_env(name: &str, value: &std::ffi::OsStr) -> String {
    let v = value
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    format!("Environment=\"{name}={v}\"\n")
}

/// The unit file, running this binary with the installing jj's `PATH` (and `OK_CONFIG_DIR`), so the
/// daemon reads, and renews, the credential the CLI does.
fn unit_text(exe: &Path) -> String {
    let mut env = String::new();
    for name in ["PATH", "OK_CONFIG_DIR"] {
        if let Some(v) = std::env::var_os(name).filter(|v| !v.is_empty()) {
            env.push_str(&unit_env(name, &v));
        }
    }
    format!(
        "# Written by ok's jj (docs/okfs.md): virtual workspaces' daemon.\n\
         [Unit]\n\
         Description=okfs: virtual workspaces for ok's jj\n\
         \n\
         [Service]\n\
         Type=notify\n\
         NotifyAccess=main\n\
         {}\
         ExecStart={} fs daemon\n\
         Restart=always\n\
         RestartSec=1\n\
         FileDescriptorStoreMax=4096\n\
         FileDescriptorStorePreserve=on-success\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        env,
        exe.display()
    )
}

fn unit_path() -> io::Result<PathBuf> {
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .ok_or_else(|| io::Error::other("no HOME"))?;
    Ok(config.join("systemd/user").join(UNIT))
}

/// Installs (or updates) the unit for this binary; true when it changed.
fn install_unit() -> io::Result<bool> {
    let exe = std::env::current_exe()?;
    let path = unit_path()?;
    let text = unit_text(&exe);
    if std::fs::read_to_string(&path).is_ok_and(|t| t == text) {
        return Ok(false);
    }
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(&path, text)?;
    Ok(true)
}

fn start(paths: &Paths) -> io::Result<()> {
    if systemd_user() {
        if install_unit()? {
            run(Command::new("systemctl").args(["--user", "daemon-reload"]))?;
        }
        return run(Command::new("systemctl").args(["--user", "enable", "--now", UNIT]));
    }
    if launchd() {
        return launch(paths, false);
    }
    Err(io::Error::other(format!(
        "no okfs daemon runs at {}, and this machine has no service manager for jj to run one under \
         (no systemd user manager, and this is not macOS): start it yourself — `jj fs daemon`, under \
         whatever supervises this machine's processes (docs/okfs.md, \"The daemon\")",
        paths.socket().display()
    )))
}

/// The launchd domain the agent runs in: the user's, which is there whether or not anyone is
/// logged in at the screen (over ssh, too). The agent is a background session's only, so a login
/// at the screen does not load a second one.
fn launchd_domain() -> String {
    format!("user/{}", unsafe { libc::geteuid() })
}

fn plist_path() -> io::Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| io::Error::other("no HOME"))?;
    Ok(PathBuf::from(home)
        .join("Library/LaunchAgents")
        .join(format!("{LAUNCHD_LABEL}.plist")))
}

fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// The agent's property list: this binary, with the installing jj's `PATH` (and `OK_CONFIG_DIR`),
/// kept alive, as an interactive process — the user's programs wait on its answers — logging to
/// the state directory.
fn plist_text(exe: &Path, log: &Path, port: u16) -> String {
    let mut env = String::new();
    for name in ["PATH", "OK_CONFIG_DIR", "OKFS_STATE_DIR", "OKFS_NFS_TRACE"] {
        if let Some(v) = std::env::var_os(name).filter(|v| !v.is_empty()) {
            env.push_str(&format!(
                "\t\t<key>{name}</key>\n\t\t<string>{}</string>\n",
                xml(&v.to_string_lossy())
            ));
        }
    }
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <!-- Written by ok's jj (docs/okfs.md): virtual workspaces' daemon. -->\n\
         <plist version=\"1.0\">\n<dict>\n\
         \t<key>Label</key>\n\t<string>{LAUNCHD_LABEL}</string>\n\
         \t<key>ProgramArguments</key>\n\t<array>\n\
         \t\t<string>{}</string>\n\t\t<string>fs</string>\n\t\t<string>daemon</string>\n\
         \t</array>\n\
         \t<key>EnvironmentVariables</key>\n\t<dict>\n{env}\t</dict>\n\
         \t<key>LimitLoadToSessionType</key>\n\t<string>Background</string>\n\
         \t<key>RunAtLoad</key>\n\t<true/>\n\
         \t<key>KeepAlive</key>\n\t<true/>\n\
         \t<key>ThrottleInterval</key>\n\t<integer>1</integer>\n\
         \t<key>ProcessType</key>\n\t<string>Interactive</string>\n\
         \t<key>Sockets</key>\n\t<dict>\n\t\t<key>{socket}</key>\n\t\t<dict>\n\
         \t\t\t<key>SockNodeName</key>\n\t\t\t<string>127.0.0.1</string>\n\
         \t\t\t<key>SockServiceName</key>\n\t\t\t<string>{port}</string>\n\
         \t\t\t<key>SockFamily</key>\n\t\t\t<string>IPv4</string>\n\
         \t\t\t<key>SockType</key>\n\t\t\t<string>stream</string>\n\
         \t\t</dict>\n\t</dict>\n\
         \t<key>StandardOutPath</key>\n\t<string>{log}</string>\n\
         \t<key>StandardErrorPath</key>\n\t<string>{log}</string>\n\
         </dict>\n</plist>\n",
        xml(&exe.to_string_lossy()),
        log = xml(&log.to_string_lossy()),
        socket = NFS_SOCKET,
    )
}

/// The launchd socket's name, as the daemon asks for it.
#[cfg(target_os = "macos")]
const NFS_SOCKET: &str = crate::nfsd::LAUNCHD_SOCKET;
#[cfg(not(target_os = "macos"))]
const NFS_SOCKET: &str = "NFS";

/// The port the agent's socket listens on.
fn agent_port(paths: &Paths, loaded: bool) -> io::Result<u16> {
    #[cfg(target_os = "macos")]
    {
        crate::nfsd::agent_port(&paths.state, loaded)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (paths, loaded);
        Err(io::Error::other("launchd is macOS's"))
    }
}

/// Starts the agent — or, `replace`ing, restarts it — installing or updating its property list.
fn launch(paths: &Paths, replace: bool) -> io::Result<()> {
    private_dir(&paths.state)?;
    let path = plist_path()?;
    let domain = launchd_domain();
    let service = format!("{domain}/{LAUNCHD_LABEL}");
    let loaded = || {
        Command::new("launchctl")
            .args(["print", &service])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    };
    let port = agent_port(paths, loaded())?;
    let text = plist_text(
        &std::env::current_exe()?,
        &paths.state.join("daemon.log"),
        port,
    );
    let changed = std::fs::read_to_string(&path).map_or(true, |t| t != text);
    // Loading while launchd still unloads the agent fails: it is waited out, and tried again.
    let bootstrap = || -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match run(Command::new("launchctl")
                .args(["bootstrap", &domain])
                .arg(&path))
            {
                Ok(()) => return Ok(()),
                Err(_) if loaded() => return Ok(()),
                Err(e) if Instant::now() > deadline => return Err(e),
                Err(_) => std::thread::sleep(Duration::from_millis(250)),
            }
        }
    };
    if changed {
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(&path, &text)?;
        if loaded() {
            // Unloading stops the daemon; its mounts wait for the one loaded next.
            let _ = run(Command::new("launchctl").args(["bootout", &service]));
            let deadline = Instant::now() + Duration::from_secs(10);
            while loaded() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        return bootstrap();
    }
    if !loaded() {
        return bootstrap();
    }
    let mut args = vec!["kickstart"];
    if replace {
        args.push("-k");
    }
    args.push(&service);
    run(Command::new("launchctl").args(args))
}

/// Replaces a daemon of another build with this one.
fn restart(paths: &Paths, pid: u32) -> io::Result<()> {
    if mode() == "external" {
        return Ok(());
    }
    if systemd_user() {
        if install_unit()? {
            run(Command::new("systemctl").args(["--user", "daemon-reload"]))?;
        }
        return run(Command::new("systemctl").args(["--user", "restart", UNIT]));
    }
    if launchd() {
        return launch(paths, true);
    }
    Err(io::Error::other(format!(
        "the okfs daemon at {} (pid {pid}) is another build of jj, and only what started it can \
         replace it: stop it, and start `jj fs daemon` again (docs/okfs.md, \"The daemon\")",
        paths.socket().display()
    )))
}

/// Whether lingering is on for this user (mounts outlive the login session).
pub fn lingering() -> bool {
    let user = std::env::var("USER").unwrap_or_default();
    Path::new("/var/lib/systemd/linger").join(user).exists()
}

/// Whether the daemon runs under systemd here.
pub fn under_systemd() -> bool {
    systemd_user()
}
