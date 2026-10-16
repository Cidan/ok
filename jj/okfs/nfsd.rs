//! The daemon's NFS service, on macOS (docs/okfs.md, "macOS"): one okfs-nfs server for every
//! workspace, on a port of 127.0.0.1 and under a handle key both kept in the state directory, so
//! that across a daemon restart the kernel's mounts reach the new daemon and the handles they hold
//! still work. Under launchd the port's socket is launchd's, held open across restarts: the
//! kernel's reconnection, made at once, finds it listening — a refused one backs off for seconds. A workspace is mounted with mount_nfs, through a one-time path; a mount a
//! predecessor made is adopted; unmounting forces, as FUSE's detaching does. The mount table is
//! read with getfsstat, which asks no server: a stat of a mount whose server is gone would wait
//! for it.

use std::ffi::CStr;
use std::io;
use std::io::Read as _;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use okfs_nfs as nfs;
use serde::Deserialize;
use serde::Serialize;

use crate::model::ROOT;
use crate::nfs::NfsMount;

/// Threads answering the kernel's calls: as many as it sends at once from a few readers.
const WORKERS: usize = 32;

/// mount_nfs's options: NFSv3 over TCP to the server's port, locks kept by the kernel itself (no
/// lock daemon), interruptible waits, hidden from the Finder's sidebar, and large transfers.
const OPTIONS: &str = "vers=3,tcp,locallocks,intr,nobrowse,noquota,rsize=1048576,wsize=1048576,dsize=65536,readahead=32";

/// What the server keeps across restarts: its port and handle key.
#[derive(Serialize, Deserialize)]
struct Kept {
    port: u16,
    key: String,
}

/// The name of the launchd agent's socket (client.rs's property list).
pub const LAUNCHD_SOCKET: &str = "NFS";

/// The server's port and key, made the first time: the port one free now.
fn kept(state: &Path) -> io::Result<(u16, [u8; 32])> {
    let path = state.join("nfs.toml");
    let kept: Option<Kept> = std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| toml::from_str(&t).ok());
    if let Some(k) = &kept
        && let Ok(key) = hex::decode(&k.key)
        && let Ok(key) = <[u8; 32]>::try_from(key.as_slice())
    {
        return Ok((k.port, key));
    }
    let port = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?
        .local_addr()?
        .port();
    let key = random()?;
    keep(state, port, &key)?;
    Ok((port, key))
}

fn keep(state: &Path, port: u16, key: &[u8; 32]) -> io::Result<()> {
    std::fs::create_dir_all(state)?;
    write_private(
        &state.join("nfs.toml"),
        &toml::to_string(&Kept {
            port,
            key: hex::encode(key),
        })
        .map_err(io::Error::other)?,
    )
}

/// The port the launchd agent's socket listens on (client.rs): the kept one — or, taken by
/// something else with no agent holding it, a new one, kept from now on.
pub fn agent_port(state: &Path, agent_loaded: bool) -> io::Result<u16> {
    let (port, key) = kept(state)?;
    if agent_loaded || TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).is_ok() {
        return Ok(port);
    }
    let port = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?
        .local_addr()?
        .port();
    keep(state, port, &key)?;
    Ok(port)
}

unsafe extern "C" {
    fn launch_activate_socket(
        name: *const libc::c_char,
        fds: *mut *mut libc::c_int,
        cnt: *mut libc::size_t,
    ) -> libc::c_int;
}

/// The listening socket launchd holds for the agent, if this process is the agent.
fn launchd_socket() -> Option<TcpListener> {
    use std::os::fd::FromRawFd as _;
    let name = std::ffi::CString::new(LAUNCHD_SOCKET).unwrap();
    let mut fds: *mut libc::c_int = std::ptr::null_mut();
    let mut n: libc::size_t = 0;
    if unsafe { launch_activate_socket(name.as_ptr(), &mut fds, &mut n) } != 0 || fds.is_null() {
        return None;
    }
    let all: Vec<libc::c_int> = unsafe { std::slice::from_raw_parts(fds, n) }.to_vec();
    unsafe { libc::free(fds.cast()) };
    let mut all = all.into_iter();
    let first = all.next()?;
    for extra in all {
        unsafe { libc::close(extra) };
    }
    let listener = unsafe { TcpListener::from_raw_fd(first) };
    // As launchd made it, perhaps non-blocking: the server's accepts wait.
    listener.set_nonblocking(false).ok()?;
    Some(listener)
}

/// What a workspace's mount is, as its state directory keeps it: the export its handles name, and
/// what the mount table says it was mounted from, at which port.
#[derive(Serialize, Deserialize, Default)]
struct Mounted {
    epoch: u64,
    from: String,
    port: u16,
}

pub struct Nfsd {
    server: Arc<nfs::Server>,
    port: u16,
}

fn random<const N: usize>() -> io::Result<[u8; N]> {
    let mut b = [0u8; N];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
    Ok(b)
}

fn write_private(path: &Path, text: &str) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        std::io::Write::write_all(&mut f, text.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(tmp, path)
}

/// The export a workspace's handles name: its ID's and epoch's hash. A new epoch serves the
/// workspace under a new export, which no handle given out before names.
fn export_id(workspace: &str, epoch: u64) -> u64 {
    let h = blake3::hash(format!("{workspace}\0{epoch}").as_bytes());
    u64::from_be_bytes(h.as_bytes()[..8].try_into().unwrap())
}

/// A path as the mount table names it: its parent's real path, and its name. The path itself is
/// not resolved — on a mount whose server is gone, that would wait.
fn canonical(path: &Path) -> PathBuf {
    match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) => std::fs::canonicalize(parent)
            .map(|p| p.join(name))
            .unwrap_or_else(|_| path.to_owned()),
        _ => path.to_owned(),
    }
}

/// The mount at `path`, as the mount table has it: its type and what it was mounted from.
fn mount_at(path: &Path) -> Option<(String, String)> {
    let path = canonical(path);
    let n = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
    if n <= 0 {
        return None;
    }
    let mut buf: Vec<libc::statfs> = vec![unsafe { std::mem::zeroed() }; n as usize + 8];
    let size = (buf.len() * std::mem::size_of::<libc::statfs>()) as libc::c_int;
    let n = unsafe { libc::getfsstat(buf.as_mut_ptr(), size, libc::MNT_NOWAIT) };
    if n <= 0 {
        return None;
    }
    let want = path.as_os_str().as_encoded_bytes();
    buf.truncate(n as usize);
    buf.iter().find_map(|st| {
        let on = unsafe { CStr::from_ptr(st.f_mntonname.as_ptr()) };
        (on.to_bytes() == want).then(|| {
            let ty = unsafe { CStr::from_ptr(st.f_fstypename.as_ptr()) };
            let from = unsafe { CStr::from_ptr(st.f_mntfromname.as_ptr()) };
            (
                ty.to_string_lossy().into_owned(),
                from.to_string_lossy().into_owned(),
            )
        })
    })
}

fn run(cmd: &mut Command) -> Result<(), String> {
    let out = cmd
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| format!("{cmd:?}: {e}"))?;
    if out.status.success() {
        return Ok(());
    }
    Err(format!(
        "{}",
        String::from_utf8_lossy(&out.stderr)
            .trim()
            .trim_end_matches('.')
    ))
}

/// Unmounts `path`, forcing it: what uses it gets errors from then on, as with FUSE's detaching.
pub fn unmount(path: &Path) -> Result<(), String> {
    if mount_at(path).is_none() {
        return Ok(());
    }
    run(Command::new("/sbin/umount").arg("-f").arg(path))
        .map_err(|e| format!("unmounting {}: {e}", path.display()))
}

/// Why this machine cannot mount a workspace over NFS, if it cannot.
pub fn can_mount() -> Result<(), String> {
    if Path::new("/sbin/mount_nfs").exists() {
        Ok(())
    } else {
        Err("/sbin/mount_nfs is missing".into())
    }
}

impl Nfsd {
    /// Starts the server under its kept key, on launchd's socket — or, outside launchd, on its
    /// kept port (taken by something else, a new one: the old port's mounts are made anew).
    pub fn start(state: &Path) -> io::Result<Self> {
        let (port, key) = kept(state)?;
        let listener = match launchd_socket() {
            Some(l) => l,
            None => {
                TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).or_else(|e| {
                    eprintln!("okfs: NFS port {port} is unavailable ({e}): moving");
                    TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
                })?
            }
        };
        let port = listener.local_addr()?.port();
        keep(state, port, &key)?;
        let server = nfs::Server::new(nfs::Options {
            key,
            owner: unsafe { libc::geteuid() },
            workers: WORKERS,
            trace: std::env::var_os("OKFS_NFS_TRACE").is_some(),
            // A predecessor's mounts wait for their workspaces, served as the daemon remounts them.
            starting: true,
        });
        {
            let server = server.clone();
            std::thread::Builder::new()
                .name("okfs-nfs".into())
                .spawn(move || server.serve(listener))?;
        }
        Ok(Self { server, port })
    }

    fn mounted(state: &Path) -> Mounted {
        std::fs::read_to_string(state.join("nfs-mount.toml"))
            .ok()
            .and_then(|t| toml::from_str(&t).ok())
            .unwrap_or_default()
    }

    /// Whether a predecessor's mount of the workspace is still in place, from this server's port:
    /// its handles are good here, and its inodes are the kernel's still.
    pub fn adoptable(&self, mount_point: &Path, state: &Path) -> bool {
        let m = Self::mounted(state);
        m.port == self.port
            && !m.from.is_empty()
            && mount_at(mount_point).is_some_and(|(ty, from)| ty == "nfs" && from == m.from)
    }

    /// Serves a workspace and mounts it at `mount_point` — or, `adopt`ing, serves the mount already
    /// there. A one-time path another caller used first fails the mount: the workspace is then
    /// served under a new export, whose handles no one else holds, and mounted again.
    pub fn serve(
        &self,
        workspace: &str,
        fs: Arc<NfsMount>,
        mount_point: &Path,
        state: &Path,
        adopt: bool,
    ) -> Result<(), String> {
        let mut m = Self::mounted(state);
        if adopt {
            self.server
                .export(export_id(workspace, m.epoch), ROOT, fs.clone());
            return Ok(());
        }
        // Whatever is mounted there is no mount of this server's: a dead predecessor's, or one at
        // a port no longer served.
        unmount(mount_point)?;
        for attempt in 0..2 {
            if attempt > 0 {
                m.epoch += 1;
            }
            let id = export_id(workspace, m.epoch);
            self.server.export(id, ROOT, fs.clone());
            let token = hex::encode(random::<16>().map_err(|e| e.to_string())?);
            let path = format!("/okfs-{token}");
            self.server.arm(path.as_bytes(), id);
            let from = format!("localhost:{path}");
            let res = run(Command::new("/sbin/mount_nfs")
                .arg("-o")
                .arg(format!("{OPTIONS},port={0},mountport={0}", self.port))
                .arg(&from)
                .arg(mount_point));
            let stolen = !self.server.armed(path.as_bytes());
            self.server.disarm(path.as_bytes());
            match res {
                Ok(()) => {
                    m.from = from;
                    m.port = self.port;
                    write_private(
                        &state.join("nfs-mount.toml"),
                        &toml::to_string(&m).map_err(|e| e.to_string())?,
                    )
                    .map_err(|e| e.to_string())?;
                    return Ok(());
                }
                Err(e) => {
                    self.server.unexport(id);
                    if !stolen || attempt > 0 {
                        return Err(format!("mounting {}: {e}", mount_point.display()));
                    }
                    eprintln!(
                        "okfs: the mount path of {} was used by another caller: serving it anew",
                        mount_point.display()
                    );
                }
            }
        }
        unreachable!()
    }

    /// Stops serving a workspace and unmounts it.
    pub fn unserve(&self, workspace: &str, mount_point: &Path, state: &Path) -> Result<(), String> {
        let m = Self::mounted(state);
        let res = unmount(mount_point);
        self.server.unexport(export_id(workspace, m.epoch));
        let _ = std::fs::remove_file(state.join("nfs-mount.toml"));
        res
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Every registered workspace is served: a call on another export is stale from now on.
    pub fn ready(&self) {
        self.server.ready();
    }
}
