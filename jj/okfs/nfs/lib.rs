//! okfs-nfs: an NFSv3 server (RFC 1813) and its MOUNT protocol, on one TCP port, for a file
//! system served from user space — what okfs serves on macOS, whose kernel has no FUSE and whose
//! own replacement, FSKit, admits only file systems Apple's paid developer program has signed. The
//! crate knows nothing of jj or ok: it is the transport, as okfs-fuse is on Linux.
//!
//! A server serves several file systems (exports) on one port: the kernel's NFS client mounts each
//! with its own one-time path (`Server::arm`), and from the root handle `MNT` returns it reaches
//! everything else. Handles carry a keyed hash ([`handle`]), so a local process that was never
//! given one cannot make one, and calls from anyone but the owner's user ID are refused: the user
//! ID in a call on a good handle is the kernel's, as only the kernel holds handles. `MNT` itself
//! checks no user (mount_nfs sends it as root): a path used by another caller first fails the
//! owner's mount, which then serves the file system as a new export (`Server::export`).

pub mod client;
pub mod handle;
pub mod nfs3;
pub mod rpc;
#[cfg(test)]
mod tests;
pub mod xdr;

use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

use crate::handle::Handles;
use crate::rpc::Accept;
use crate::rpc::AuthStat;
use crate::rpc::Cred;
use crate::rpc::Unanswerable;

/// An operation's outcome: the value, or an errno.
pub type Result<T> = std::result::Result<T, i32>;

pub const NFS_PROGRAM: u32 = 100003;
pub const MOUNT_PROGRAM: u32 = 100005;

/// The most a READ returns and a WRITE carries.
pub const MAX_IO: u32 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    File,
    Dir,
    Symlink,
    Block,
    Char,
    Socket,
    Fifo,
}

/// A time as NFS carries it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Time {
    pub secs: i64,
    pub nanos: u32,
}

/// A file's attributes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Attr {
    pub kind: Kind,
    /// Permission bits (and setuid, setgid, sticky).
    pub perm: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    /// Bytes the file takes on disk.
    pub used: u64,
    pub rdev: (u32, u32),
    /// The file's number, which its handle names.
    pub fileid: u64,
    pub atime: Time,
    pub mtime: Time,
    pub ctime: Time,
}

/// A time a SETATTR sets: the server's clock, or the client's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetTime {
    Now,
    At(Time),
}

/// What a SETATTR (or a create's initial attributes) sets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SetAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<SetTime>,
    pub mtime: Option<SetTime>,
}

/// Who a call is from: AUTH_SYS's user and groups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ctx {
    pub uid: u32,
    pub gid: u32,
    pub gids: Vec<u32>,
}

/// A directory entry a listing returns: its name, file, the cookie a listing resumes after it
/// from, and — for READDIRPLUS — its attributes.
#[derive(Clone, Debug)]
pub struct Entry {
    pub name: Vec<u8>,
    pub fileid: u64,
    pub cookie: u64,
    pub attr: Option<Attr>,
}

/// Entries of a directory, from a cookie on; `eof` when they are the last.
#[derive(Clone, Debug, Default)]
pub struct Listing {
    pub entries: Vec<Entry>,
    pub eof: bool,
}

/// A file system's space and file counts.
#[derive(Clone, Copy, Debug, Default)]
pub struct FsStat {
    pub tbytes: u64,
    pub fbytes: u64,
    pub abytes: u64,
    pub tfiles: u64,
    pub ffiles: u64,
    pub afiles: u64,
}

/// A file system the server serves, by file number. Every method answers for files the server has
/// handed out handles to; one no longer known is ESTALE.
pub trait Filesystem: Send + Sync {
    fn getattr(&self, ctx: &Ctx, ino: u64) -> Result<Attr>;
    fn setattr(&self, ctx: &Ctx, ino: u64, set: &SetAttr) -> Result<()>;
    /// The file `name` names in `dir`, including `.` and `..`.
    fn lookup(&self, ctx: &Ctx, dir: u64, name: &[u8]) -> Result<u64>;
    fn readlink(&self, ctx: &Ctx, ino: u64) -> Result<Vec<u8>>;
    fn read(&self, ctx: &Ctx, ino: u64, offset: u64, count: u32) -> Result<Vec<u8>>;
    fn write(&self, ctx: &Ctx, ino: u64, offset: u64, data: &[u8]) -> Result<u32>;
    /// Makes `name` in `dir`, which must not exist (EEXIST): a file, directory, symlink (to
    /// `target`), FIFO, or socket, with `mode`'s permissions.
    fn create(
        &self,
        ctx: &Ctx,
        dir: u64,
        name: &[u8],
        kind: Kind,
        mode: u32,
        target: &[u8],
    ) -> Result<u64>;
    fn remove(&self, ctx: &Ctx, dir: u64, name: &[u8]) -> Result<()>;
    fn rmdir(&self, ctx: &Ctx, dir: u64, name: &[u8]) -> Result<()>;
    fn rename(&self, ctx: &Ctx, from: u64, name: &[u8], to: u64, to_name: &[u8]) -> Result<()>;
    fn link(&self, ctx: &Ctx, ino: u64, dir: u64, name: &[u8]) -> Result<()>;
    /// Up to `max` of `dir`'s entries after `cookie` (0: from the first), `.` and `..` among them;
    /// with their attributes when `plus`.
    fn readdir(&self, ctx: &Ctx, dir: u64, cookie: u64, plus: bool, max: usize) -> Result<Listing>;
    fn fsstat(&self, ctx: &Ctx, ino: u64) -> Result<FsStat>;
    /// Makes a file's written data durable.
    fn commit(&self, ctx: &Ctx, ino: u64) -> Result<()>;
}

/// How a server runs.
pub struct Options {
    /// The key handles are made under: the same across restarts, for handles the kernel still
    /// holds to keep working.
    pub key: [u8; 32],
    /// The one user ID served.
    pub owner: u32,
    /// Threads answering calls.
    pub workers: usize,
    /// Logs every call — its procedure, user, and status — to standard error.
    pub trace: bool,
    /// The server starts in place of another, whose mounts the kernel still has: a call on an
    /// export not yet served waits for it — or for `ready` — rather than finding its handle stale.
    pub starting: bool,
}

/// How long a call waits for its export while the server starts.
const START_WAIT: std::time::Duration = std::time::Duration::from_secs(120);

struct Export {
    fs: Arc<dyn Filesystem>,
    root: u64,
}

/// The verifiers of exclusive creates lately, by export and file, oldest first.
type Verifiers = VecDeque<((u64, u64), [u8; 8])>;

/// Calls in flight on one connection, and the bound on them.
struct Inflight {
    n: Mutex<usize>,
    freed: Condvar,
}

const INFLIGHT_PER_CONNECTION: usize = 128;

/// A call to answer, and where to write the answer.
struct Job {
    record: Vec<u8>,
    out: Arc<Mutex<TcpStream>>,
    inflight: Arc<Inflight>,
}

pub struct Server {
    handles: Handles,
    owner: u32,
    trace: bool,
    /// The write verifier: new each time a server starts, as NFS requires.
    verifier: [u8; 8],
    exports: RwLock<HashMap<u64, Export>>,
    /// Whether every export a start serves is served: until then, a call on an unknown export
    /// waits (`Options::starting`).
    ready: Mutex<bool>,
    served: Condvar,
    /// One-time MNT paths, and the exports they mount.
    armed: Mutex<HashMap<Vec<u8>, u64>>,
    /// The verifiers of files exclusive CREATEs made lately, so a retransmitted one succeeds.
    exclusive: Mutex<Verifiers>,
    jobs: Mutex<Option<mpsc::Sender<Job>>>,
    stopped: AtomicBool,
    local: Mutex<Option<SocketAddr>>,
}

impl Server {
    /// A server with its workers running; `serve` gives it a listener.
    pub fn new(opts: Options) -> Arc<Self> {
        let mut verifier = [0u8; 8];
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        verifier.copy_from_slice(&(nanos ^ u64::from(std::process::id())).to_be_bytes());
        let (tx, rx) = mpsc::channel::<Job>();
        let server = Arc::new(Self {
            handles: Handles::new(opts.key),
            owner: opts.owner,
            trace: opts.trace,
            verifier,
            exports: RwLock::new(HashMap::new()),
            ready: Mutex::new(!opts.starting),
            served: Condvar::new(),
            armed: Mutex::new(HashMap::new()),
            exclusive: Mutex::new(VecDeque::new()),
            jobs: Mutex::new(Some(tx)),
            stopped: AtomicBool::new(false),
            local: Mutex::new(None),
        });
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..opts.workers.max(1) {
            let rx = rx.clone();
            let server = Arc::downgrade(&server);
            std::thread::Builder::new()
                .name(format!("okfs-nfs-{i}"))
                .spawn(move || {
                    loop {
                        let job = rx.lock().unwrap().recv();
                        let Ok(job) = job else { return };
                        let Some(server) = server.upgrade() else {
                            return;
                        };
                        if let Some(reply) = server.answer(&job.record) {
                            let mut out = job.out.lock().unwrap();
                            let _ = rpc::write_record(&mut *out, &reply);
                        }
                        let mut n = job.inflight.n.lock().unwrap();
                        *n -= 1;
                        job.inflight.freed.notify_one();
                    }
                })
                .expect("starting an NFS worker");
        }
        server
    }

    /// Serves `fs` as export `id`, its root being file `root`; replaces what `id` served.
    pub fn export(&self, id: u64, root: u64, fs: Arc<dyn Filesystem>) {
        self.exports
            .write()
            .unwrap()
            .insert(id, Export { fs, root });
        let _g = self.ready.lock().unwrap();
        self.served.notify_all();
    }

    /// Every export the start serves is served: a call on any other is stale from now on.
    pub fn ready(&self) {
        *self.ready.lock().unwrap() = true;
        self.served.notify_all();
    }

    /// Stops serving export `id`: its handles are stale from now on.
    pub fn unexport(&self, id: u64) {
        self.exports.write().unwrap().remove(&id);
        self.armed.lock().unwrap().retain(|_, e| *e != id);
    }

    /// Lets one MNT of `path` mount export `id`, once.
    pub fn arm(&self, path: &[u8], id: u64) {
        self.armed.lock().unwrap().insert(path.to_vec(), id);
    }

    /// Takes back an armed path no MNT used (the mount failed).
    pub fn disarm(&self, path: &[u8]) {
        self.armed.lock().unwrap().remove(path);
    }

    /// Whether an armed path is still unused.
    pub fn armed(&self, path: &[u8]) -> bool {
        self.armed.lock().unwrap().contains_key(path)
    }

    /// The handle of file `ino` in export `id`.
    pub fn handle(&self, id: u64, ino: u64) -> [u8; handle::LEN] {
        self.handles.make(id, ino)
    }

    /// Accepts connections on `listener` until `stop`.
    pub fn serve(self: &Arc<Self>, listener: TcpListener) -> io::Result<()> {
        *self.local.lock().unwrap() = listener.local_addr().ok();
        for conn in listener.incoming() {
            if self.stopped.load(Ordering::SeqCst) {
                break;
            }
            let Ok(conn) = conn else { continue };
            let _ = conn.set_nodelay(true);
            let server = self.clone();
            let _ = std::thread::Builder::new()
                .name("okfs-nfs-conn".into())
                .spawn(move || server.connection(conn));
        }
        Ok(())
    }

    /// Stops accepting connections and answering calls.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.jobs.lock().unwrap().take();
        if let Some(addr) = *self.local.lock().unwrap() {
            let _ = TcpStream::connect(addr);
        }
    }

    fn connection(self: Arc<Self>, conn: TcpStream) {
        let Ok(out) = conn.try_clone() else { return };
        let out = Arc::new(Mutex::new(out));
        let inflight = Arc::new(Inflight {
            n: Mutex::new(0),
            freed: Condvar::new(),
        });
        let mut reader = io::BufReader::with_capacity(256 << 10, conn);
        // A WRITE of the most data, and its header.
        let max = MAX_IO as usize + 4096;
        while !self.stopped.load(Ordering::SeqCst) {
            let record = match rpc::read_record(&mut reader, max) {
                Ok(Some(r)) => r,
                _ => break,
            };
            {
                let mut n = inflight.n.lock().unwrap();
                while *n >= INFLIGHT_PER_CONNECTION {
                    n = inflight.freed.wait(n).unwrap();
                }
                *n += 1;
            }
            let job = Job {
                record,
                out: out.clone(),
                inflight: inflight.clone(),
            };
            let sent = match self.jobs.lock().unwrap().as_ref() {
                Some(tx) => tx.send(job).is_ok(),
                None => false,
            };
            if !sent {
                break;
            }
        }
        let _ = out.lock().unwrap().shutdown(std::net::Shutdown::Both);
    }

    /// The reply to a call record; None for a record that is no call at all.
    fn answer(&self, record: &[u8]) -> Option<Vec<u8>> {
        let call = match rpc::parse_call(record) {
            Ok(c) => c,
            Err(Unanswerable::Garbage) => return None,
            Err(Unanswerable::RpcMismatch(xid)) => return Some(rpc::rpc_mismatch(xid)),
            Err(Unanswerable::BadCred(xid)) => {
                return Some(rpc::auth_error(xid, AuthStat::BadCred));
            }
        };
        let xid = call.xid;
        let ctx = match &call.cred {
            Cred::Sys { uid, gid, gids } => Some(Ctx {
                uid: *uid,
                gid: *gid,
                gids: gids.clone(),
            }),
            _ => None,
        };
        // A NULL call pings, whoever asks; anything else needs AUTH_SYS.
        if call.proc_ != 0 && ctx.is_none() {
            return Some(rpc::auth_error(xid, AuthStat::TooWeak));
        }
        let result = match (call.prog, call.vers) {
            (NFS_PROGRAM, 3) => nfs3::nfs(self, ctx.as_ref(), call.proc_, call.args),
            (MOUNT_PROGRAM, 3) => nfs3::mount(self, ctx.as_ref(), call.proc_, call.args),
            (NFS_PROGRAM, _) | (MOUNT_PROGRAM, _) => {
                return Some(rpc::accepted(
                    xid,
                    Accept::ProgMismatch,
                    &rpc::versions(3, 3),
                ));
            }
            _ => Err(Accept::ProgUnavail),
        };
        if self.trace {
            let status = match &result {
                Ok(body) if body.len() >= 4 && call.proc_ != 0 => {
                    u32::from_be_bytes(body[..4].try_into().unwrap()).to_string()
                }
                Ok(_) => "-".into(),
                Err(stat) => format!("{stat:?}"),
            };
            let uid = ctx.as_ref().map_or(-1, |c| i64::from(c.uid));
            eprintln!(
                "okfs-nfs: xid {xid} program {} procedure {} uid {uid}: {status}",
                call.prog, call.proc_
            );
        }
        Some(match result {
            Ok(body) => rpc::accepted(xid, Accept::Success, &body),
            Err(stat) => rpc::accepted(xid, stat, &[]),
        })
    }

    /// The export a handle names and its file, or the NFS status to answer with. While the server
    /// starts, an export not yet served is waited for.
    fn open(&self, fh: &[u8]) -> std::result::Result<(Arc<dyn Filesystem>, u64, u64), u32> {
        let (id, ino) = self.handles.open(fh).ok_or(nfs3::BADHANDLE)?;
        let deadline = std::time::Instant::now() + START_WAIT;
        loop {
            if let Some(e) = self.exports.read().unwrap().get(&id) {
                return Ok((e.fs.clone(), id, ino));
            }
            let ready = self.ready.lock().unwrap();
            if *ready || self.exports.read().unwrap().contains_key(&id) {
                if *ready {
                    return Err(nfs3::STALE);
                }
                continue;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(nfs3::STALE);
            }
            drop(self.served.wait_timeout(ready, deadline - now).unwrap());
        }
    }

    /// Takes an armed path's export, and its root's handle.
    fn mount_path(&self, path: &[u8]) -> Option<[u8; handle::LEN]> {
        let id = self.armed.lock().unwrap().remove(path)?;
        let exports = self.exports.read().unwrap();
        let e = exports.get(&id)?;
        Some(self.handles.make(id, e.root))
    }

    /// Records an exclusive create's verifier, keeping the last 1024.
    fn created_exclusive(&self, id: u64, ino: u64, verf: [u8; 8]) {
        let mut ex = self.exclusive.lock().unwrap();
        ex.retain(|(k, _)| *k != (id, ino));
        ex.push_back(((id, ino), verf));
        while ex.len() > 1024 {
            ex.pop_front();
        }
    }

    fn exclusive_verifier(&self, id: u64, ino: u64) -> Option<[u8; 8]> {
        self.exclusive
            .lock()
            .unwrap()
            .iter()
            .find(|(k, _)| *k == (id, ino))
            .map(|(_, v)| *v)
    }
}
