//! The okfs daemon (docs/okfs.md, "The daemon"): one per user, serving every virtual
//! workspace on the machine. It mounts each registered workspace (adopting its connection after a
//! restart), answers jj on the control socket, keeps each workspace's lease, and takes
//! checkpoints as writes go quiet.

use std::collections::HashMap;
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::fd::OwnedFd;
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use jj_lib::backend::CommitId;
use jj_lib::backend::FileId;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Tree as BackendTree;
use jj_lib::backend::TreeId;
use jj_lib::config::StackedConfig;
use jj_lib::conflict_labels::ConflictLabels;
use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::matchers::EverythingMatcher;
use jj_lib::matchers::NothingMatcher;
use jj_lib::matchers::PrefixMatcher;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo::RepoLoader;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::store::Store;
use jj_lib::tree_merge::MergeOptions;
use ok_proto::ok::okfs::v1 as pb;
use ok_store::OkBackend;
use ok_store::RepoConfig;
use ok_store::Shared;
use ok_store::pb as scm;
use ok_store::shared;
use okfs_fuse as fuse;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::sync::Notify;
use tokio::sync::broadcast;

use crate::control::MAX_MESSAGE;
use crate::control::Paths;
use crate::control::private_dir;
use crate::fs::Fetch;
use crate::fs::Mount;
use crate::fs::SecretStore;
use crate::model::Conflict;
use crate::model::Model;
use crate::model::Stale;
use crate::model::Terms;
use crate::model::terms_in;
use crate::model::terms_out;
use crate::nfs::NfsMount;
use crate::persist::FileLog;
use crate::persist::MemLog;
use crate::snapshot::FsView;
use crate::snapshot::Tracking;
use crate::systemd;

/// The largest new file a checkpoint tracks: ok's `snapshot.max-new-file-size`.
const CHECKPOINT_MAX_NEW_FILE: u64 = 50 << 20;
/// A checkpoint is taken once writes have been quiet this long...
const QUIET: Duration = Duration::from_secs(1);
/// ...or this long after the first unrecorded write, while they continue.
const AT_MOST: Duration = Duration::from_secs(10);
/// How often a held lease is renewed (it lasts 30 seconds).
const RENEW_EVERY: Duration = Duration::from_secs(10);
/// How many threads serve each mount's requests.
const FUSE_THREADS: usize = 8;
/// How often an NFS mount's detached inodes are looked at.
const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// A workspace the daemon serves, as the registry keeps it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Registered {
    pub id: String,
    pub mount_point: PathBuf,
    pub repo_dir: PathBuf,
    pub workspace_name: String,
}

#[derive(Default, Serialize, Deserialize)]
struct Registry {
    #[serde(default)]
    workspace: Vec<Registered>,
}

fn load_registry(paths: &Paths) -> io::Result<Vec<Registered>> {
    match std::fs::read_to_string(paths.registry()) {
        Ok(s) => Ok(toml::from_str::<Registry>(&s)
            .map_err(io::Error::other)?
            .workspace),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

fn save_registry(paths: &Paths, regs: &[Registered]) -> io::Result<()> {
    let text = toml::to_string(&Registry {
        workspace: regs.to_vec(),
    })
    .map_err(io::Error::other)?;
    let tmp = paths.registry().with_extension("toml.tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(tmp, paths.registry())
}

/// This build of the binary: a daemon of another build is restarted.
pub fn build_id() -> String {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| {
        // Linux names the binary this process runs even once its file is replaced; elsewhere the
        // binary is read as the process first asks, before a newer build replaces it.
        let exe = if cfg!(target_os = "linux") {
            PathBuf::from("/proc/self/exe")
        } else {
            std::env::current_exe().unwrap_or_default()
        };
        std::fs::metadata(exe)
            .map(|m| {
                format!(
                    "{}-{}-{}",
                    m.ino(),
                    m.mtime_nsec() + m.mtime() * 1_000_000_000,
                    m.size()
                )
            })
            .unwrap_or_default()
    })
    .clone()
}

/// This machine's ID, made once: what leases and checkpoints name.
pub fn machine_id(paths: &Paths) -> io::Result<String> {
    let p = paths.state.join("machine");
    if let Ok(s) = std::fs::read_to_string(&p) {
        let s = s.trim().to_owned();
        if !s.is_empty() {
            return Ok(s);
        }
    }
    let mut b = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut b))?;
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap_or_default();
    let id = format!("{}-{}", host.trim(), hex::encode(b));
    std::fs::write(&p, &id)?;
    Ok(id)
}

/// What a mount reads from ok: through jj's store for trees and symlinks, and ok's shared
/// state for manifests and ranges.
struct OkFetch {
    store: Arc<Store>,
    shared: Arc<Shared>,
    /// Files a prefetch is reading: a read of one waits for it rather than reading it again.
    inflight: Arc<Mutex<HashMap<[u8; 32], Arc<Done>>>>,
}

/// Whether a prefetch has finished, and the condition its waiting readers wait on.
type Done = (Mutex<bool>, std::sync::Condvar);

/// How long a read from ok may take before it fails (`EIO`): what a process reading a file it
/// never read waits, at most, while ok cannot be reached.
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Runs a fetch on the store's runtime, failing it after `FETCH_TIMEOUT`. It blocks without an
/// executor of its own, so a snapshot (which runs inside one) can fetch the trees it lists.
fn bounded<T: Send + 'static>(
    fut: impl std::future::Future<Output = io::Result<T>> + Send + 'static,
) -> io::Result<T> {
    shared::block(async move {
        match tokio::time::timeout(FETCH_TIMEOUT, fut).await {
            Ok(r) => r,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "ok did not answer in time",
            )),
        }
    })
}

/// A file this small is read whole when its contents are not local: its manifest and chunks in
/// the file batcher's request, shared with concurrent reads.
const WHOLE: u64 = 256 << 10;

impl OkFetch {
    fn new(store: Arc<Store>, shared: Arc<Shared>) -> Self {
        Self {
            store,
            shared,
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Waits (a bounded time) for a prefetch reading `id` to finish.
    fn await_prefetch(&self, id: &[u8; 32]) {
        let Some(slot) = self.inflight.lock().unwrap().get(id).cloned() else {
            return;
        };
        let (done, cv) = &*slot;
        let guard = done.lock().unwrap();
        let _ = cv.wait_timeout_while(guard, Duration::from_secs(10), |d| !*d);
    }
}

fn store_id(id: &[u8]) -> io::Result<[u8; 32]> {
    <[u8; 32]>::try_from(id).map_err(|_| io::Error::other("an ID is not 32 bytes"))
}

fn wait_stats(w: ok_store::calls::WaitStats) -> pb::WaitStats {
    pb::WaitStats {
        reads: w.reads,
        seconds: w.seconds,
        max_seconds: w.max_seconds,
    }
}

fn status_io(s: tonic::Status) -> io::Error {
    match s.code() {
        tonic::Code::NotFound => io::Error::from_raw_os_error(libc::ENOENT),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            io::Error::from_raw_os_error(libc::EIO)
        }
        _ => io::Error::other(s.message().to_owned()),
    }
}

impl Fetch for OkFetch {
    fn tree(&self, id: &TreeId) -> io::Result<Arc<BackendTree>> {
        let (store, id) = (self.store.clone(), id.clone());
        bounded(async move {
            store
                .backend()
                .read_tree(RepoPath::root(), &id)
                .await
                .map(Arc::new)
                .map_err(io::Error::other)
        })
    }

    fn trees(&self, ids: &[TreeId]) -> io::Result<Vec<Arc<BackendTree>>> {
        // Read concurrently: the store batches concurrent reads into one request.
        let (store, ids) = (self.store.clone(), ids.to_vec());
        bounded(async move {
            let backend = store.backend();
            futures::future::join_all(ids.iter().map(|id| backend.read_tree(RepoPath::root(), id)))
                .await
                .into_iter()
                .map(|t| t.map(Arc::new).map_err(io::Error::other))
                .collect()
        })
    }

    fn sizes(&self, ids: &[FileId]) -> io::Result<HashMap<FileId, u64>> {
        let shared = self.shared.clone();
        let ids: Vec<(FileId, [u8; 32])> = ids
            .iter()
            .map(|i| Ok((i.clone(), store_id(i.as_bytes())?)))
            .collect::<io::Result<_>>()?;
        bounded(async move {
            let mut out = HashMap::new();
            let found =
                futures::future::join_all(ids.iter().map(|(_, id)| shared.manifest(*id))).await;
            for ((fid, _), m) in ids.into_iter().zip(found) {
                if let Some(m) = m.map_err(status_io)? {
                    out.insert(fid, m.size);
                }
            }
            Ok(out)
        })
    }

    fn tree_sizes(&self, trees: &[(TreeId, Arc<BackendTree>)]) -> io::Result<HashMap<FileId, u64>> {
        let shared = self.shared.clone();
        let trees: Vec<([u8; 32], Arc<BackendTree>)> = trees
            .iter()
            .map(|(id, t)| Ok((store_id(id.as_bytes())?, t.clone())))
            .collect::<io::Result<_>>()?;
        bounded(async move {
            let records =
                futures::future::join_all(trees.iter().map(|(id, _)| shared.tree_sizes(*id))).await;
            let mut out = HashMap::new();
            for ((id, tree), record) in trees.iter().zip(records) {
                let Some(record) = record.map_err(status_io)? else {
                    continue;
                };
                // The record lists the tree's entries in its order, which is jj's; one that does
                // not match the tree is the SCM's error, and its files are sized one at a time.
                let entries: Vec<_> = tree.entries().collect();
                if entries.len() != record.sizes.len() {
                    eprintln!(
                        "okfs: tree {}'s sizes list {} entries, and it has {}",
                        hex::encode(id),
                        record.sizes.len(),
                        entries.len()
                    );
                    continue;
                }
                for (entry, size) in entries.iter().zip(&record.sizes) {
                    if let jj_lib::backend::TreeValue::File { id, .. } = entry.value() {
                        out.insert(id.clone(), *size);
                    }
                }
            }
            Ok(out)
        })
    }

    fn symlink(&self, id: &SymlinkId) -> io::Result<Vec<u8>> {
        let (store, id) = (self.store.clone(), id.clone());
        bounded(async move {
            store
                .read_symlink(RepoPath::root(), &id)
                .await
                .map(String::into_bytes)
                .map_err(io::Error::other)
        })
    }

    fn content(&self, id: &FileId, to: &Path) -> io::Result<()> {
        let shared = self.shared.clone();
        let sid = store_id(id.as_bytes())?;
        let manifest = {
            let shared = shared.clone();
            bounded(async move { shared.file(sid).await.map_err(status_io) })?
        }
        .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
        let mut out = std::io::BufWriter::new(std::fs::File::create(to)?);
        for (cid, _) in &manifest.chunks {
            let data = shared.chunk(cid).ok_or_else(|| {
                io::Error::other(format!(
                    "chunk {} of file {} is not local",
                    hex::encode(cid),
                    id.hex()
                ))
            })?;
            std::io::Write::write_all(&mut out, &data)?;
        }
        std::io::Write::flush(&mut out)
    }

    fn read(&self, id: &FileId, size: u64, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let shared = self.shared.clone();
        let sid = store_id(id.as_bytes())?;
        self.await_prefetch(&sid);
        bounded(async move {
            // A small file whose contents are not local is fetched whole, batched with
            // concurrent reads, whether or not its size is known: a presentation knows every
            // file's size from the start.
            if size <= WHOLE && !shared.cache.has_file(&sid) {
                shared.file(sid).await.map_err(status_io)?;
            }
            shared.read_range(sid, offset, len).await.map_err(status_io)
        })
    }

    fn prefetch(&self, files: Vec<(FileId, Option<u64>)>, max: u64) {
        let shared = self.shared.clone();
        let mut files: Vec<([u8; 32], Option<u64>)> = files
            .iter()
            .filter_map(|(i, size)| store_id(i.as_bytes()).ok().map(|id| (id, *size)))
            .collect();
        files.retain(|(id, _)| !shared.cache.has_file(id));
        let slot = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        {
            let mut inflight = self.inflight.lock().unwrap();
            files.retain(|(id, _)| !inflight.contains_key(id));
            for (id, _) in &files {
                inflight.insert(*id, slot.clone());
            }
        }
        if files.is_empty() {
            return;
        }
        let ids: Vec<[u8; 32]> = files.iter().map(|(id, _)| *id).collect();
        let inflight = self.inflight.clone();
        // The small files' contents, in one batch: a size not given is read first (one batch of
        // manifests; local for sizes already known). A failure only means a later read fetches
        // the file itself.
        shared::spawn(async move {
            let reads = futures::future::join_all(files.iter().map(|(id, size)| {
                let shared = shared.clone();
                async move {
                    let size = match size {
                        Some(size) => Some(*size),
                        None => match shared.manifest(*id).await {
                            Ok(Some(m)) => Some(m.size),
                            _ => None,
                        },
                    };
                    if size.is_some_and(|size| size <= max) {
                        let _ = shared.file(*id).await;
                    }
                }
            }));
            let _ = tokio::time::timeout(FETCH_TIMEOUT, reads).await;
            let mut map = inflight.lock().unwrap();
            for id in &ids {
                map.remove(id);
            }
            drop(map);
            *slot.0.lock().unwrap() = true;
            slot.1.notify_all();
        });
    }
}

/// A workspace's secrets, from ok secrets (docs/secrets.md, "In a virtual workspace"): what its mount
/// reads and writes as the files under `secret/`.
struct OkSecrets {
    shared: Arc<Shared>,
}

/// A refusal of ok secrets' as the errno a file operation fails with — `EACCES` for what the
/// caller may not do, or has not unsealed — with why, and what to run, in the daemon's log.
fn secret_errno(path: &str, s: tonic::Status) -> io::Error {
    eprintln!("okfs: {path}: {}", s.message());
    io::Error::from_raw_os_error(match s.code() {
        tonic::Code::PermissionDenied
        | tonic::Code::Unauthenticated
        | tonic::Code::FailedPrecondition => libc::EACCES,
        tonic::Code::InvalidArgument => libc::EINVAL,
        _ => libc::EIO,
    })
}

impl SecretStore for OkSecrets {
    fn keeps_secrets(&self) -> bool {
        let shared = self.shared.clone();
        bounded(async move { Ok(shared.keeps_secrets().await) }).unwrap_or(true)
    }

    fn read(&self, path: &str, digest: &ok_secret::Digest) -> io::Result<ok_secret::SecretBuf> {
        let (shared, p, d) = (self.shared.clone(), path.to_owned(), *digest);
        bounded(async move {
            shared
                .read_secret(&p, &d)
                .await
                .map_err(|s| secret_errno(&p, s))
        })
    }

    fn write(&self, path: &str, value: &[u8]) -> io::Result<ok_secret::Digest> {
        let (shared, p, v) = (
            self.shared.clone(),
            path.to_owned(),
            ok_secret::SecretBuf::new(value.to_vec()),
        );
        // Only a snapshot seals: its error says why, as ok secrets did.
        bounded(async move {
            shared
                .write_secret(&p, v.as_slice())
                .await
                .map_err(|s| io::Error::other(s.message().to_owned()))
        })
    }

    fn may_write(&self, path: &str) -> io::Result<bool> {
        let (shared, p) = (self.shared.clone(), path.to_owned());
        bounded(async move {
            let a = shared
                .secret_access(&p)
                .await
                .map_err(|s| secret_errno(&p, s))?;
            if !a.write {
                eprintln!("okfs: {p}: {}", a.why_not_write);
            }
            Ok(a.write)
        })
    }
}

/// The file system, read in process by the checkpointer.
struct LocalView(Arc<Mount>);

impl FsView for LocalView {
    fn list_dir(&self, path: &RepoPath) -> io::Result<pb::ListDirReply> {
        self.0.list_dir(path).map_err(io::Error::from_raw_os_error)
    }

    fn seal_secret(&self, path: &RepoPath) -> io::Result<Option<pb::DirEntry>> {
        self.0.seal_secret(path)
    }

    fn remember(&self, files: Vec<pb::Remembered>) -> io::Result<()> {
        let mut m = self.0.model();
        for f in files {
            m.remember(f.ino, f.version, FileId::new(f.file_id));
        }
        Ok(())
    }
}

/// A workspace's lease, as the daemon holds it.
#[derive(Default)]
struct LeaseState {
    lease: Option<scm::Lease>,
    /// Why it is not held, when it is not.
    status: String,
    /// What is wrong with reaching ok, while something is: a renewal that could not reach it,
    /// or a checkpoint that failed.
    trouble: Option<String>,
    latest: i64,
    latest_tree: Option<Terms>,
    /// The model's change count the latest checkpoint covered: None until one is recorded (or
    /// found unneeded) in this daemon's life, so a restarted daemon checks what its predecessor
    /// left unrecorded.
    covered_seq: Option<u64>,
}

/// A mounted workspace.
struct Active {
    reg: Registered,
    mount: Arc<Mount>,
    store: Arc<Store>,
    shared: Arc<Shared>,
    lease: tokio::sync::Mutex<LeaseState>,
    /// Milliseconds (since the daemon started) of the first and the latest unrecorded write; 0
    /// when none.
    first_change: Arc<AtomicU64>,
    last_change: Arc<AtomicU64>,
    events: broadcast::Sender<pb::Event>,
    stop: Notify,
    /// The FUSE session's thread.
    session: Mutex<Option<std::thread::JoinHandle<io::Result<()>>>>,
    /// The workspace as NFS serves it (macOS).
    nfs: Option<Arc<NfsMount>>,
}

pub struct Daemon {
    paths: Paths,
    machine: String,
    version: String,
    started: Instant,
    active: tokio::sync::Mutex<HashMap<String, Arc<Active>>>,
    stores: Mutex<HashMap<PathBuf, Arc<Store>>>,
    /// Descriptors systemd kept for mounts, waiting to be adopted.
    adoptable: Mutex<HashMap<String, OwnedFd>>,
    settings: UserSettings,
    /// Commits presented for ok's roles, by mount point: each slot's lock orders the Presents
    /// at one mount point.
    presented: tokio::sync::Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<Option<Presentation>>>>>,
    /// The NFS server every workspace is served by (macOS), from the daemon's start.
    #[cfg(target_os = "macos")]
    nfsd: Mutex<Option<Arc<crate::nfsd::Nfsd>>>,
}

/// A commit presented read-only for one of ok's roles (docs/okfs.md, "ok's roles").
struct Presentation {
    mount_point: PathBuf,
    repository_id: i64,
    commit: Vec<u8>,
    base: Terms,
    /// The files shown at the root beside the commit's.
    files: Vec<std::ffi::OsString>,
    mount: Arc<Mount>,
    /// Its repository's shared state, for its reads' waits.
    shared: Arc<Shared>,
    session: Option<std::thread::JoinHandle<io::Result<()>>>,
}

impl Presentation {
    /// Whether its connection is still served: an unmount from outside ends it.
    fn alive(&self) -> bool {
        self.session.as_ref().is_some_and(|h| !h.is_finished())
    }
}

/// Why a request failed: what to say, and whether it was refused for what it asked
/// (`Response.refused`).
struct Failure {
    message: String,
    refused: bool,
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self {
            message,
            refused: false,
        }
    }
}

impl From<&str> for Failure {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}

fn fdname(id: &str) -> String {
    format!("fuse-{id}")
}

impl Daemon {
    fn new(paths: Paths) -> io::Result<Self> {
        let settings =
            UserSettings::from_config(StackedConfig::with_defaults()).map_err(io::Error::other)?;
        Ok(Self {
            machine: machine_id(&paths)?,
            paths,
            version: build_id(),
            started: Instant::now(),
            active: tokio::sync::Mutex::new(HashMap::new()),
            stores: Mutex::new(HashMap::new()),
            adoptable: Mutex::new(HashMap::new()),
            settings,
            presented: tokio::sync::Mutex::new(HashMap::new()),
            #[cfg(target_os = "macos")]
            nfsd: Mutex::new(None),
        })
    }

    /// The NFS server, which `serve` starts before mounting anything.
    #[cfg(target_os = "macos")]
    fn nfsd(&self) -> Result<Arc<crate::nfsd::Nfsd>, String> {
        self.nfsd
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "the NFS server has not started".to_owned())
    }

    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64 + 1
    }

    fn store(&self, repo_dir: &Path) -> io::Result<(Arc<Store>, Arc<Shared>)> {
        let mut stores = self.stores.lock().unwrap();
        let store = match stores.get(repo_dir) {
            Some(s) => s.clone(),
            None => {
                let mut factories = jj_lib::default_backend_factories::default_backend_factories();
                factories.merge(ok_store::factories());
                let loader =
                    RepoLoader::init_from_file_system(&self.settings, repo_dir, &factories)
                        .map_err(io::Error::other)?;
                let s = loader.store().clone();
                stores.insert(repo_dir.to_owned(), s.clone());
                s
            }
        };
        let shared = store
            .backend_impl::<OkBackend>()
            .ok_or_else(|| {
                io::Error::other(format!("{} is not an ok repository", repo_dir.display()))
            })?
            .shared()
            .clone();
        Ok((store, shared))
    }

    /// Takes a workspace's lease; with takeover, waits for the holder to hand over (or its lease
    /// to lapse). Ok(None) when ok cannot be reached: the workspace mounts, and the lease is
    /// taken later.
    async fn attach(
        &self,
        shared: &Arc<Shared>,
        name: &str,
        takeover: bool,
    ) -> Result<Option<scm::AttachResponse>, String> {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let req = scm::AttachRequest {
                repository_id: shared.client.repository_id(),
                workspace: name.to_owned(),
                machine: self.machine.clone(),
                takeover,
            };
            let client = shared.client.clone();
            let resp = shared::run(async move {
                client
                    .workspaces()
                    .await?
                    .attach(req)
                    .await
                    .map(|r| r.into_inner())
            })
            .await;
            match resp {
                Ok(r) if r.granted => return Ok(Some(r)),
                Ok(r) if !takeover => {
                    return Err(format!(
                        "workspace {name} is attached on machine {}; `jj clone --attach {name}` (or `jj fs attach` in it) takes it over",
                        r.holder
                    ));
                }
                Ok(_) => {
                    if Instant::now() > deadline {
                        return Err(format!("workspace {name}'s holder did not hand it over"));
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(s)
                    if matches!(
                        s.code(),
                        tonic::Code::Unavailable
                            | tonic::Code::DeadlineExceeded
                            | tonic::Code::Unknown
                    ) =>
                {
                    return Ok(None);
                }
                Err(s) => return Err(format!("taking workspace {name}'s lease: {}", s.message())),
            }
        }
    }

    /// Mounts a registered workspace: its model loaded (or made from `first` for a new one),
    /// served on a new or adopted connection, with its lease and checkpointer started.
    async fn mount(
        self: &Arc<Self>,
        reg: Registered,
        first: Option<(Terms, Vec<RepoPathBuf>)>,
        takeover: bool,
    ) -> Result<pb::MountReply, String> {
        if self.active.lock().await.contains_key(&reg.id) {
            return Ok(pb::MountReply::default());
        }
        let started = Instant::now();
        let state = self.paths.workspace(&reg.id);
        private_dir(&state).map_err(|e| format!("{}: {e}", state.display()))?;
        let (store, shared) = self.store(&reg.repo_dir).map_err(|e| e.to_string())?;
        let opened = started.elapsed();
        let attach = self.attach(&shared, &reg.workspace_name, takeover).await?;
        let attached = started.elapsed();
        let (log, records) =
            FileLog::open(&state).map_err(|e| format!("{}: {e}", state.display()))?;
        let jj_target = state.join("jj").into_os_string().into_encoded_bytes();
        let adopted = self.adoptable.lock().unwrap().remove(&fdname(&reg.id));
        let mut reply = pb::MountReply::default();
        let fresh_model =
            records.is_empty() || (takeover && attach.as_ref().is_some_and(|a| a.latest.is_some()));
        // A predecessor's NFS mount is served on, its handles naming the model's inodes — unless the
        // model is made anew, whose inodes are numbered anew: then the mount is made anew too.
        #[cfg(target_os = "macos")]
        let nfs_adopt = !fresh_model && self.nfsd()?.adoptable(&reg.mount_point, &state);
        #[cfg(not(target_os = "macos"))]
        let nfs_adopt = false;
        let mut model = if fresh_model {
            let mut base = first
                .as_ref()
                .map(|f| f.0.clone())
                .unwrap_or_else(|| Merge::resolved(None));
            if let Some(cp) = attach.as_ref().and_then(|a| a.latest.as_ref()) {
                base = terms_in(Some(&pb::Tree {
                    ids: cp.tree_ids.clone(),
                }));
                reply.checkpoint_id = cp.id;
                reply.checkpoint_tree = Some(pb::Tree {
                    ids: cp.tree_ids.clone(),
                });
            }
            if !records.is_empty() {
                // Taking over: what this machine held is in its forks; the latest checkpoint wins.
                let _ = std::fs::remove_dir_all(state.join("overlay"));
            }
            let mut log = log;
            crate::persist::Log::compact(&mut log, &[]).map_err(|e| e.to_string())?;
            Model::new(
                base,
                state.join("overlay"),
                jj_target,
                Box::new(log),
                crate::model::now,
            )
            .map_err(|e| e.to_string())?
        } else {
            Model::load(
                records,
                state.join("overlay"),
                jj_target,
                Box::new(log),
                crate::model::now,
                adopted.is_none() && !nfs_adopt,
            )
            .map_err(|e| e.to_string())?
        };
        if let Some((_, sparse)) = &first
            && !sparse.is_empty()
        {
            model
                .set_sparse(sparse.clone())
                .map_err(|_| "setting the sparse patterns".to_owned())?;
        }
        let base = model.base().clone();
        let first_change = Arc::new(AtomicU64::new(0));
        let last_change = Arc::new(AtomicU64::new(0));
        let on_change = {
            let (first, last, started) = (first_change.clone(), last_change.clone(), self.started);
            Box::new(move || {
                let now = started.elapsed().as_millis() as u64 + 1;
                let _ = first.compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst);
                last.store(now, Ordering::SeqCst);
            })
        };
        #[allow(unused_mut)]
        let mut mount = Mount::new(
            model,
            Arc::new(OkFetch::new(store.clone(), shared.clone())),
            state.join("staging"),
            on_change,
        )
        .map_err(|e| e.to_string())?;
        // Over FUSE a secret reads as its value; over NFS, whose client caches file data so no read
        // can be made the only one, as its pointer (docs/secrets.md, "Clients").
        #[cfg(not(target_os = "macos"))]
        mount.use_secrets(Arc::new(OkSecrets {
            shared: shared.clone(),
        }));
        let mount = Arc::new(mount);
        let loaded = started.elapsed();
        #[cfg(target_os = "macos")]
        let (thread, nfs) = {
            let _ = adopted;
            let refresh = Arc::new(crate::nfs::Refresh::new(reg.mount_point.clone()));
            if nfs_adopt {
                refresh.seed(mount.model().inos());
            }
            let nfs = Arc::new(NfsMount::new(mount.clone(), refresh));
            let nfsd = self.nfsd()?;
            let (id, served, mount_point, st) = (
                reg.id.clone(),
                nfs.clone(),
                reg.mount_point.clone(),
                state.clone(),
            );
            tokio::task::spawn_blocking(move || {
                nfsd.serve(&id, served, &mount_point, &st, nfs_adopt)
            })
            .await
            .map_err(|e| e.to_string())??;
            (None, Some(nfs))
        };
        #[cfg(not(target_os = "macos"))]
        let (thread, nfs) = self.serve_fuse(&reg, &state, &mount, adopted)?;
        eprintln!(
            "okfs: mounted {} at {} in {} ms (its store opened at {}, attached at {}, its model loaded at {})",
            reg.workspace_name,
            reg.mount_point.display(),
            started.elapsed().as_millis(),
            opened.as_millis(),
            attached.as_millis(),
            loaded.as_millis()
        );
        let (events, _) = broadcast::channel(1024);
        let lease = LeaseState {
            lease: attach.as_ref().and_then(|a| a.lease.clone()),
            status: if attach.is_some() {
                "held".into()
            } else {
                "acquiring: ok is not reachable".into()
            },
            latest: attach
                .as_ref()
                .and_then(|a| a.latest.as_ref())
                .map(|c| c.id)
                .unwrap_or(0),
            latest_tree: attach.as_ref().and_then(|a| a.latest.as_ref()).map(|c| {
                terms_in(Some(&pb::Tree {
                    ids: c.tree_ids.clone(),
                }))
            }),
            covered_seq: None,
            trouble: None,
        };
        let active = Arc::new(Active {
            reg: reg.clone(),
            mount,
            store,
            shared: shared.clone(),
            lease: tokio::sync::Mutex::new(lease),
            first_change,
            last_change,
            events,
            stop: Notify::new(),
            session: Mutex::new(thread),
            nfs,
        });
        // What the overlay holds is checked against the latest checkpoint once, at once: a
        // restarted daemon records what its predecessor had not.
        active.first_change.store(self.now_ms(), Ordering::SeqCst);
        self.active
            .lock()
            .await
            .insert(reg.id.clone(), active.clone());
        {
            let mut regs = load_registry(&self.paths).map_err(|e| e.to_string())?;
            regs.retain(|r| r.id != reg.id);
            regs.push(reg.clone());
            save_registry(&self.paths, &regs).map_err(|e| e.to_string())?;
        }
        apply_redirections(&self.paths, &active).await;
        tokio::spawn(keep(self.clone(), active.clone()));
        // Every tree under the base, in the background: listings are local soon after a mount.
        for id in base.iter().flatten() {
            let (shared, id) = (shared.clone(), id.clone());
            tokio::spawn(async move {
                let _ = ok_store::prefetch::trees(shared, &id).await;
            });
        }
        Ok(reply)
    }

    /// Presents a commit read-only at a mount point (docs/okfs.md, "ok's roles"): the
    /// first mounts it; a later one moves the presentation to the new commit's tree, as a
    /// workspace's checkout does, so nothing unread is fetched. Then the files are put at the root,
    /// and what the read-ahead globs name is made local.
    async fn present(self: &Arc<Self>, p: pb::Present) -> Result<pb::PresentReply, Failure> {
        let mount_point = PathBuf::from(&p.mount_point);
        if !mount_point.is_absolute() {
            return Err(format!("{} is not an absolute path", mount_point.display()).into());
        }
        if p.commit_id.len() != 32 {
            return Err(format!("a commit ID of {} bytes", p.commit_id.len()).into());
        }
        for f in &p.files {
            if f.name.is_empty()
                || f.name == "."
                || f.name == ".."
                || f.name == crate::model::JJ_NAME
                || f.name.contains('/')
            {
                return Err(format!("{:?} is not a name at the root", f.name).into());
            }
        }
        let read_ahead = read_ahead_matcher(&p.read_ahead)?;
        let slot = self
            .presented
            .lock()
            .await
            .entry(mount_point.clone())
            .or_default()
            .clone();
        let mut slot = slot.lock().await;
        let (store, shared) = self.presented_store(&p.endpoint, p.repository_id)?;
        let commit = {
            let (store, id) = (store.clone(), CommitId::new(p.commit_id.clone()));
            shared::run(async move { store.get_commit_async(&id).await })
                .await
                .map_err(|e| format!("reading commit {}: {e}", hex::encode(&p.commit_id)))?
        };
        let tree = commit.tree();
        let Some(tree_id) = tree.tree_ids().as_resolved().cloned() else {
            return Err(Failure {
                message: format!(
                    "commit {} has conflicts: it has no tree to present",
                    hex::encode(&p.commit_id)
                ),
                refused: true,
            });
        };
        let new = Merge::resolved(Some(tree_id.clone()));

        let alive = slot
            .as_ref()
            .is_some_and(|pr| pr.repository_id == p.repository_id && pr.alive());
        let mounted = !alive;
        if alive {
            let pr = slot.as_mut().expect("a live presentation");
            let (mount, files, old, new) = (
                pr.mount.clone(),
                std::mem::take(&mut pr.files),
                pr.base.clone(),
                new.clone(),
            );
            let stale = tokio::task::spawn_blocking(move || -> Result<Vec<Stale>, i32> {
                let mut stale = Vec::new();
                for f in &files {
                    stale.extend(mount.with(|m| m.drop_file(f))?);
                }
                let (_, moved) = mount.with(|m| m.check_out(old.clone(), new.clone(), &[]))?;
                stale.extend(moved);
                Ok(stale)
            })
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| {
                format!(
                    "moving the presentation: {}",
                    io::Error::from_raw_os_error(e)
                )
            })?;
            pr.mount.invalidate(&stale);
        } else {
            if let Some(old) = slot.take() {
                unpresent(old);
            }
            *slot = Some(self.mount_presentation(
                &mount_point,
                p.repository_id,
                store,
                shared.clone(),
                new.clone(),
            )?);
        }
        let pr = slot.as_mut().expect("a presentation");
        pr.base = new;
        pr.commit = p.commit_id.clone();
        for f in p.files {
            let name = std::ffi::OsString::from(f.name);
            let mount = pr.mount.clone();
            let n = name.clone();
            let stale =
                tokio::task::spawn_blocking(move || mount.with(|m| m.put_file(&n, &f.contents)))
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| {
                        format!(
                            "putting {name:?} at the root: {}",
                            io::Error::from_raw_os_error(e)
                        )
                    })?;
            pr.mount.invalidate(&stale);
            pr.files.push(name);
        }
        let mut reply = pb::PresentReply {
            tree: Some(pb::Tree {
                ids: vec![tree_id.to_bytes()],
            }),
            mounted,
            ..Default::default()
        };
        match read_ahead {
            Some(wanted) => {
                let got = ok_store::prefetch::tree_and_files(shared, &tree_id, wanted)
                    .await
                    .map_err(|s| format!("reading ahead: {}", s.message()))?;
                reply.read_ahead_files = got.files;
                reply.read_ahead_bytes = got.bytes;
            }
            None if mounted => {
                // Every tree under the commit, in the background: listings are local soon after.
                tokio::spawn(async move {
                    let _ = ok_store::prefetch::trees(shared, &tree_id).await;
                });
            }
            None => {}
        }
        Ok(reply)
    }

    /// A repository's bare store for presentations: ok's stores over a directory of their own
    /// (`presented/repos/<id>/store`), with no jj repository or operation log — a presentation
    /// reads commits, trees, and files, and records nothing.
    fn presented_store(
        &self,
        endpoint: &str,
        repository_id: i64,
    ) -> Result<(Arc<Store>, Arc<Shared>), String> {
        if endpoint.is_empty() {
            return Err("the presentation names no endpoint".to_owned());
        }
        let store_dir = self
            .paths
            .presented()
            .join("repos")
            .join(repository_id.to_string())
            .join("store");
        let config = RepoConfig {
            endpoint: endpoint.to_owned(),
            repository_id,
            repository: repository_id.to_string(),
            dev_principal: ok_store::client::dev_principal(endpoint),
            cache: None,
            workspace: None,
        };
        let mut stores = self.stores.lock().unwrap();
        if RepoConfig::read(&store_dir).ok().as_ref() != Some(&config) {
            stores.remove(&store_dir);
            std::fs::create_dir_all(&store_dir)
                .and_then(|()| config.write(&store_dir))
                .map_err(|e| format!("{}: {e}", store_dir.display()))?;
        }
        let store = match stores.get(&store_dir) {
            Some(s) => s.clone(),
            None => {
                let backend = OkBackend::load(&store_dir)
                    .map_err(|e| format!("{}: {e}", store_dir.display()))?;
                let signer = Signer::from_settings(&self.settings).map_err(|e| e.to_string())?;
                let merge =
                    MergeOptions::from_settings(&self.settings).map_err(|e| e.to_string())?;
                let s = Store::new(Box::new(backend), signer, merge);
                stores.insert(store_dir.clone(), s.clone());
                s
            }
        };
        let shared = store
            .backend_impl::<OkBackend>()
            .expect("an ok backend")
            .shared()
            .clone();
        Ok((store, shared))
    }

    /// Mounts a presentation of `base` at `mount_point`: read-only, with no `.jj`, its overlay —
    /// only the files put at its root — under `presented/mounts/`.
    fn mount_presentation(
        &self,
        mount_point: &Path,
        repository_id: i64,
        store: Arc<Store>,
        shared: Arc<Shared>,
        base: Terms,
    ) -> Result<Presentation, String> {
        let state = self
            .paths
            .presented()
            .join("mounts")
            .join(mount_dir_name(mount_point));
        let _ = std::fs::remove_dir_all(&state);
        for d in [state.join("overlay"), state.join("staging")] {
            std::fs::create_dir_all(&d).map_err(|e| format!("{}: {e}", d.display()))?;
        }
        // A dead mount there (a daemon that died) is not missing: preparing detaches it.
        if std::fs::symlink_metadata(mount_point)
            .is_err_and(|e| e.kind() == io::ErrorKind::NotFound)
        {
            std::fs::create_dir_all(mount_point)
                .map_err(|e| format!("{}: {e}", mount_point.display()))?;
        }
        prepare_mount_point(mount_point)?;
        let model = Model::new(
            base.clone(),
            state.join("overlay"),
            Vec::new(),
            Box::new(MemLog::default()),
            crate::model::now,
        )
        .map_err(|e| e.to_string())?;
        let mount = Arc::new(
            Mount::new(
                model,
                Arc::new(OkFetch::new(store, shared.clone())),
                state.join("staging"),
                Box::new(|| {}),
            )
            .map_err(|e| e.to_string())?,
        );
        mount.set_read_only(true);
        let opts = fuse::MountOptions {
            read_only: true,
            ..Default::default()
        };
        let fd = fuse::mount(mount_point, &opts)
            .map_err(|e| format!("mounting {}: {e}", mount_point.display()))?;
        let session = {
            let mount = mount.clone();
            let name = format!("okfs-present-{repository_id}");
            std::thread::Builder::new()
                .name(name)
                .spawn(move || fuse::Session::new(fd).run(mount, FUSE_THREADS, |_| {}))
                .map_err(|e| e.to_string())?
        };
        Ok(Presentation {
            mount_point: mount_point.to_owned(),
            repository_id,
            commit: Vec::new(),
            base,
            files: Vec::new(),
            mount,
            shared,
            session: Some(session),
        })
    }

    /// Serves a workspace's mount over FUSE: the connection systemd kept, adopted, or a new mount.
    /// Returns the session's thread.
    #[cfg(not(target_os = "macos"))]
    #[allow(clippy::type_complexity)]
    fn serve_fuse(
        &self,
        reg: &Registered,
        state: &Path,
        mount: &Arc<Mount>,
        adopted: Option<OwnedFd>,
    ) -> Result<
        (
            Option<std::thread::JoinHandle<io::Result<()>>>,
            Option<Arc<NfsMount>>,
        ),
        String,
    > {
        let negotiated_path = state.join("fuse");
        let (fd, session) = match adopted {
            Some(fd) => {
                let negotiated = read_negotiated(&negotiated_path)
                    .ok_or("an adopted connection without its INIT")?;
                let fd = Arc::new(fd);
                let dup = fd.try_clone().map_err(|e| e.to_string())?;
                (fd, fuse::Session::adopt(dup, negotiated))
            }
            None => {
                prepare_mount_point(&reg.mount_point)?;
                let fd = fuse::mount(&reg.mount_point, &fuse::MountOptions::default())
                    .map_err(|e| format!("mounting {}: {e}", reg.mount_point.display()))?;
                let fd = Arc::new(fd);
                let dup = fd.try_clone().map_err(|e| e.to_string())?;
                (fd, fuse::Session::new(dup))
            }
        };
        let _ = systemd::store_fd(&fdname(&reg.id), &fd);
        let thread = {
            let mount = mount.clone();
            let path = negotiated_path.clone();
            std::thread::Builder::new()
                .name(format!("okfs-{}", reg.id))
                .spawn(move || {
                    session.run(mount, FUSE_THREADS, move |n| write_negotiated(&path, n))
                })
                .map_err(|e| e.to_string())?
        };
        // Requests a dead predecessor read and never answered are sent again.
        if let Some(n) = mount.notifier()
            && n.can_resend()
        {
            let _ = n.resend();
        }
        Ok((Some(thread), None))
    }

    /// Unmounts a workspace, leaving its state.
    async fn unmount(&self, id: &str) -> Result<(), String> {
        let Some(active) = self.active.lock().await.remove(id) else {
            return Ok(());
        };
        // A stored permit: the keeper sees it even if it is mid-checkpoint now.
        active.stop.notify_one();
        let _ = systemd::remove_fd(&fdname(id));
        // Closed first, so nothing more reaches the model or its log; then detached. The
        // connection ends once nothing references the mount — perhaps the very jj process that
        // asked for this, whose working directory is in it — so it is not waited for: its thread
        // exits on its own.
        active.mount.close();
        #[cfg(target_os = "macos")]
        {
            let nfsd = self.nfsd()?;
            let (id, mount_point, state) = (
                id.to_owned(),
                active.reg.mount_point.clone(),
                self.paths.workspace(id),
            );
            tokio::task::spawn_blocking(move || nfsd.unserve(&id, &mount_point, &state))
                .await
                .map_err(|e| e.to_string())??;
        }
        #[cfg(not(target_os = "macos"))]
        fuse::unmount(&active.reg.mount_point)
            .map_err(|e| format!("unmounting {}: {e}", active.reg.mount_point.display()))?;
        drop(active.session.lock().unwrap().take());
        let _ = active.mount.model().compact();
        if let Some(lease) = active.lease.lock().await.lease.take() {
            let client = active.shared.client.clone();
            let _ = shared::run(async move {
                client
                    .workspaces()
                    .await?
                    .release(scm::ReleaseRequest { lease: Some(lease) })
                    .await
            })
            .await;
        }
        Ok(())
    }

    async fn active(&self, id: &str) -> Result<Arc<Active>, String> {
        self.active
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| format!("workspace {id} is not mounted"))
    }

    async fn handle(
        self: &Arc<Self>,
        req: pb::request::Request,
        conn: &mut UnixStream,
    ) -> Result<Option<pb::response::Response>, Failure> {
        use pb::request::Request as Q;
        use pb::response::Response as A;
        Ok(Some(match req {
            Q::Hello(_) => A::Hello(pb::HelloReply {
                version: self.version.clone(),
                pid: std::process::id(),
            }),
            Q::Mount(m) => {
                let reg = Registered {
                    id: m.workspace_id.clone(),
                    mount_point: PathBuf::from(&m.mount_point),
                    repo_dir: PathBuf::from(&m.repo_dir),
                    workspace_name: m.workspace_name.clone(),
                };
                let sparse = m
                    .sparse
                    .iter()
                    .filter_map(|p| RepoPathBuf::from_internal_string(p.as_str()).ok())
                    .collect();
                let first = Some((terms_in(m.tree.as_ref()), sparse));
                if m.attach {
                    self.unmount(&reg.id).await?;
                }
                A::Mount(self.mount(reg, first, m.attach).await?)
            }
            Q::Unmount(u) => {
                self.unmount(&u.workspace_id).await?;
                let mut regs = load_registry(&self.paths).map_err(|e| e.to_string())?;
                regs.retain(|r| r.id != u.workspace_id);
                save_registry(&self.paths, &regs).map_err(|e| e.to_string())?;
                A::Empty(pb::Empty {})
            }
            Q::Forget(f) => {
                if let Ok(active) = self.active(&f.workspace_id).await {
                    checkpoint(&active, None, true).await?;
                }
                self.unmount(&f.workspace_id).await?;
                let mut regs = load_registry(&self.paths).map_err(|e| e.to_string())?;
                regs.retain(|r| r.id != f.workspace_id);
                save_registry(&self.paths, &regs).map_err(|e| e.to_string())?;
                let _ = std::fs::remove_dir_all(self.paths.workspace(&f.workspace_id));
                A::Empty(pb::Empty {})
            }
            Q::ListDir(l) => {
                let active = self.active(&l.workspace_id).await?;
                let path = RepoPathBuf::from_internal_string(l.path).map_err(|e| e.to_string())?;
                let mount = active.mount.clone();
                A::ListDir(
                    tokio::task::spawn_blocking(move || mount.list_dir(&path))
                        .await
                        .map_err(|e| e.to_string())?
                        .map_err(|e| format!("listing: {}", io::Error::from_raw_os_error(e)))?,
                )
            }
            Q::SealSecret(s) => {
                let active = self.active(&s.workspace_id).await?;
                let path = RepoPathBuf::from_internal_string(s.path).map_err(|e| e.to_string())?;
                let mount = active.mount.clone();
                let entry = tokio::task::spawn_blocking(move || mount.seal_secret(&path))
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| format!("storing the secret: {e}"))?;
                A::SealSecret(pb::SealSecretReply { entry })
            }
            Q::RememberFiles(r) => {
                let active = self.active(&r.workspace_id).await?;
                LocalView(active.mount.clone())
                    .remember(r.files)
                    .map_err(|e| e.to_string())?;
                A::Empty(pb::Empty {})
            }
            Q::CheckOut(c) => {
                let active = self.active(&c.workspace_id).await?;
                let reply = check_out(&active, c).await?;
                apply_redirections(&self.paths, &active).await;
                A::CheckOut(reply)
            }
            Q::SetRecorded(s) => {
                let active = self.active(&s.workspace_id).await?;
                let tree = terms_in(s.tree.as_ref());
                active
                    .mount
                    .model()
                    .set_recorded(tree.clone(), s.commit_id.clone())
                    .map_err(|_| "recording the tree".to_owned())?;
                if s.checkpoint {
                    checkpoint(&active, Some(tree), false).await?;
                }
                A::Empty(pb::Empty {})
            }
            Q::SetSparse(s) => {
                let active = self.active(&s.workspace_id).await?;
                let patterns: Vec<RepoPathBuf> = s
                    .patterns
                    .iter()
                    .filter_map(|p| RepoPathBuf::from_internal_string(p.as_str()).ok())
                    .collect();
                active
                    .mount
                    .with(|m| m.set_sparse(patterns.clone()))
                    .map_err(|e| e.to_string())?;
                // A sparse change answers as a checkout does, with what to ask the kernel about.
                A::CheckOut(pb::CheckOutReply {
                    skipped: Vec::new(),
                    refresh: refresh_out(active.mount.invalidate_all(&[])),
                })
            }
            Q::Status(_) => {
                let mut out = pb::StatusReply {
                    version: self.version.clone(),
                    pid: std::process::id(),
                    online: true,
                    workspaces: Vec::new(),
                    presentations: Vec::new(),
                    calls: ok_store::calls::calls()
                        .into_iter()
                        .map(|(method, c)| pb::CallStats {
                            method,
                            calls: c.calls,
                            failures: c.failures,
                            seconds: c.seconds,
                            max_seconds: c.max_seconds,
                            sent_bytes: c.sent_bytes,
                            received_bytes: c.received_bytes,
                        })
                        .collect(),
                    fetches: ok_store::calls::fetches()
                        .into_iter()
                        .map(|(kind, f)| pb::FetchStats {
                            kind: kind.to_owned(),
                            batches: f.batches,
                            keys: f.keys,
                            seconds: f.seconds,
                        })
                        .collect(),
                };
                for reg in load_registry(&self.paths).map_err(|e| e.to_string())? {
                    let mut w = pb::WorkspaceStatus {
                        workspace_id: reg.id.clone(),
                        mount_point: reg.mount_point.display().to_string(),
                        workspace_name: reg.workspace_name.clone(),
                        repo_dir: reg.repo_dir.display().to_string(),
                        ..Default::default()
                    };
                    if let Ok(a) = self.active(&reg.id).await {
                        w.mounted = true;
                        let l = a.lease.lock().await;
                        w.lease = match &l.trouble {
                            Some(t) => {
                                out.online = false;
                                format!("{} ({t})", l.status)
                            }
                            None => l.status.clone(),
                        };
                        w.latest_checkpoint = l.latest;
                        let (files, bytes) = a.mount.model().overlay_stats();
                        w.materialized_files = files;
                        w.overlay_bytes = bytes;
                        w.fetched_files = a.mount.stats.fetched_files.load(Ordering::Relaxed);
                        w.fetched_bytes = a.mount.stats.fetched_bytes.load(Ordering::Relaxed);
                        w.prefetches = a.mount.stats.prefetches.load(Ordering::Relaxed);
                        w.prefetched_files = a.mount.stats.prefetched_files.load(Ordering::Relaxed);
                        w.waits = Some(wait_stats(a.shared.waits()));
                        if l.status.starts_with("acquiring") {
                            out.online = false;
                        }
                    }
                    out.workspaces.push(w);
                }
                let slots: Vec<_> = self.presented.lock().await.values().cloned().collect();
                for slot in slots {
                    let slot = slot.lock().await;
                    let Some(pr) = slot.as_ref() else {
                        continue;
                    };
                    let st = &pr.mount.stats;
                    out.presentations.push(pb::PresentationStatus {
                        mount_point: pr.mount_point.display().to_string(),
                        repository_id: pr.repository_id,
                        commit_id: pr.commit.clone(),
                        fetched_files: st.fetched_files.load(Ordering::Relaxed),
                        fetched_bytes: st.fetched_bytes.load(Ordering::Relaxed),
                        prefetches: st.prefetches.load(Ordering::Relaxed),
                        prefetched_files: st.prefetched_files.load(Ordering::Relaxed),
                        waits: Some(wait_stats(pr.shared.waits())),
                    });
                }
                A::Status(out)
            }
            Q::Present(p) => A::Present(self.present(p).await?),
            Q::Unpresent(u) => {
                self.unpresent_at(Path::new(&u.mount_point)).await;
                A::Empty(pb::Empty {})
            }
            Q::Prefetch(p) => {
                let active = self.active(&p.workspace_id).await?;
                prefetch(&active, &p.paths).await?;
                A::Empty(pb::Empty {})
            }
            Q::Subscribe(s) => {
                let active = self.active(&s.workspace_id).await?;
                let mut rx = active.events.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(e) => {
                            let r = pb::Response {
                                error: String::new(),
                                refused: false,
                                response: Some(A::Event(e)),
                            };
                            if write_frame_async(conn, &r).await.is_err() {
                                return Ok(None);
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Too far behind: a checkout-shaped event says "look again".
                            let r = pb::Response {
                                error: String::new(),
                                refused: false,
                                response: Some(A::Event(pb::Event {
                                    seq: 0,
                                    paths: Vec::new(),
                                    checkout: true,
                                })),
                            };
                            if write_frame_async(conn, &r).await.is_err() {
                                return Ok(None);
                            }
                        }
                        Err(_) => return Ok(None),
                    }
                }
            }
            Q::Flush(f) => {
                let active = self.active(&f.workspace_id).await?;
                checkpoint(&active, None, true).await?;
                A::Flush(pb::FlushReply {
                    checkpoint_id: active.lease.lock().await.latest,
                })
            }
        }))
    }
}

#[cfg(not(target_os = "macos"))]
fn read_negotiated(p: &Path) -> Option<fuse::Negotiated> {
    let s = std::fs::read_to_string(p).ok()?;
    let (minor, flags) = s.trim().split_once(' ')?;
    Some(fuse::Negotiated {
        minor: minor.parse().ok()?,
        flags: flags.parse().ok()?,
    })
}

#[cfg(not(target_os = "macos"))]
fn write_negotiated(p: &Path, n: fuse::Negotiated) {
    let _ = std::fs::write(p, format!("{} {}", n.minor, n.flags));
}

/// Makes a mount point ready: an existing directory, with no dead mount of a previous daemon on it
/// (a connection that ended leaves ENOTCONN behind).
/// Ends a presentation: refuses everything from now on, and detaches its mount (the connection
/// ends once nothing uses it; its thread exits on its own).
impl Daemon {
    /// Stops presenting at a mount point, and deletes its state.
    async fn unpresent_at(&self, mount_point: &Path) {
        let slot = self.presented.lock().await.remove(mount_point);
        if let Some(slot) = slot
            && let Some(pr) = slot.lock().await.take()
        {
            unpresent(pr);
        }
        let state = self
            .paths
            .presented()
            .join("mounts")
            .join(mount_dir_name(mount_point));
        let _ = std::fs::remove_dir_all(&state);
    }
}

fn unpresent(pr: Presentation) {
    pr.mount.close();
    if let Err(e) = fuse::unmount(&pr.mount_point) {
        eprintln!("okfs: unmounting {}: {e}", pr.mount_point.display());
    }
    drop(pr.session);
}

/// A presentation's state directory's name: its mount point's, hashed.
fn mount_dir_name(mount_point: &Path) -> String {
    use std::hash::Hash as _;
    use std::hash::Hasher as _;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    mount_point.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// The read-ahead's globs, as a matcher of repository-relative paths: `*` stays within a name,
/// `**` crosses directories. None when there are none.
fn read_ahead_matcher(globs: &[String]) -> Result<Option<ok_store::prefetch::Wanted>, String> {
    if globs.is_empty() {
        return Ok(None);
    }
    let mut set = globset::GlobSetBuilder::new();
    for g in globs {
        let glob = globset::GlobBuilder::new(g)
            .literal_separator(true)
            .build()
            .map_err(|e| format!("read-ahead glob {g:?}: {e}"))?;
        set.add(glob);
    }
    let set = set.build().map_err(|e| e.to_string())?;
    Ok(Some(Arc::new(move |p: &str| set.is_match(p))))
}

fn prepare_mount_point(p: &Path) -> Result<(), String> {
    match std::fs::metadata(p) {
        Ok(m) if m.is_dir() => {
            let parent = p.parent().and_then(|pp| std::fs::metadata(pp).ok());
            if parent.is_some_and(|pm| pm.dev() != m.dev()) {
                // Something is mounted there already: a previous mount of this workspace.
                fuse::unmount(p)
                    .map_err(|e| format!("clearing the old mount at {}: {e}", p.display()))?;
            }
            Ok(())
        }
        Ok(_) => Err(format!("{} is not a directory", p.display())),
        Err(e) if e.raw_os_error() == Some(libc::ENOTCONN) => {
            fuse::unmount(p).map_err(|e| format!("clearing the dead mount at {}: {e}", p.display()))
        }
        Err(e) => Err(format!("{}: {e}", p.display())),
    }
}

/// A workspace's keeper: renews its lease, hands it over when asked, takes checkpoints as writes
/// go quiet, and publishes its journal.
async fn keep(daemon: Arc<Daemon>, active: Arc<Active>) {
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut last_renew = Instant::now();
    // After a failed checkpoint, the next waits (doubling, to 30 s), unless one succeeds first.
    let mut backoff = Duration::from_secs(1);
    let mut retry_at = Instant::now();
    let mut last_sweep = Instant::now();
    loop {
        tokio::select! {
            _ = active.stop.notified() => return,
            _ = tick.tick() => {}
        }
        // Over NFS, the inodes a checkout left with no name, once their grace is over.
        if let Some(nfs) = &active.nfs
            && last_sweep.elapsed() >= SWEEP_EVERY
        {
            last_sweep = Instant::now();
            let nfs = nfs.clone();
            let _ = tokio::task::spawn_blocking(move || nfs.sweep()).await;
        }
        // The journal.
        let changed = active.mount.model().take_changed();
        for c in changed {
            let _ = active.events.send(pb::Event {
                seq: active.mount.model().seq(),
                paths: c.paths,
                checkout: c.checkout,
            });
        }
        // The lease.
        if last_renew.elapsed() >= RENEW_EVERY {
            last_renew = Instant::now();
            lease_tick(&daemon, &active).await;
        }
        // A checkpoint, once writes are quiet (or have gone on long enough).
        let first = active.first_change.load(Ordering::SeqCst);
        if first == 0 {
            continue;
        }
        let now = daemon.now_ms();
        let last = active.last_change.load(Ordering::SeqCst);
        if now - last < QUIET.as_millis() as u64 && now - first < AT_MOST.as_millis() as u64 {
            continue;
        }
        if Instant::now() < retry_at {
            continue;
        }
        active.first_change.store(0, Ordering::SeqCst);
        match checkpoint(&active, None, false).await {
            Ok(()) => {
                backoff = Duration::from_secs(1);
                let mut l = active.lease.lock().await;
                if l.trouble
                    .as_ref()
                    .is_some_and(|t| t.starts_with("checkpoint failed"))
                {
                    l.trouble = None;
                }
            }
            Err(e) => {
                eprintln!(
                    "okfs: workspace {}: checkpoint: {e} (again in {backoff:?})",
                    active.reg.workspace_name
                );
                active.lease.lock().await.trouble = Some(format!("checkpoint failed: {e}"));
                retry_at = Instant::now() + backoff;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                let _ = active.first_change.compare_exchange(
                    0,
                    now,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        }
    }
}

async fn lease_tick(daemon: &Arc<Daemon>, active: &Arc<Active>) {
    let held = active.lease.lock().await.lease.clone();
    let Some(lease) = held else {
        // Not held: taken when ok is reachable, unless another machine has it.
        if active.lease.lock().await.status.starts_with("acquiring") {
            match daemon
                .attach(&active.shared, &active.reg.workspace_name, false)
                .await
            {
                Ok(Some(a)) => {
                    let mut l = active.lease.lock().await;
                    l.lease = a.lease;
                    l.status = "held".into();
                    if let Some(cp) = a.latest {
                        l.latest = cp.id;
                        l.latest_tree = Some(terms_in(Some(&pb::Tree { ids: cp.tree_ids })));
                    }
                    // What was written while it was not held is recorded now.
                    let now = daemon.now_ms();
                    let _ = active.first_change.compare_exchange(
                        0,
                        now,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                }
                Ok(None) => {}
                Err(e) => {
                    active.lease.lock().await.status = format!("lost: {e}");
                    active.mount.set_read_only(true);
                }
            }
        }
        return;
    };
    let client = active.shared.client.clone();
    let req = scm::RenewRequest {
        lease: Some(lease.clone()),
    };
    let resp = shared::run(async move {
        client
            .workspaces()
            .await?
            .renew(req)
            .await
            .map(|r| r.into_inner())
    })
    .await;
    match resp {
        Ok(r) if !r.takeover_by.is_empty() => {
            // Another machine asked: a final checkpoint, then the lease goes, and writes stop.
            let _ = checkpoint(active, None, true).await;
            let client = active.shared.client.clone();
            let release = scm::ReleaseRequest { lease: Some(lease) };
            let _ =
                shared::run(async move { client.workspaces().await?.release(release).await }).await;
            active.mount.set_read_only(true);
            let mut l = active.lease.lock().await;
            l.lease = None;
            l.status = format!("lost: handed over to machine {}", r.takeover_by);
        }
        Ok(r) => {
            let mut l = active.lease.lock().await;
            l.lease = r.lease;
            if l.trouble.take().is_some() {
                // Back: what was written meanwhile is recorded now.
                let now = daemon.now_ms();
                let _ = active.first_change.compare_exchange(
                    0,
                    now,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        }
        Err(s) if s.code() == tonic::Code::FailedPrecondition => {
            active.mount.set_read_only(true);
            let mut l = active.lease.lock().await;
            l.lease = None;
            l.status = "lost: another machine took the workspace".into();
        }
        Err(s) => {
            eprintln!(
                "okfs: workspace {}: renewing the lease: {}",
                active.reg.workspace_name,
                s.message()
            );
            active.lease.lock().await.trouble = Some(format!("ok unreachable: {}", s.message()));
        }
    }
}

/// Records a checkpoint of the workspace as it stands (or of `tree`, a snapshot jj just took),
/// unless nothing changed since the latest. With `force`, even an unchanged state is checked.
async fn checkpoint(active: &Arc<Active>, tree: Option<Terms>, force: bool) -> Result<(), String> {
    let mut l = active.lease.lock().await;
    let seq = active.mount.model().seq();
    if tree.is_none() && !force && l.covered_seq == Some(seq) {
        return Ok(());
    }
    let computed = tree.is_none();
    let (recorded, commit, sparse) = {
        let m = active.mount.model();
        let (r, c) = m.recorded();
        (r.clone(), c.to_vec(), m.sparse().to_vec())
    };
    let tree = match tree {
        Some(t) => t,
        None => {
            let view = LocalView(active.mount.clone());
            let store = active.store.clone();
            let recorded_tree = MergedTree::new(
                store.clone(),
                recorded.map(|t| t.clone().unwrap_or_else(|| store.empty_tree_id().clone())),
                ConflictLabels::unlabeled(),
            );
            let snap = tokio::task::spawn_blocking(move || {
                let sparse = PrefixMatcher::new(&sparse);
                let t = Tracking {
                    base_ignores: crate::platform_ignores(&GitIgnoreFile::empty()),
                    start_tracking: &EverythingMatcher,
                    force_tracking: &NothingMatcher,
                    max_new_file_size: CHECKPOINT_MAX_NEW_FILE,
                    sparse: &sparse,
                };
                futures::executor::block_on(crate::snapshot::snapshot(
                    &view,
                    &store,
                    &recorded_tree,
                    &t,
                ))
                .map(|s| s.tree)
            })
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| format!("computing the checkpoint's tree: {e}"))?;
            snap.tree_ids().map(|id| Some(id.clone()))
        }
    };
    if !worth_recording(
        &tree,
        l.latest_tree.as_ref(),
        &recorded,
        computed,
        active.store.empty_tree_id(),
    ) {
        l.covered_seq = Some(seq);
        return Ok(());
    }
    let Some(lease) = l.lease.clone() else {
        // Offline, or not ours: the state stays in the overlay, recorded when the lease is back.
        return Ok(());
    };
    active
        .store
        .backend_impl::<OkBackend>()
        .unwrap()
        .publish_objects()
        .await
        .map_err(|e| format!("publishing the checkpoint's objects: {e}"))?;
    let base = if commit.len() == 32 {
        commit
    } else {
        vec![0; 32]
    };
    let req = scm::RecordCheckpointRequest {
        lease: Some(lease),
        parent_id: l.latest,
        tree_ids: terms_out(&tree)
            .ids
            .into_iter()
            .map(|b| {
                if b.is_empty() {
                    active.store.empty_tree_id().to_bytes()
                } else {
                    b
                }
            })
            .collect(),
        base_commit_id: base,
    };
    let client = active.shared.client.clone();
    let cp = shared::run(async move {
        client
            .workspaces()
            .await?
            .record_checkpoint(req)
            .await
            .map(|r| r.into_inner())
    })
    .await
    .map_err(|s| format!("recording a checkpoint: {}", s.message()))?;
    if !cp.fork {
        l.latest = cp.id;
        l.latest_tree = Some(tree);
    }
    l.covered_seq = Some(seq);
    Ok(())
}

/// Whether a checkpoint of `tree` records anything the server does not already hold: a tree other
/// than the latest checkpoint's — or, before the workspace has one, other than the tree jj recorded
/// (its working-copy commit's), when the daemon computed it itself. A workspace just mounted is
/// checked at once, and one in which nothing was written is the tree jj recorded: recording it
/// would add a checkpoint of nothing, at whatever moment the check ran. A tree jj asks to have
/// checkpointed (a snapshot it took) is recorded unless it is the latest's.
pub(crate) fn worth_recording(
    tree: &Terms,
    latest: Option<&Terms>,
    recorded: &Terms,
    computed: bool,
    empty: &TreeId,
) -> bool {
    let resolved = |t: &Terms| t.map(|id| id.clone().unwrap_or_else(|| empty.clone()));
    match latest {
        Some(latest) => resolved(latest) != resolved(tree),
        None => !computed || resolved(recorded) != resolved(tree),
    }
}

/// Moves a workspace to a new tree (the model's checkout), and drops what the kernel cached of
/// what changed.
async fn check_out(active: &Arc<Active>, c: pb::CheckOut) -> Result<pb::CheckOutReply, String> {
    let old = terms_in(c.old_tree.as_ref());
    let new = terms_in(c.new_tree.as_ref());
    let conflicts: Vec<Conflict> = c
        .conflicts
        .into_iter()
        .filter_map(|m| {
            Some(Conflict {
                path: RepoPathBuf::from_internal_string(m.path).ok()?,
                contents: m.contents,
                executable: m.executable,
                marker_len: m.conflict_marker_len,
            })
        })
        .collect();
    let mount = active.mount.clone();
    let new2 = new.clone();
    let (skipped, stale) = tokio::task::spawn_blocking(move || {
        mount.with(|m| m.check_out(old.clone(), new2.clone(), &conflicts))
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| format!("checking out: {}", io::Error::from_raw_os_error(e)))?;
    active.mount.forget_secrets();
    let refresh = refresh_out(active.mount.invalidate(&stale));
    for id in new.iter().flatten() {
        let (shared, id) = (active.shared.clone(), id.clone());
        tokio::spawn(async move {
            let _ = ok_store::prefetch::trees(shared, &id).await;
        });
    }
    Ok(pb::CheckOutReply {
        skipped: skipped
            .iter()
            .map(|p| p.as_internal_file_string().to_owned())
            .collect(),
        refresh,
    })
}

/// What the jj command that asked for a change asks the kernel about, over NFS (`nfs::ask`).
fn refresh_out(targets: Vec<crate::nfs::Target>) -> Vec<pb::Refresh> {
    targets
        .into_iter()
        .filter_map(|(path, mode)| {
            Some(pb::Refresh {
                path: path.to_str()?.to_owned(),
                mode,
            })
        })
        .collect()
}

/// The file a repository declares its redirections in: one repo-relative path a line, `#`
/// starting a comment.
const REDIRECTIONS: &str = ".ok/redirections";

/// Keeps the workspace's redirections as its `.ok/redirections` declares them
/// (docs/okfs.md, "Tools"): each path a symlink to its own directory on local disk, under the
/// workspace's state (`redirections/<path>`), which outlives checkouts; one no longer declared goes
/// (its directory stays, until the workspace is forgotten). A path where something else stands is
/// left alone, and logged.
async fn apply_redirections(paths: &Paths, active: &Arc<Active>) {
    let mount = active.mount.clone();
    let declared = tokio::task::spawn_blocking(move || {
        let path = RepoPathBuf::from_internal_string(REDIRECTIONS).expect("a valid path");
        mount.read_path(&path)
    })
    .await;
    let declared: Vec<RepoPathBuf> = match declared {
        Ok(Ok(Some(text))) => String::from_utf8_lossy(&text)
            .lines()
            .map(|l| l.split('#').next().unwrap_or("").trim().trim_matches('/'))
            .filter(|l| !l.is_empty())
            .filter_map(|l| RepoPathBuf::from_internal_string(l).ok())
            .filter(|p| !p.is_root() && p.components().all(|c| c.as_internal_str() != ".jj"))
            .collect(),
        Ok(Ok(None)) => Vec::new(),
        Ok(Err(e)) => {
            eprintln!(
                "okfs: workspace {}: reading {REDIRECTIONS}: {e}",
                active.reg.workspace_name
            );
            return;
        }
        Err(_) => return,
    };
    let dir = paths.workspace(&active.reg.id).join("redirections");
    let mount = active.mount.clone();
    let name = active.reg.workspace_name.clone();
    let _ = tokio::task::spawn_blocking(move || {
        let held = mount.model().redirects();
        // Each name changed is dropped from the kernel's cache, a negative entry included.
        let mut touched: Vec<&RepoPath> = Vec::new();
        for p in held.iter().filter(|p| !declared.contains(p)) {
            touched.push(p);
            if let Err(e) = mount.with(|m| m.remove_redirect(p)) {
                eprintln!("okfs: workspace {name}: removing the redirection {}: {}", p.as_internal_file_string(), io::Error::from_raw_os_error(e));
            }
        }
        for p in &declared {
            if !held.contains(p) {
                touched.push(p);
            }
            let target = p.to_fs_path_unchecked(&dir);
            if let Err(e) = std::fs::create_dir_all(&target) {
                eprintln!("okfs: workspace {name}: {}: {e}", target.display());
                continue;
            }
            let bytes = target.as_os_str().as_encoded_bytes().to_vec();
            match mount.with(|m| m.set_redirect(p, &bytes)) {
                Ok(()) => {}
                Err(libc::EEXIST) => eprintln!(
                    "okfs: workspace {name}: {} is declared a redirection, but something else is there; left alone",
                    p.as_internal_file_string()
                ),
                Err(e) => eprintln!("okfs: workspace {name}: redirecting {}: {}", p.as_internal_file_string(), io::Error::from_raw_os_error(e)),
            }
        }
        if let Some(n) = mount.notifier() {
            for p in touched {
                let Some((parent, name)) = p.split() else {
                    continue;
                };
                if let Ok(Some(dir)) = mount.with(|m| m.resolve(parent)) {
                    let _ = n.inval_entry(dir, std::ffi::OsStr::new(name.as_internal_str()));
                }
            }
        }
    })
    .await;
}

/// Reads the files under paths into the cache.
async fn prefetch(active: &Arc<Active>, paths: &[String]) -> Result<(), String> {
    use futures::StreamExt as _;
    let mut ids = Vec::new();
    for p in paths {
        let path = RepoPathBuf::from_internal_string(p.as_str()).map_err(|e| e.to_string())?;
        let mount = active.mount.clone();
        let found = tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            mount.with(|m| {
                out.clear();
                m.files_under(&path, &mut out, 1_000_000)
            })?;
            Ok::<_, i32>(out)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("listing {p}: {}", io::Error::from_raw_os_error(e)))?;
        ids.extend(found);
    }
    let shared = active.shared.clone();
    let results: Vec<_> = futures::stream::iter(ids)
        .map(|id| {
            let shared = shared.clone();
            async move {
                let sid = store_id(id.as_bytes()).map_err(|e| e.to_string())?;
                shared::run(async move { shared.file(sid).await })
                    .await
                    .map_err(|s| s.message().to_owned())
            }
        })
        .buffer_unordered(32)
        .collect()
        .await;
    for r in results {
        r?;
    }
    Ok(())
}

async fn read_frame_async(conn: &mut UnixStream) -> io::Result<Option<pb::Request>> {
    let mut len = [0u8; 4];
    match conn.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let n = u32::from_be_bytes(len) as usize;
    if n > MAX_MESSAGE {
        return Err(io::Error::other("a control message too large"));
    }
    let mut body = vec![0u8; n];
    conn.read_exact(&mut body).await?;
    prost::Message::decode(body.as_slice())
        .map(Some)
        .map_err(io::Error::other)
}

async fn write_frame_async(conn: &mut UnixStream, msg: &pb::Response) -> io::Result<()> {
    let body = prost::Message::encode_to_vec(msg);
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    conn.write_all(&buf).await
}

async fn serve_conn(daemon: Arc<Daemon>, mut conn: UnixStream) {
    while let Ok(Some(req)) = read_frame_async(&mut conn).await {
        let Some(r) = req.request else {
            break;
        };
        let resp = match daemon.handle(r, &mut conn).await {
            Ok(Some(r)) => pb::Response {
                response: Some(r),
                ..Default::default()
            },
            Ok(None) => return,
            Err(f) => pb::Response {
                error: f.message,
                refused: f.refused,
                response: None,
            },
        };
        if write_frame_async(&mut conn, &resp).await.is_err() {
            return;
        }
    }
}

/// The write end of the pipe SIGTERM and SIGINT are reported on.
static STOP_PIPE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

extern "C" fn on_stop_signal(_: libc::c_int) {
    let fd = STOP_PIPE.load(Ordering::Relaxed);
    if fd >= 0 {
        unsafe {
            libc::write(fd, b"x".as_ptr().cast(), 1);
        }
    }
}

/// Takes SIGTERM and SIGINT from jj-cli's handler (which exits at once, with 1) and reports them
/// on a pipe instead, whose read end it returns: the daemon then stops as it means to — a last
/// checkpoint, each log compacted. SIGHUP is ignored: the daemon has no terminal.
fn stop_signals() -> io::Result<OwnedFd> {
    let (read, write) = io::pipe()?;
    // The handler writes to it for the life of the process.
    STOP_PIPE.store(
        std::os::fd::IntoRawFd::into_raw_fd(write),
        Ordering::Relaxed,
    );
    let read = OwnedFd::from(read);
    for (sig, handler) in [
        (
            libc::SIGTERM,
            on_stop_signal as extern "C" fn(libc::c_int) as libc::sighandler_t,
        ),
        (
            libc::SIGINT,
            on_stop_signal as extern "C" fn(libc::c_int) as libc::sighandler_t,
        ),
        (libc::SIGHUP, libc::SIG_IGN),
    ] {
        let mut sa: libc::sigaction = unsafe { std::mem::zeroed() };
        sa.sa_sigaction = handler;
        sa.sa_flags = libc::SA_RESTART;
        unsafe { libc::sigemptyset(&mut sa.sa_mask) };
        if unsafe { libc::sigaction(sig, &sa, std::ptr::null_mut()) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(read)
}

/// Runs the daemon until it is told to stop. Under systemd, mounts stay up across a stop (their
/// connections in its store); otherwise they are unmounted.
pub fn run(paths: Paths) -> io::Result<()> {
    private_dir(&paths.state)?;
    private_dir(&paths.runtime)?;
    private_dir(&paths.workspaces())?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(paths.runtime.join("daemon.lock"))?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(io::Error::other("a okfs daemon is already running"));
    }
    let stop = Arc::new(Notify::new());
    let signalled = stop_signals()?;
    {
        let stop = stop.clone();
        std::thread::Builder::new()
            .name("okfs-signals".into())
            .spawn(move || {
                let mut b = [0u8; 1];
                while unsafe { libc::read(signalled.as_raw_fd(), b.as_mut_ptr().cast(), 1) } < 0
                    && io::Error::last_os_error().raw_os_error() == Some(libc::EINTR)
                {
                }
                stop.notify_one();
            })?;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let r = rt.block_on(serve(paths, stop));
    // A request still being served (a long checkout, a fetch) does not hold up the exit.
    rt.shutdown_timeout(Duration::from_secs(10));
    drop(lock);
    r
}

async fn serve(paths: Paths, stop: Arc<Notify>) -> io::Result<()> {
    let daemon = Arc::new(Daemon::new(paths.clone())?);
    #[cfg(target_os = "macos")]
    {
        let nfsd = crate::nfsd::Nfsd::start(&paths.state)?;
        *daemon.nfsd.lock().unwrap() = Some(Arc::new(nfsd));
    }
    for (name, fd) in systemd::listen_fds() {
        daemon.adoptable.lock().unwrap().insert(name, fd);
    }
    let _ = std::fs::remove_file(paths.socket());
    let listener = UnixListener::bind(paths.socket())?;
    for reg in load_registry(&paths)? {
        if let Err(e) = daemon.mount(reg.clone(), None, false).await {
            eprintln!(
                "okfs: mounting {} at {}: {e}",
                reg.workspace_name,
                reg.mount_point.display()
            );
        }
    }
    // Stored descriptors no workspace claimed: their mounts are gone.
    for (name, _) in daemon.adoptable.lock().unwrap().drain() {
        let _ = systemd::remove_fd(&name);
    }
    #[cfg(target_os = "macos")]
    if let Ok(nfsd) = daemon.nfsd() {
        nfsd.ready();
    }
    let _ = systemd::notify("READY=1", &[]);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                if let Ok((conn, _)) = accepted {
                    tokio::spawn(serve_conn(daemon.clone(), conn));
                }
            }
            _ = stop.notified() => break,
        }
    }
    let _ = systemd::notify("STOPPING=1", &[]);
    // Under systemd or launchd, mounts outlive the daemon: its successor serves them on.
    let supervised = std::env::var_os("NOTIFY_SOCKET").is_some() || crate::client::under_launchd();
    let ids: Vec<String> = daemon.active.lock().await.keys().cloned().collect();
    for id in ids {
        let active = daemon.active(&id).await.ok();
        if let Some(a) = &active {
            let _ = checkpoint(a, None, false).await;
            let mut m = a.mount.model();
            let _ = m.compact();
        }
        if !supervised {
            let _ = daemon.unmount(&id).await;
        }
    }
    let slots: Vec<_> = daemon
        .presented
        .lock()
        .await
        .drain()
        .map(|(_, s)| s)
        .collect();
    for slot in slots {
        if let Some(pr) = slot.lock().await.take() {
            unpresent(pr);
        }
    }
    let _ = std::fs::remove_file(paths.socket());
    Ok(())
}
