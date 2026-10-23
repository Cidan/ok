//! A mounted workspace (docs/okfs.md, "The file system"): the FUSE operations, served from the
//! model. The model's needs — trees, sizes, symlink targets, contents — are fetched outside its
//! lock and the operation tried again; a file whose content is ok's is read by range, outside
//! the lock too. Kernel notifications are sent once the lock is released. Over NFS (macOS), nfs.rs
//! serves the same mount, and the kernel is told of changes through it (`nfs::Refresh`).
//!
//! A workspace's secrets (docs/secrets.md, "Clients") are files under `secret/` whose contents, as
//! the tree and the overlay hold them, are pointers. Over FUSE the mount serves their values
//! instead: a read-only open fetches its value from ok secrets, using one of the person's unseals,
//! and holds it for that open alone; a write is held in memory — never in the overlay — until a
//! snapshot seals it (`seal_secret`), storing the value in ok secrets and the pointer in the
//! overlay. Over NFS, whose client caches file data, and in a presentation, secrets are pointers.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use jj_lib::backend::FileId;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Tree as BackendTree;
use jj_lib::backend::TreeId;
use jj_lib::object_id::ObjectId as _;
use ok_proto::ok::okfs::v1 as pb;
use ok_secret::Digest;
use ok_secret::SecretBuf;
use okfs_fuse as fuse;

use crate::model::Error;
use crate::model::Ino;
use crate::model::Kind;
use crate::model::Listed;
use crate::model::Model;
use crate::model::Need;
use crate::model::Opened;
use crate::model::R;
use crate::model::ROOT;
use crate::model::Stale;

/// What a mount reads from ok.
pub trait Fetch: Send + Sync {
    fn tree(&self, id: &TreeId) -> io::Result<Arc<BackendTree>>;
    /// Trees read together.
    fn trees(&self, ids: &[TreeId]) -> io::Result<Vec<Arc<BackendTree>>> {
        ids.iter().map(|id| self.tree(id)).collect()
    }
    /// Sizes of files, read together.
    fn sizes(&self, ids: &[FileId]) -> io::Result<HashMap<FileId, u64>>;
    /// The sizes of the files these trees name, from each tree's sizes record
    /// (docs/scm.md, "Tree sizes"): one request for many directories, or none when they
    /// are cached. A tree with no record is left out — its files' sizes are read one at a time,
    /// as `sizes` reads them — and so is every tree by default.
    fn tree_sizes(
        &self,
        _trees: &[(TreeId, Arc<BackendTree>)],
    ) -> io::Result<HashMap<FileId, u64>> {
        Ok(HashMap::new())
    }
    fn symlink(&self, id: &SymlinkId) -> io::Result<Vec<u8>>;
    /// A file's whole contents, written to `to`.
    fn content(&self, id: &FileId, to: &std::path::Path) -> io::Result<()>;
    /// Bytes [offset, offset + len) of a file of `size` bytes.
    fn read(&self, id: &FileId, size: u64, offset: u64, len: u64) -> io::Result<Vec<u8>>;
    /// Reads into the local cache, in the background, each of these files whose size is at most
    /// `max`: those whose size is given at once, and the others once their sizes, read first
    /// together, say so.
    fn prefetch(&self, _files: Vec<(FileId, Option<u64>)>, _max: u64) {}
}

/// What a workspace's mount asks ok secrets about its secrets (docs/secrets.md). Errors are the
/// errno a file operation fails with: `EACCES` for what the caller may not do, or has not unsealed.
pub trait SecretStore: Send + Sync {
    /// Whether the repository keeps secrets: elsewhere `secret/` is an ordinary folder. When ok
    /// secrets cannot say, it does.
    fn keeps_secrets(&self) -> bool;
    /// The value of the version at `path` a pointer's digest names, using one of the person's
    /// unseals of `path`.
    fn read(&self, path: &str, digest: &Digest) -> io::Result<SecretBuf>;
    /// Keeps `value` as a new version of the secret at `path`, returning its digest.
    fn write(&self, path: &str, value: &[u8]) -> io::Result<Digest>;
    /// Whether the caller may write the secrets of `path`'s folder.
    fn may_write(&self, path: &str) -> io::Result<bool>;
}

/// How long ok secrets' answer to whether the caller may write a folder's secrets is kept.
const SECRET_ACCESS_TTL: Duration = Duration::from_secs(60);

/// The repository's settings, whose `secrets:` stanza makes it keep secrets.
const SETTINGS: &str = ".ok/repository.yaml";

/// A version of the workspace's settings: written (its inode and content version), or ok's.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SettingsAt {
    Written(Ino, u64),
    Ok(FileId),
}

/// The most of the settings read for their secrets stanza.
const MAX_SETTINGS: u64 = 1 << 20;

/// A secret's value the mount holds for writing: written since it was sealed, or held for the
/// handles open to write it. A read-only open's value is its handle's alone.
struct SecretFile {
    value: SecretBuf,
    /// The pointer the value is sealed as — the one the file holds — with the value's hash, which
    /// tells a value rewritten as it was from a new one.
    sealed: Option<(Digest, [u8; 32])>,
    /// Written since it was sealed (or read).
    dirty: bool,
    /// Bumped by every write: a seal that raced one leaves the file dirty.
    generation: u64,
    /// The handles open to write it: once none is, and it is sealed, the value is dropped.
    opens: u32,
}

/// A directory's first opens: when the count started, how many, and the model's change count when
/// the directory was prefetched, if it was.
type Siblings = (std::time::Instant, u32, Option<u64>);

/// A directory's files are fetched together once this many of them are first opened within
/// `SIBLING_WINDOW`: a build reading a package reads the rest soon.
const SIBLINGS: u32 = 3;
const SIBLING_WINDOW: Duration = Duration::from_secs(2);
/// The largest file a directory prefetch reads (larger ones are read by range, as used).
const PREFETCH_MAX: u64 = 1 << 20;

/// How long the kernel may keep entries and attributes: they change only when the daemon says so.
const TTL: Duration = Duration::from_secs(3600);

/// An open file or directory.
enum Handle {
    File {
        opened: Opened,
    },
    Dir {
        entries: Arc<Vec<(Listed, Ino)>>,
    },
    /// A secret. Opened to write, it is read and written in `secret_files` (`value` none); opened
    /// to read, `value` is what that open fetched, for it alone.
    Secret {
        ino: Ino,
        value: Option<SecretBuf>,
    },
}

/// What a mount has fetched, for `jj fs status`.
#[derive(Default)]
pub struct Stats {
    pub fetched_files: AtomicU64,
    pub fetched_bytes: AtomicU64,
    /// Directory prefetches started, and the files they asked for.
    pub prefetches: AtomicU64,
    pub prefetched_files: AtomicU64,
}

pub struct Mount {
    model: Mutex<Model>,
    fetch: Arc<dyn Fetch>,
    notifier: Mutex<Option<fuse::Notifier>>,
    /// How the kernel's NFS client is told of changes, for a mount served over NFS.
    refresh: Mutex<Option<Arc<crate::nfs::Refresh>>>,
    handles: Mutex<HashMap<u64, Handle>>,
    next_fh: AtomicU64,
    /// Where whole contents are fetched to, to materialize files.
    staging: PathBuf,
    /// Writes are refused (the lease was lost to another machine).
    read_only: AtomicBool,
    /// Unmounted: every operation is refused and the model is left alone.
    closed: AtomicBool,
    /// First opens of ok's files, by directory.
    siblings: Mutex<HashMap<Ino, Siblings>>,
    pub stats: Stats,
    /// Called after every change to the file system: the checkpointer's and subscribers' cue.
    on_change: Box<dyn Fn() + Send + Sync>,
    /// ok's secrets, for a workspace over FUSE; none for a presentation, or over NFS, which show
    /// pointers.
    secrets: Option<Arc<dyn SecretStore>>,
    /// The secrets whose values the mount holds for writing, by inode: never on disk.
    secret_files: Mutex<HashMap<Ino, SecretFile>>,
    /// Whether the caller may write each folder's secrets, as ok secrets last said.
    secret_access: Mutex<HashMap<String, (bool, Instant)>>,
    /// Whether the workspace's settings keep secrets, for the version of them last read.
    settings: Mutex<Option<(SettingsAt, bool)>>,
    /// Held while a secret is sealed: jj's snapshot and the checkpointer's seal one at a time, so
    /// a value is stored once.
    sealing: Mutex<()>,
}

fn to_errno(e: io::Error) -> i32 {
    e.raw_os_error().unwrap_or(libc::EIO)
}

impl Mount {
    pub fn new(
        model: Model,
        fetch: Arc<dyn Fetch>,
        staging: PathBuf,
        on_change: Box<dyn Fn() + Send + Sync>,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(&staging)?;
        Ok(Self {
            model: Mutex::new(model),
            fetch,
            notifier: Mutex::new(None),
            refresh: Mutex::new(None),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            staging,
            read_only: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            siblings: Mutex::new(HashMap::new()),
            stats: Stats::default(),
            on_change,
            secrets: None,
            secret_files: Mutex::new(HashMap::new()),
            secret_access: Mutex::new(HashMap::new()),
            settings: Mutex::new(None),
            sealing: Mutex::new(()),
        })
    }

    /// Serves the workspace's secrets through ok secrets (docs/secrets.md).
    pub fn use_secrets(&mut self, secrets: Arc<dyn SecretStore>) {
        self.secrets = Some(secrets);
    }

    pub fn model(&self) -> MutexGuard<'_, Model> {
        self.model.lock().unwrap()
    }

    pub fn set_read_only(&self, ro: bool) {
        self.read_only.store(ro, Ordering::SeqCst);
    }

    /// Refuses everything from now on, leaving the model as it is (the mount was unmounted, and
    /// the workspace may be mounted again from its state on disk).
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn writable(&self) -> fuse::Result<()> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(libc::EROFS);
        }
        Ok(())
    }

    /// Runs a model operation, fetching what it needs and trying again.
    pub fn with<T>(&self, mut f: impl FnMut(&mut Model) -> R<T>) -> fuse::Result<T> {
        if self.is_closed() {
            return Err(libc::ENOTCONN);
        }
        let mut staged: Vec<FileId> = Vec::new();
        let result = loop {
            let r = f(&mut self.model());
            match r {
                Ok(t) => break Ok(t),
                Err(Error::Errno(e)) => break Err(e),
                Err(Error::Need(n)) => {
                    if let Err(e) = self.satisfy(&n, &mut staged) {
                        break Err(to_errno(e));
                    }
                }
            }
        };
        if !staged.is_empty() {
            let mut m = self.model();
            for id in staged {
                if let Some(p) = m.cache.contents.remove(&id) {
                    let _ = std::fs::remove_file(p);
                }
            }
        }
        result
    }

    fn satisfy(&self, n: &Need, staged: &mut Vec<FileId>) -> io::Result<()> {
        match n {
            Need::Tree(id) => {
                let t = self.fetch.tree(id)?;
                self.model().cache.trees.insert(id.clone(), t.clone());
                self.learn_sizes(vec![(id.clone(), t)]);
            }
            Need::Trees(ids) => {
                let trees = self.fetch.trees(ids)?;
                let loaded: Vec<_> = ids.iter().cloned().zip(trees).collect();
                {
                    let mut m = self.model();
                    for (id, t) in &loaded {
                        m.cache.trees.insert(id.clone(), t.clone());
                    }
                }
                self.learn_sizes(loaded);
            }
            Need::Size(id) => {
                let sizes = self.fetch.sizes(std::slice::from_ref(id))?;
                let size = *sizes
                    .get(id)
                    .ok_or_else(|| io::Error::from_raw_os_error(libc::EIO))?;
                self.model().cache.sizes.insert(id.clone(), size);
            }
            Need::Symlink(id) => {
                let t = self.fetch.symlink(id)?;
                self.model().cache.symlinks.insert(id.clone(), t);
            }
            Need::Content(id) => {
                let to = self.staging.join(format!(
                    "{}-{}",
                    id.hex(),
                    self.next_fh.fetch_add(1, Ordering::Relaxed)
                ));
                self.fetch.content(id, &to)?;
                let size = std::fs::metadata(&to)?.len();
                self.stats.fetched_files.fetch_add(1, Ordering::Relaxed);
                self.stats.fetched_bytes.fetch_add(size, Ordering::Relaxed);
                self.model().cache.contents.insert(id.clone(), to);
                staged.push(id.clone());
            }
        }
        Ok(())
    }

    /// Counts a first open of one of ok's files; the `SIBLINGS`th in a directory within
    /// `SIBLING_WINDOW` fetches the directory's other small files in the background.
    pub(crate) fn sibling_opened(&self, ino: Ino) {
        let (dir, seq) = {
            let m = self.model();
            (m.parent_of(ino), m.seq())
        };
        let Some(dir) = dir else {
            return;
        };
        {
            let now = std::time::Instant::now();
            let mut s = self.siblings.lock().unwrap();
            let e = s.entry(dir).or_insert((now, 0, None));
            if e.2.is_some_and(|at| at == seq) {
                return;
            }
            if now.duration_since(e.0) > SIBLING_WINDOW {
                *e = (now, 0, None);
            }
            e.1 += 1;
            if e.1 < SIBLINGS {
                return;
            }
            e.2 = Some(seq);
        }
        // The file being opened is read now, directly.
        if let Ok(ids) = self.with(|m| m.prefetchable(dir, PREFETCH_MAX, ino))
            && !ids.is_empty()
        {
            self.stats.prefetches.fetch_add(1, Ordering::Relaxed);
            self.stats
                .prefetched_files
                .fetch_add(ids.len() as u64, Ordering::Relaxed);
            self.fetch.prefetch(ids, PREFETCH_MAX);
        }
    }

    /// Learns the sizes of the files the trees just loaded name, a record a directory, so a stat
    /// of any of them asks ok nothing. Trees whose files' sizes are all known already are
    /// skipped; one whose record is not to be had leaves its files to be sized one at a time.
    fn learn_sizes(&self, loaded: Vec<(TreeId, Arc<BackendTree>)>) {
        let unknown: Vec<_> = {
            let m = self.model();
            loaded
                .into_iter()
                .filter(|(_, t)| {
                    t.entries().any(|e| {
                        matches!(e.value(), jj_lib::backend::TreeValue::File { id, .. }
                            if !m.cache.sizes.contains_key(id))
                    })
                })
                .collect()
        };
        if unknown.is_empty() {
            return;
        }
        if let Ok(sizes) = self.fetch.tree_sizes(&unknown) {
            self.model().cache.sizes.extend(sizes);
        }
    }

    /// Fetches the sizes a directory listing will need, together.
    pub(crate) fn prefetch_sizes(&self, dir: Ino) {
        if let Ok(ids) = self.with(|m| m.missing_sizes(dir)) {
            if ids.is_empty() {
                return;
            }
            if let Ok(sizes) = self.fetch.sizes(&ids) {
                self.model().cache.sizes.extend(sizes);
            }
        }
    }

    fn entry(&self, ino: Ino) -> fuse::Result<fuse::Entry> {
        let mut attr = self.with(|m| m.attr(ino))?;
        let attr_ttl = self.secret_attr(&mut attr);
        Ok(fuse::Entry {
            attr,
            generation: 0,
            entry_ttl: TTL,
            attr_ttl,
        })
    }

    /// Counts a reference the kernel now holds.
    pub(crate) fn counted(&self, ino: Ino) {
        if !self.is_closed() {
            let _ = self.model().lookup_count(ino);
        }
    }

    pub(crate) fn changed(&self) {
        (self.on_change)();
    }

    /// A file's contents as the workspace shows it, by path; None where there is no file.
    pub fn read_path(&self, path: &jj_lib::repo_path::RepoPath) -> io::Result<Option<Vec<u8>>> {
        let ino = match self.with(|m| m.resolve(path)) {
            Ok(Some(i)) => i,
            Ok(None) => return Ok(None),
            Err(e) => return Err(io::Error::from_raw_os_error(e)),
        };
        let opened = match self.with(|m| m.current(ino)) {
            Ok(o) => o,
            Err(libc::EINVAL) => return Ok(None),
            Err(e) => return Err(io::Error::from_raw_os_error(e)),
        };
        match opened {
            Opened::Overlay => self
                .with(|m| m.read_overlay(ino, 0, u32::MAX))
                .map(Some)
                .map_err(io::Error::from_raw_os_error),
            Opened::Value { id, size } => self.fetch.read(&id, size, 0, size).map(Some),
        }
    }

    /// Serves the mount over NFS: changes are pushed to the kernel through `refresh`.
    pub fn set_refresh(&self, refresh: Arc<crate::nfs::Refresh>) {
        *self.refresh.lock().unwrap() = Some(refresh);
    }

    /// Bytes [offset, offset + len) of one of ok's files of `size` bytes, counted as fetched.
    pub(crate) fn read_value(
        &self,
        id: &FileId,
        size: u64,
        offset: u64,
        len: u64,
    ) -> fuse::Result<Vec<u8>> {
        let data = self.fetch.read(id, size, offset, len).map_err(to_errno)?;
        self.stats
            .fetched_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
    }

    /// Sends the kernel the invalidations a change made. Over NFS, it returns what the jj command
    /// that asked for the change must ask the kernel about (`nfs::ask`); over FUSE, nothing.
    pub fn invalidate(&self, stale: &[Stale]) -> Vec<crate::nfs::Target> {
        let refresh = self.refresh.lock().unwrap().clone();
        if let Some(r) = refresh {
            return r.refresh(self, stale);
        }
        let Some(n) = self.notifier.lock().unwrap().clone() else {
            return Vec::new();
        };
        for s in stale {
            let _ = match s {
                Stale::Entry(parent, name) => n.inval_entry(*parent, name),
                Stale::Inode(ino) => n.inval_inode(*ino, 0, 0),
            };
        }
        Vec::new()
    }

    /// Drops every entry the kernel cached, where it can be asked to (a sparse change). Over NFS,
    /// returns what to ask the kernel about, as `invalidate` does.
    pub fn invalidate_all(
        &self,
        dirs: &[(Ino, Vec<std::ffi::OsString>)],
    ) -> Vec<crate::nfs::Target> {
        let refresh = self.refresh.lock().unwrap().clone();
        if let Some(r) = refresh {
            return r.refresh_all(self);
        }
        let Some(n) = self.notifier.lock().unwrap().clone() else {
            return Vec::new();
        };
        if n.inc_epoch().unwrap_or(false) {
            return Vec::new();
        }
        for (dir, names) in dirs {
            for name in names {
                let _ = n.inval_entry(*dir, name);
            }
        }
        Vec::new()
    }

    pub fn notifier(&self) -> Option<fuse::Notifier> {
        self.notifier.lock().unwrap().clone()
    }

    fn new_fh(&self, h: Handle) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.lock().unwrap().insert(fh, h);
        fh
    }
}

/// A `statvfs` count as FUSE carries it: `u64` on Linux, `u32` on macOS.
fn wide(n: impl Into<u64>) -> u64 {
    n.into()
}

fn kind_of(mode: u32) -> Option<Kind> {
    match mode & fuse::abi::S_IFMT {
        fuse::abi::S_IFREG => Some(Kind::File),
        fuse::abi::S_IFDIR => Some(Kind::Dir),
        fuse::abi::S_IFIFO => Some(Kind::Fifo),
        fuse::abi::S_IFSOCK => Some(Kind::Socket),
        _ => None,
    }
}

impl fuse::Filesystem for Mount {
    fn init(&self, notifier: fuse::Notifier) {
        *self.notifier.lock().unwrap() = Some(notifier);
    }

    fn lookup(
        &self,
        _: &fuse::Ctx,
        parent: u64,
        name: &OsStr,
    ) -> fuse::Result<Option<fuse::Entry>> {
        let found = self.with(|m| {
            let ino = m.lookup(parent, name, false)?;
            Ok(match ino {
                Some(i) => Some((i, m.attr(i)?)),
                None => None,
            })
        })?;
        Ok(found.map(|(ino, mut attr)| {
            self.counted(ino);
            let attr_ttl = self.secret_attr(&mut attr);
            fuse::Entry {
                attr,
                generation: 0,
                entry_ttl: TTL,
                attr_ttl,
            }
        }))
    }

    fn forget(&self, ino: u64, nlookup: u64) {
        if !self.is_closed() {
            self.model().forget(ino, nlookup);
        }
    }

    fn getattr(
        &self,
        _: &fuse::Ctx,
        ino: u64,
        _: Option<u64>,
    ) -> fuse::Result<(fuse::Attr, Duration)> {
        let mut attr = self.with(|m| m.attr(ino))?;
        let ttl = self.secret_attr(&mut attr);
        Ok((attr, ttl))
    }

    fn setattr(
        &self,
        _: &fuse::Ctx,
        ino: u64,
        set: &fuse::SetAttr,
    ) -> fuse::Result<(fuse::Attr, Duration)> {
        self.writable()?;
        let mut set = *set;
        if let (Some(size), Some(path)) = (set.size, self.secret_path(ino)) {
            // A secret's size is its value's, which the overlay never holds.
            self.check_write(&path)?;
            if size != 0 && !self.secret_files.lock().unwrap().contains_key(&ino) {
                let (value, sealed) = self.fetch_secret(ino, &path)?;
                self.secret_files
                    .lock()
                    .unwrap()
                    .entry(ino)
                    .or_insert(SecretFile {
                        value,
                        sealed,
                        dirty: false,
                        generation: 0,
                        opens: 0,
                    });
            }
            self.write_secret(ino, |v| v.set_len(size as usize))?;
            set.size = None;
        }
        self.with(|m| m.setattr(ino, &set))?;
        self.changed();
        let mut attr = self.with(|m| m.attr(ino))?;
        let ttl = self.secret_attr(&mut attr);
        Ok((attr, ttl))
    }

    fn readlink(&self, _: &fuse::Ctx, ino: u64) -> fuse::Result<Vec<u8>> {
        self.with(|m| m.readlink(ino))
    }

    fn symlink(
        &self,
        _: &fuse::Ctx,
        parent: u64,
        name: &OsStr,
        target: &[u8],
    ) -> fuse::Result<fuse::Entry> {
        self.writable()?;
        self.only_files_in_secrets(parent, name)?;
        let ino = self.with(|m| m.create(parent, name, Kind::Symlink, 0o777, target))?;
        self.changed();
        self.entry(ino)
    }

    fn mknod(
        &self,
        _: &fuse::Ctx,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _: u32,
    ) -> fuse::Result<fuse::Entry> {
        self.writable()?;
        let kind = kind_of(mode)
            .filter(|k| *k != Kind::Dir)
            .ok_or(libc::EPERM)?;
        self.only_files_in_secrets(parent, name)?;
        let ino = self.with(|m| m.create(parent, name, kind, mode & !umask, b""))?;
        self.changed();
        self.entry(ino)
    }

    fn mkdir(
        &self,
        _: &fuse::Ctx,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> fuse::Result<fuse::Entry> {
        self.writable()?;
        self.utf8_in_secrets(parent, name)?;
        let ino = self.with(|m| m.create(parent, name, Kind::Dir, mode & !umask, b""))?;
        self.changed();
        self.entry(ino)
    }

    fn unlink(&self, _: &fuse::Ctx, parent: u64, name: &OsStr) -> fuse::Result<()> {
        self.writable()?;
        let ino = self.with(|m| m.unlink(parent, name))?;
        if self.model().path_of(ino).is_none() {
            self.secret_files.lock().unwrap().remove(&ino);
        }
        self.changed();
        Ok(())
    }

    fn rmdir(&self, _: &fuse::Ctx, parent: u64, name: &OsStr) -> fuse::Result<()> {
        self.writable()?;
        self.with(|m| m.rmdir(parent, name))?;
        self.changed();
        Ok(())
    }

    fn rename(
        &self,
        _: &fuse::Ctx,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
    ) -> fuse::Result<()> {
        self.writable()?;
        self.utf8_in_secrets(newparent, newname)?;
        // A secret's value is bound to its path: moving one is writing it anew, which `mv` and
        // editors do when told the move crosses file systems.
        if self.touches_secrets(parent, name) || self.touches_secrets(newparent, newname) {
            return Err(libc::EXDEV);
        }
        self.with(|m| m.rename(parent, name, newparent, newname, flags))?;
        self.changed();
        Ok(())
    }

    fn link(
        &self,
        _: &fuse::Ctx,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
    ) -> fuse::Result<fuse::Entry> {
        self.writable()?;
        self.utf8_in_secrets(newparent, newname)?;
        if self.secret_path(ino).is_some() || self.touches_secrets(newparent, newname) {
            return Err(libc::EXDEV);
        }
        self.with(|m| m.link(ino, newparent, newname))?;
        self.changed();
        self.entry(ino)
    }

    fn open(&self, _: &fuse::Ctx, ino: u64, flags: i32) -> fuse::Result<fuse::Open> {
        if let Some(path) = self.secret_path(ino) {
            return self.open_secret(ino, &path, flags);
        }
        if flags & libc::O_ACCMODE != libc::O_RDONLY || flags & libc::O_TRUNC != 0 {
            self.writable()?;
        }
        let opened = self.with(|m| m.open(ino, flags))?;
        if flags & libc::O_TRUNC != 0 {
            self.changed();
        }
        if let Opened::Value { .. } = opened {
            self.sibling_opened(ino);
        }
        let fh = self.new_fh(Handle::File { opened });
        Ok(fuse::Open {
            fh,
            flags: fuse::abi::FOPEN_KEEP_CACHE,
        })
    }

    fn create(
        &self,
        _: &fuse::Ctx,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> fuse::Result<(fuse::Entry, fuse::Open)> {
        self.writable()?;
        self.utf8_in_secrets(parent, name)?;
        if let Some(path) = self.secret_child(parent, name) {
            // A new secret: empty, and written in memory.
            self.check_write(&path)?;
            let ino = self.with(|m| m.create(parent, name, Kind::File, 0o600, b""))?;
            self.with(|m| m.open(ino, flags & !libc::O_TRUNC))?;
            self.write_secret(ino, |v| v.set_len(0))?;
            if let Some(f) = self.secret_files.lock().unwrap().get_mut(&ino) {
                f.opens += 1;
            }
            let fh = self.new_fh(Handle::Secret { ino, value: None });
            return Ok((
                self.entry(ino)?,
                fuse::Open {
                    fh,
                    flags: fuse::abi::FOPEN_DIRECT_IO,
                },
            ));
        }
        let ino = self.with(|m| m.create(parent, name, Kind::File, mode & !umask, b""))?;
        let opened = self.with(|m| m.open(ino, flags & !libc::O_TRUNC))?;
        self.changed();
        let fh = self.new_fh(Handle::File { opened });
        Ok((
            self.entry(ino)?,
            fuse::Open {
                fh,
                flags: fuse::abi::FOPEN_KEEP_CACHE,
            },
        ))
    }

    fn read(
        &self,
        _: &fuse::Ctx,
        ino: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> fuse::Result<Vec<u8>> {
        let known = match self.handles.lock().unwrap().get(&fh) {
            Some(Handle::File { opened }) => Some(opened.clone()),
            _ => None,
        };
        if let Some(path) = self.secret_path(ino) {
            return self.read_secret(ino, fh, &path, offset, size);
        }
        // A handle a previous daemon opened (it restarted): the file as it stands.
        let opened = match known {
            Some(o) => o,
            None => self.with(|m| m.current(ino))?,
        };
        match opened {
            Opened::Overlay => self.with(|m| m.read_overlay(ino, offset, size)),
            Opened::Value { id, size: len } => {
                if offset >= len {
                    return Ok(Vec::new());
                }
                let n = (size as u64).min(len - offset);
                let data = self.fetch.read(&id, len, offset, n).map_err(to_errno)?;
                self.stats
                    .fetched_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                Ok(data)
            }
        }
    }

    fn write(
        &self,
        _: &fuse::Ctx,
        ino: u64,
        _: u64,
        offset: u64,
        data: &[u8],
        _: u32,
    ) -> fuse::Result<u32> {
        self.writable()?;
        if self.secret_path(ino).is_some() {
            self.write_secret(ino, |v| v.write_at(offset as usize, data))?;
            return Ok(data.len() as u32);
        }
        let n = self.with(|m| m.write(ino, offset, data))?;
        self.changed();
        Ok(n)
    }

    fn release(&self, _: &fuse::Ctx, ino: u64, fh: u64, _: i32) {
        let h = self.handles.lock().unwrap().remove(&fh);
        if let Some(h) = h {
            self.release_secret(h);
        }
        if !self.is_closed() {
            self.model().release(ino);
        }
    }

    fn fsync(&self, _: &fuse::Ctx, ino: u64, _: u64, _: bool) -> fuse::Result<()> {
        self.with(|m| m.fsync(ino))
    }

    fn opendir(&self, _: &fuse::Ctx, ino: u64, _: i32) -> fuse::Result<fuse::Open> {
        let entries = self.dir_entries(ino)?;
        let fh = self.new_fh(Handle::Dir { entries });
        Ok(fuse::Open {
            fh,
            flags: fuse::abi::FOPEN_KEEP_CACHE | fuse::abi::FOPEN_CACHE_DIR,
        })
    }

    fn readdir(
        &self,
        _: &fuse::Ctx,
        ino: u64,
        fh: u64,
        offset: u64,
        buf: &mut fuse::DirBuf<'_>,
    ) -> fuse::Result<()> {
        let known = match self.handles.lock().unwrap().get(&fh) {
            Some(Handle::Dir { entries }) => Some(entries.clone()),
            _ => None,
        };
        // A handle a previous daemon opened (it restarted): the directory as it stands, whose
        // order is the same.
        let entries = match known {
            Some(e) => e,
            None => self.dir_entries(ino)?,
        };
        self.fill(ino, &entries, offset, buf)
    }

    fn releasedir(&self, _: &fuse::Ctx, _: u64, fh: u64) {
        self.handles.lock().unwrap().remove(&fh);
    }

    fn statfs(&self, _: &fuse::Ctx, _: u64) -> fuse::Result<fuse::Statfs> {
        self.statvfs()
    }

    fn setxattr(
        &self,
        _: &fuse::Ctx,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
    ) -> fuse::Result<()> {
        self.writable()?;
        self.with(|m| m.setxattr(ino, name, value, flags))
    }

    fn getxattr(&self, _: &fuse::Ctx, ino: u64, name: &OsStr) -> fuse::Result<Vec<u8>> {
        self.with(|m| m.getxattr(ino, name))
    }

    fn listxattr(&self, _: &fuse::Ctx, ino: u64) -> fuse::Result<Vec<u8>> {
        self.with(|m| m.listxattr(ino))
    }

    fn removexattr(&self, _: &fuse::Ctx, ino: u64, name: &OsStr) -> fuse::Result<()> {
        self.writable()?;
        self.with(|m| m.removexattr(ino, name))
    }
}

impl Mount {
    /// A directory's entries as a listing serves them: `.`, `..`, then its names in order.
    pub(crate) fn dir_entries(&self, ino: Ino) -> fuse::Result<Arc<Vec<(Listed, Ino)>>> {
        let listed = self.with(|m| m.list(ino))?;
        let parent = self.model().parent_of(ino).unwrap_or(ROOT);
        let mut entries = vec![
            (
                Listed {
                    name: ".".into(),
                    kind: Kind::Dir,
                },
                ino,
            ),
            (
                Listed {
                    name: "..".into(),
                    kind: Kind::Dir,
                },
                parent,
            ),
        ];
        entries.extend(listed.into_iter().map(|l| (l, 0)));
        Ok(Arc::new(entries))
    }

    /// Fills a listing's buffer from the entry after `offset`. An entry that is gone since the
    /// directory was opened is left out; one that cannot be read is never left out — the kernel
    /// keeps a listing for as long as the directory's inode lives (`FOPEN_CACHE_DIR`), so an
    /// entry skipped once would be missing until then. A failure fails the call if it would
    /// return no entries, and otherwise ends it, so the next call, from that entry, meets it.
    fn fill(
        &self,
        ino: Ino,
        entries: &[(Listed, Ino)],
        offset: u64,
        buf: &mut fuse::DirBuf<'_>,
    ) -> fuse::Result<()> {
        if buf.is_plus() && offset < entries.len() as u64 {
            self.prefetch_sizes(ino);
        }
        let mut filled = false;
        for (i, (l, known)) in entries.iter().enumerate().skip(offset as usize) {
            let dot = i < 2;
            if buf.is_plus() {
                if !buf.fits_plus(&l.name) {
                    break;
                }
                let found = if dot {
                    None
                } else {
                    let looked = self.with(|m| {
                        Ok(match m.lookup(ino, &l.name, false)? {
                            Some(c) => Some((c, m.attr(c)?)),
                            None => None,
                        })
                    });
                    match looked {
                        Ok(found) => found,
                        Err(e) if !filled => return Err(e),
                        Err(_) => break,
                    }
                };
                let found = found.map(|(child, mut attr)| {
                    self.secret_attr(&mut attr);
                    (child, attr)
                });
                let Some((child, attr)) = found.or(if dot {
                    Some((*known, fuse::Attr::default()))
                } else {
                    None
                }) else {
                    // Gone since the directory was opened.
                    continue;
                };
                let entry = fuse::DirEntry {
                    ino: child,
                    offset: i as u64 + 1,
                    kind: l.kind.mode_bits(),
                    name: l.name.clone(),
                };
                let secret =
                    attr.mode & libc::S_IFMT == libc::S_IFREG && self.secret_path(child).is_some();
                let lookup = (!dot).then_some(fuse::Entry {
                    attr,
                    generation: 0,
                    entry_ttl: TTL,
                    attr_ttl: if secret { Duration::ZERO } else { TTL },
                });
                if !buf.push_plus(&fuse::DirEntryPlus { entry, lookup }) {
                    break;
                }
                filled = true;
                if !dot {
                    self.counted(child);
                }
            } else {
                let child = if dot {
                    *known
                } else {
                    match self.with(|m| m.lookup(ino, &l.name, false)) {
                        Ok(Some(c)) => c,
                        Ok(None) => continue,
                        Err(e) if !filled => return Err(e),
                        Err(_) => break,
                    }
                };
                let entry = fuse::DirEntry {
                    ino: child,
                    offset: i as u64 + 1,
                    kind: l.kind.mode_bits(),
                    name: l.name.clone(),
                };
                if !buf.push(&entry) {
                    break;
                }
                filled = true;
            }
        }
        Ok(())
    }

    /// The space and files left where the mount keeps what it writes.
    pub(crate) fn statvfs(&self) -> fuse::Result<fuse::Statfs> {
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        let path = std::ffi::CString::new(self.staging.as_os_str().as_encoded_bytes())
            .map_err(|_| libc::EINVAL)?;
        if unsafe { libc::statvfs(path.as_ptr(), &mut st) } != 0 {
            return Err(to_errno(io::Error::last_os_error()));
        }
        Ok(fuse::Statfs {
            blocks: wide(st.f_blocks),
            bfree: wide(st.f_bfree),
            bavail: wide(st.f_bavail),
            files: wide(st.f_files),
            ffree: wide(st.f_ffree),
            bsize: st.f_bsize as u32,
            namelen: 255,
        })
    }
}

// ─── Secrets (docs/secrets.md, "Clients") ───

impl Mount {
    /// Whether this mount serves secrets: a workspace's, of a repository that keeps them — as ok
    /// secrets says of main, or as the workspace's own settings do, so values stay off disk from
    /// the moment a workspace starts keeping them (ok secrets refuses to store one before main
    /// does, saying why). It may ask ok secrets: never call it holding the model's lock.
    fn keeps(&self) -> bool {
        let Some(store) = self.secrets.as_ref() else {
            return false;
        };
        self.settings_keep() || store.keeps_secrets()
    }

    /// Whether the workspace's settings have a secrets stanza: a line starting `secrets:`. Ones
    /// that cannot be read count as having one.
    fn settings_keep(&self) -> bool {
        let path = jj_lib::repo_path::RepoPathBuf::from_internal_string(SETTINGS).unwrap();
        let ino = match self.with(|m| m.resolve(&path)) {
            Ok(Some(ino)) => ino,
            Ok(None) => return false,
            Err(_) => return true,
        };
        if self.model().kind_of(ino) != Some(Kind::File) {
            return false;
        }
        let at = match self.with(|m| m.current(ino)) {
            Ok(Opened::Overlay) => match self.model().version_of(ino) {
                Some(v) => SettingsAt::Written(ino, v),
                None => return true,
            },
            Ok(Opened::Value { id, size }) => {
                let at = SettingsAt::Ok(id.clone());
                if let Some((known, keeps)) = self.settings.lock().unwrap().as_ref()
                    && *known == at
                {
                    return *keeps;
                }
                let keeps = match self.fetch.read(&id, size, 0, size.min(MAX_SETTINGS)) {
                    Ok(data) => keeps_secrets(&data),
                    Err(_) => return true,
                };
                *self.settings.lock().unwrap() = Some((at, keeps));
                return keeps;
            }
            Err(_) => return true,
        };
        if let Some((known, keeps)) = self.settings.lock().unwrap().as_ref()
            && *known == at
        {
            return *keeps;
        }
        let keeps = match self.with(|m| m.read_overlay(ino, 0, MAX_SETTINGS as u32)) {
            Ok(data) => keeps_secrets(&data),
            Err(_) => return true,
        };
        *self.settings.lock().unwrap() = Some((at, keeps));
        keeps
    }

    /// A secret's path, for an inode this mount serves as one: a regular file under `secret/`,
    /// named neither `acl.yaml` nor as an OWNERS file, in a workspace of a repository that keeps
    /// secrets.
    fn secret_path(&self, ino: Ino) -> Option<String> {
        self.secrets.as_ref()?;
        let path = {
            let m = self.model();
            if !m.is_secret(ino) {
                return None;
            }
            m.path_of(ino)?.as_internal_file_string().to_owned()
        };
        self.keeps().then_some(path)
    }

    /// The path a name in a directory has, as the repository names it.
    fn child_path(&self, parent: Ino, name: &OsStr) -> Option<String> {
        let dir = self.model().path_of(parent)?;
        let name = name.to_str()?;
        Some(if dir.is_root() {
            name.to_owned()
        } else {
            format!("{}/{name}", dir.as_internal_file_string())
        })
    }

    /// A name in a directory, as a secret's path: where a file made there is a secret.
    fn secret_child(&self, parent: Ino, name: &OsStr) -> Option<String> {
        self.secrets.as_ref()?;
        self.child_path(parent, name)
            .filter(|p| ok_secret::is_secret(p))
            .filter(|_| self.keeps())
    }

    /// Whether a name in a directory is in the `secret/` tree, of a workspace of a repository that
    /// keeps secrets.
    fn touches_secrets(&self, parent: Ino, name: &OsStr) -> bool {
        self.secrets.is_some()
            && self
                .child_path(parent, name)
                .is_some_and(|p| ok_secret::in_tree(&p))
            && self.keeps()
    }

    /// Refuses anything but a file or a folder in the `secret/` tree: a symlink, FIFO, or socket
    /// there is no secret.
    fn only_files_in_secrets(&self, parent: Ino, name: &OsStr) -> fuse::Result<()> {
        self.utf8_in_secrets(parent, name)?;
        if self.touches_secrets(parent, name) {
            return Err(libc::EPERM);
        }
        Ok(())
    }

    /// Refuses a name in the `secret/` tree that no repository path can hold (one not UTF-8): the
    /// mount could not tell whether a file of that name is a secret, to keep its bytes off disk.
    fn utf8_in_secrets(&self, parent: Ino, name: &OsStr) -> fuse::Result<()> {
        if self.secrets.is_some()
            && name.to_str().is_none()
            && self
                .model()
                .path_of(parent)
                .is_some_and(|p| ok_secret::in_tree(p.as_internal_file_string()))
            && self.keeps()
        {
            return Err(libc::EINVAL);
        }
        Ok(())
    }

    /// Shows a secret as its value: its size — what the mount holds for writing, else what an open
    /// handle read, else 0 — and `0600`. Returns how long the kernel may keep the attributes: not
    /// at all for a secret, whose size changes without the kernel seeing a write.
    fn secret_attr(&self, attr: &mut fuse::Attr) -> Duration {
        if attr.mode & libc::S_IFMT != libc::S_IFREG || self.secret_path(attr.ino).is_none() {
            return TTL;
        }
        let held = self
            .secret_files
            .lock()
            .unwrap()
            .get(&attr.ino)
            .map(|f| f.value.len() as u64);
        attr.size = held.unwrap_or_else(|| {
            self.handles
                .lock()
                .unwrap()
                .values()
                .find_map(|h| match h {
                    Handle::Secret {
                        ino,
                        value: Some(v),
                    } if *ino == attr.ino => Some(v.len() as u64),
                    _ => None,
                })
                .unwrap_or(0)
        });
        attr.mode = (attr.mode & libc::S_IFMT) | 0o600;
        Duration::ZERO
    }

    /// Refuses a write to a folder's secrets the caller may not make (`EACCES`), as ok secrets
    /// says.
    fn check_write(&self, path: &str) -> fuse::Result<()> {
        let folder = path.rsplit_once('/').map_or(path, |(f, _)| f).to_owned();
        if let Some((ok, at)) = self.secret_access.lock().unwrap().get(&folder)
            && at.elapsed() < SECRET_ACCESS_TTL
        {
            return if *ok { Ok(()) } else { Err(libc::EACCES) };
        }
        let store = self.secrets.as_ref().ok_or(libc::EPERM)?;
        let ok = store.may_write(path).map_err(to_errno)?;
        self.secret_access
            .lock()
            .unwrap()
            .insert(folder, (ok, Instant::now()));
        if ok { Ok(()) } else { Err(libc::EACCES) }
    }

    /// A secret's value as the file holds it: what its pointer names, read from ok secrets with one
    /// of the person's unseals, with the pointer and the value's hash; or, for a file holding no
    /// pointer — one made and not yet written, or a conflict's markers — its bytes as they are.
    fn fetch_secret(&self, ino: Ino, path: &str) -> fuse::Result<FetchedSecret> {
        let held = match self.with(|m| m.current(ino))? {
            Opened::Overlay => self.with(|m| m.read_overlay(ino, 0, u32::MAX))?,
            Opened::Value { id, size } => self.fetch.read(&id, size, 0, size).map_err(to_errno)?,
        };
        match ok_secret::parse_pointer(&held) {
            Some(d) => {
                let store = self.secrets.as_ref().ok_or(libc::EPERM)?;
                let value = store.read(path, &d).map_err(to_errno)?;
                let hash = ok_secret::value_hash(value.as_slice());
                Ok((value, Some((d, hash))))
            }
            None => Ok((SecretBuf::new(held), None)),
        }
    }

    /// Holds a secret's value for a handle open to write it that keeps its contents: what the
    /// mount holds already, or what the file holds, fetched.
    fn hold_secret(&self, ino: Ino, path: &str) -> fuse::Result<()> {
        if let Some(f) = self.secret_files.lock().unwrap().get_mut(&ino) {
            f.opens += 1;
            return Ok(());
        }
        let (value, sealed) = self.fetch_secret(ino, path)?;
        let mut files = self.secret_files.lock().unwrap();
        match files.get_mut(&ino) {
            // Held meanwhile — written, or opened by another: that wins.
            Some(f) => f.opens += 1,
            None => {
                files.insert(
                    ino,
                    SecretFile {
                        value,
                        sealed,
                        dirty: false,
                        generation: 0,
                        opens: 1,
                    },
                );
            }
        }
        Ok(())
    }

    /// Opens a secret. To write, it first asks ok secrets whether the caller may, and the value is
    /// held in the mount's memory until a snapshot seals it; to read, the open fetches the value —
    /// using an unseal, unless the mount holds a value written here — for itself alone. Reads and
    /// writes go to the mount's memory, never the kernel's page cache (`FOPEN_DIRECT_IO`).
    fn open_secret(&self, ino: Ino, path: &str, flags: i32) -> fuse::Result<fuse::Open> {
        let writing = flags & libc::O_ACCMODE != libc::O_RDONLY || flags & libc::O_TRUNC != 0;
        let handle = if writing {
            self.writable()?;
            self.check_write(path)?;
            if flags & libc::O_TRUNC != 0 {
                self.with(|m| m.open(ino, flags & !libc::O_TRUNC))?;
                self.write_secret(ino, |v| v.set_len(0))?;
                if let Some(f) = self.secret_files.lock().unwrap().get_mut(&ino) {
                    f.opens += 1;
                }
            } else {
                self.hold_secret(ino, path)?;
                self.with(|m| m.open(ino, flags))?;
            }
            Handle::Secret { ino, value: None }
        } else {
            let held = self
                .secret_files
                .lock()
                .unwrap()
                .get(&ino)
                .map(|f| f.value.clone());
            let value = match held {
                Some(v) => v,
                None => self.fetch_secret(ino, path)?.0,
            };
            self.with(|m| m.open(ino, flags))?;
            Handle::Secret {
                ino,
                value: Some(value),
            }
        };
        let fh = self.new_fh(handle);
        Ok(fuse::Open {
            fh,
            flags: fuse::abi::FOPEN_DIRECT_IO,
        })
    }

    /// Reads a secret through a handle: its own value, opened to read; the mount's, opened to
    /// write — fetched again if a checkout dropped it.
    fn read_secret(
        &self,
        ino: Ino,
        fh: u64,
        path: &str,
        offset: u64,
        size: u32,
    ) -> fuse::Result<Vec<u8>> {
        let slice = |v: &[u8]| {
            let start = (offset as usize).min(v.len());
            let end = start.saturating_add(size as usize).min(v.len());
            v[start..end].to_vec()
        };
        if let Some(Handle::Secret { value: Some(v), .. }) = self.handles.lock().unwrap().get(&fh) {
            return Ok(slice(v.as_slice()));
        }
        if !self.secret_files.lock().unwrap().contains_key(&ino) {
            self.hold_secret(ino, path)?;
        }
        let files = self.secret_files.lock().unwrap();
        Ok(files
            .get(&ino)
            .map(|f| slice(f.value.as_slice()))
            .unwrap_or_default())
    }

    /// Changes a secret's value in memory, and records the change in the model (whose overlay keeps
    /// the pointer it had), so a snapshot or checkpoint seals it.
    fn write_secret(&self, ino: Ino, f: impl FnOnce(&mut SecretBuf)) -> fuse::Result<()> {
        {
            let mut files = self.secret_files.lock().unwrap();
            let file = files.entry(ino).or_insert_with(|| SecretFile {
                value: SecretBuf::default(),
                sealed: None,
                dirty: false,
                generation: 0,
                opens: 0,
            });
            f(&mut file.value);
            if file.value.len() > ok_secret::MAX_VALUE {
                return Err(libc::EFBIG);
            }
            file.dirty = true;
            file.generation += 1;
        }
        self.with(|m| m.secret_written(ino))?;
        self.changed();
        Ok(())
    }

    /// Lets go of a secret's handle: a read's value is dropped with it; a write's, once no handle
    /// holds it and it is sealed.
    fn release_secret(&self, h: Handle) {
        if let Handle::Secret { ino, value: None } = h {
            let mut files = self.secret_files.lock().unwrap();
            if let Some(f) = files.get_mut(&ino) {
                f.opens = f.opens.saturating_sub(1);
                if f.opens == 0 && !f.dirty {
                    files.remove(&ino);
                }
            }
        }
    }

    /// A directory as a snapshot reads it (ListDir): the model's listing, with each secret that is
    /// written and not sealed marked pending, and each whose value is gone — written by a daemon
    /// that stopped before sealing it — marked lost.
    pub fn list_dir(&self, path: &jj_lib::repo_path::RepoPath) -> fuse::Result<pb::ListDirReply> {
        let mut reply = self.with(|m| m.list_dir(path))?;
        let dir = path.as_internal_file_string();
        if self.secrets.is_none() || !ok_secret::in_tree(dir) || !self.keeps() {
            return Ok(reply);
        }
        let files = self.secret_files.lock().unwrap();
        for e in &mut reply.entries {
            let Ok(name) = std::str::from_utf8(&e.name) else {
                continue;
            };
            if e.kind() != pb::dir_entry::Kind::File
                || e.overlay_path.is_empty()
                || !ok_secret::is_secret(&format!("{dir}/{name}"))
            {
                continue;
            }
            if files.get(&e.ino).is_some_and(|f| f.dirty) {
                e.secret_pending = true;
                continue;
            }
            let pointer = e.size == ok_secret::POINTER_LEN as u64
                && std::fs::read(&e.overlay_path)
                    .is_ok_and(|b| ok_secret::parse_pointer(&b).is_some());
            if !pointer && !e.checkout_conflict {
                e.secret_lost = true;
            }
        }
        Ok(reply)
    }

    /// Seals a written secret for a snapshot that tracks it: its value is kept in ok secrets as a
    /// new version — unless it is the value its pointer already names, or is itself a pointer,
    /// which the file then holds (how a conflict over a secret is resolved) — and its pointer put
    /// in the overlay; the value is dropped unless a handle open to write still holds it. Returns
    /// the file's entry as a snapshot then reads it; None when it is not to be recorded: its name
    /// is no secret's, or it is gone.
    pub fn seal_secret(
        &self,
        path: &jj_lib::repo_path::RepoPath,
    ) -> io::Result<Option<pb::DirEntry>> {
        let p = path.as_internal_file_string();
        let Some(store) = self.secrets.clone() else {
            return Ok(None);
        };
        if !ok_secret::valid_path(p) || !self.keeps() {
            return Ok(None);
        }
        let errno = io::Error::from_raw_os_error;
        let _sealing = self.sealing.lock().unwrap();
        let Some(ino) = self.with(|m| m.resolve(path)).map_err(errno)? else {
            return Ok(None);
        };
        let pending = {
            let files = self.secret_files.lock().unwrap();
            files.get(&ino).filter(|f| f.dirty).map(|f| {
                let hash = ok_secret::value_hash(f.value.as_slice());
                let unchanged = f.sealed.filter(|(_, h)| *h == hash).map(|(d, _)| d);
                (f.value.clone(), hash, f.generation, unchanged)
            })
        };
        if let Some((value, hash, generation, unchanged)) = pending {
            let digest = match (unchanged, ok_secret::parse_pointer(value.as_slice())) {
                (Some(d), _) | (None, Some(d)) => d,
                (None, None) => store.write(p, value.as_slice())?,
            };
            self.with(|m| m.put_contents(ino, &ok_secret::pointer(&digest)))
                .map_err(errno)?;
            let mut files = self.secret_files.lock().unwrap();
            if let Some(f) = files.get_mut(&ino)
                && f.generation == generation
            {
                f.dirty = false;
                f.sealed = Some((digest, hash));
                if f.opens == 0 {
                    files.remove(&ino);
                }
            }
        }
        let (Some(dir), Some(name)) = (path.parent(), path.components().next_back()) else {
            return Ok(None);
        };
        let listing = self.list_dir(dir).map_err(errno)?;
        Ok(listing
            .entries
            .into_iter()
            .find(|e| e.name == name.as_internal_str().as_bytes())
            .map(|mut e| {
                e.secret_pending = false;
                e
            }))
    }

    /// Drops the values the mount holds that were not written: after a checkout, a secret's
    /// pointer may name another value, which the next read fetches. What was written and not
    /// sealed stays, as does what a handle open to write holds.
    pub fn forget_secrets(&self) {
        self.secret_files
            .lock()
            .unwrap()
            .retain(|_, f| f.dirty || f.opens > 0);
    }
}

/// A secret's value as a file holds it, and the pointer and value hash it was read by.
type FetchedSecret = (SecretBuf, Option<(Digest, [u8; 32])>);

/// Whether the repository's settings have a secrets stanza: a top-level `secrets:` key.
fn keeps_secrets(data: &[u8]) -> bool {
    data.split(|b| *b == b'\n')
        .any(|l| l.starts_with(b"secrets:"))
}
