//! A mounted workspace (docs/okfs.md, "The file system"): the FUSE operations, served from the
//! model. The model's needs — trees, sizes, symlink targets, contents — are fetched outside its
//! lock and the operation tried again; a file whose content is ok's is read by range, outside
//! the lock too. Kernel notifications are sent once the lock is released.

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

use jj_lib::backend::FileId;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Tree as BackendTree;
use jj_lib::backend::TreeId;
use jj_lib::object_id::ObjectId as _;
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
    File { opened: Opened },
    Dir { entries: Arc<Vec<(Listed, Ino)>> },
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
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            staging,
            read_only: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            siblings: Mutex::new(HashMap::new()),
            stats: Stats::default(),
            on_change,
        })
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

    fn writable(&self) -> fuse::Result<()> {
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
    fn sibling_opened(&self, ino: Ino) {
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
    fn prefetch_sizes(&self, dir: Ino) {
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
        let attr = self.with(|m| m.attr(ino))?;
        Ok(fuse::Entry {
            attr,
            generation: 0,
            entry_ttl: TTL,
            attr_ttl: TTL,
        })
    }

    /// Counts a reference the kernel now holds.
    fn counted(&self, ino: Ino) {
        if !self.is_closed() {
            let _ = self.model().lookup_count(ino);
        }
    }

    fn changed(&self) {
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

    /// Sends the kernel the invalidations a change made.
    pub fn invalidate(&self, stale: &[Stale]) {
        let Some(n) = self.notifier.lock().unwrap().clone() else {
            return;
        };
        for s in stale {
            let _ = match s {
                Stale::Entry(parent, name) => n.inval_entry(*parent, name),
                Stale::Inode(ino) => n.inval_inode(*ino, 0, 0),
            };
        }
    }

    /// Drops every entry the kernel cached, where it can be asked to (a sparse change).
    pub fn invalidate_all(&self, dirs: &[(Ino, Vec<std::ffi::OsString>)]) {
        let Some(n) = self.notifier.lock().unwrap().clone() else {
            return;
        };
        if n.inc_epoch().unwrap_or(false) {
            return;
        }
        for (dir, names) in dirs {
            for name in names {
                let _ = n.inval_entry(*dir, name);
            }
        }
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
        Ok(found.map(|(ino, attr)| {
            self.counted(ino);
            fuse::Entry {
                attr,
                generation: 0,
                entry_ttl: TTL,
                attr_ttl: TTL,
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
        Ok((self.with(|m| m.attr(ino))?, TTL))
    }

    fn setattr(
        &self,
        _: &fuse::Ctx,
        ino: u64,
        set: &fuse::SetAttr,
    ) -> fuse::Result<(fuse::Attr, Duration)> {
        self.writable()?;
        self.with(|m| m.setattr(ino, set))?;
        self.changed();
        Ok((self.with(|m| m.attr(ino))?, TTL))
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
        let ino = self.with(|m| m.create(parent, name, Kind::Dir, mode & !umask, b""))?;
        self.changed();
        self.entry(ino)
    }

    fn unlink(&self, _: &fuse::Ctx, parent: u64, name: &OsStr) -> fuse::Result<()> {
        self.writable()?;
        self.with(|m| m.unlink(parent, name))?;
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
        self.with(|m| m.link(ino, newparent, newname))?;
        self.changed();
        self.entry(ino)
    }

    fn open(&self, _: &fuse::Ctx, ino: u64, flags: i32) -> fuse::Result<fuse::Open> {
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
        let n = self.with(|m| m.write(ino, offset, data))?;
        self.changed();
        Ok(n)
    }

    fn release(&self, _: &fuse::Ctx, ino: u64, fh: u64, _: i32) {
        self.handles.lock().unwrap().remove(&fh);
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

    fn statfs(&self, ctx: &fuse::Ctx, ino: u64) -> fuse::Result<fuse::Statfs> {
        self.statfs_impl(ctx, ino)
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
    fn dir_entries(&self, ino: Ino) -> fuse::Result<Arc<Vec<(Listed, Ino)>>> {
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
                let lookup = (!dot).then_some(fuse::Entry {
                    attr,
                    generation: 0,
                    entry_ttl: TTL,
                    attr_ttl: TTL,
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

    fn statfs_impl(&self, _: &fuse::Ctx, _: u64) -> fuse::Result<fuse::Statfs> {
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
