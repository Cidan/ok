//! A mounted workspace over NFS (docs/okfs.md, "macOS"): okfs-nfs's procedures, served from the
//! model as fs.rs serves FUSE's. NFS differs from FUSE in four ways this answers:
//!
//! - **No open or close.** A read reads the file as it stands; the overlay files writes and reads
//!   open stay open while recently used, the rest closed (`OPEN_FILES`).
//! - **No forget.** The kernel may use any handle it was given at any time, across daemon restarts,
//!   so every inode it was given is kept — until it is removed, or for `DETACHED_GRACE` after a
//!   checkout leaves no name reaching it, for a process that held it open.
//! - **Listings by cookie.** An entry's cookie is its place in the listing, `.` and `..` first.
//! - **No notifications.** A change the kernel did not make — a checkout — is pushed to it by
//!   [`Refresh`]: the times of the directories and files it changed are moved on, and each is asked
//!   for through the mount with its own mode (a SETATTR that changes nothing here), whose reply
//!   carries the new times, so the kernel drops what it cached of them and looks again. The jj
//!   command that asked for the change asks the kernel ([`ask`]): macOS keeps network volumes, a
//!   mount of 127.0.0.1 among them, from background processes like the daemon.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use okfs_fuse as fuse;
use okfs_nfs as nfs;

use crate::fs::Mount;
use crate::model::Ino;
use crate::model::Kind;
use crate::model::Opened;
use crate::model::ROOT;
use crate::model::Stale;
use crate::model::Time;

/// Overlay files kept open for writes and reads, the most recently used.
const OPEN_FILES: usize = 128;
/// How long an inode no name reaches is kept for a handle the kernel holds to it.
pub const DETACHED_GRACE: Duration = Duration::from_secs(600);
/// The most files a refresh asks the kernel about one by one; beyond, their directories' refresh
/// reaches them when they are next looked up.
const REFRESH_FILES: usize = 4096;

/// What the kernel is asked about after a change it did not make: a path, and the mode it has.
pub type Target = (PathBuf, u32);
/// Threads a refresh asks the kernel with.
const REFRESH_THREADS: usize = 8;
/// Directories whose listings are kept between the calls that page through them.
const LISTINGS: usize = 64;

/// What the kernel's NFS client has been given, and how it is told of changes it did not make.
pub struct Refresh {
    mount_point: PathBuf,
    /// Times reported over the model's: a directory's or file's after a change the kernel did not
    /// make, which tells the kernel its cache of it is stale.
    bumps: Mutex<HashMap<Ino, Time>>,
    /// Every inode the kernel has a handle to.
    handed: Mutex<HashSet<Ino>>,
    /// Logs each refresh to standard error (`OKFS_NFS_TRACE`).
    trace: bool,
}

impl Refresh {
    pub fn new(mount_point: PathBuf) -> Self {
        Self {
            mount_point,
            bumps: Mutex::new(HashMap::new()),
            handed: Mutex::new(HashSet::from([ROOT])),
            trace: std::env::var_os("OKFS_NFS_TRACE").is_some(),
        }
    }

    /// Counts every inode the model holds as handed to the kernel: after a restart, the kernel may
    /// hold a handle to any of them.
    pub fn seed(&self, inos: impl IntoIterator<Item = Ino>) {
        self.handed.lock().unwrap().extend(inos);
    }

    /// An inode's attributes with its moved times.
    fn apply(&self, a: &mut fuse::Attr) {
        if let Some(b) = self.bumps.lock().unwrap().get(&a.ino) {
            a.mtime = a.mtime.max(*b);
            a.ctime = a.ctime.max(*b);
        }
    }

    /// Moves an inode's times on: now, or just after the last time they moved.
    fn bump(&self, ino: Ino) {
        let now = crate::model::now();
        let mut bumps = self.bumps.lock().unwrap();
        let t = match bumps.get(&ino) {
            Some(&(s, n)) if (s, n) >= now => {
                if n >= 999_999_999 {
                    (s + 1, 0)
                } else {
                    (s, n + 1)
                }
            }
            _ => now,
        };
        bumps.insert(ino, t);
    }

    /// Moves on the times of what `stale` names that the kernel has — each changed directory, and
    /// each changed file up to `REFRESH_FILES` — and returns what to [`ask`] it about.
    pub fn refresh(&self, mount: &Mount, stale: &[Stale]) -> Vec<Target> {
        let mut dirs = BTreeSet::new();
        let mut files = BTreeSet::new();
        {
            let m = mount.model();
            let handed = self.handed.lock().unwrap();
            for s in stale {
                match s {
                    Stale::Entry(parent, _) => {
                        if handed.contains(parent) {
                            dirs.insert(*parent);
                        }
                    }
                    Stale::Inode(ino) => {
                        if handed.contains(ino) {
                            files.insert(*ino);
                            if let Some(p) = m.parent_of(*ino)
                                && handed.contains(&p)
                            {
                                dirs.insert(p);
                            }
                        }
                    }
                }
            }
        }
        if self.trace {
            eprintln!(
                "okfs-nfs: refresh of {} stale: {} directories, {} files",
                stale.len(),
                dirs.len(),
                files.len()
            );
        }
        let inos: Vec<Ino> = dirs
            .into_iter()
            .chain(files.into_iter().take(REFRESH_FILES))
            .collect();
        self.push(mount, &inos)
    }

    /// Moves on the times of every directory the kernel has — a change to everything — and returns
    /// what to [`ask`] it about.
    pub fn refresh_all(&self, mount: &Mount) -> Vec<Target> {
        let handed: Vec<Ino> = self.handed.lock().unwrap().iter().copied().collect();
        let dirs: Vec<Ino> = {
            let m = mount.model();
            handed.into_iter().filter(|i| m.is_dir(*i)).collect()
        };
        self.push(mount, &dirs)
    }

    /// Moves each inode's times on, and returns its path through the mount and its mode.
    fn push(&self, mount: &Mount, inos: &[Ino]) -> Vec<Target> {
        let mut targets: Vec<(Ino, PathBuf)> = Vec::new();
        {
            let m = mount.model();
            for ino in inos {
                let rel = if *ino == ROOT {
                    Some(String::new())
                } else {
                    m.path_of(*ino)
                        .map(|p| p.as_internal_file_string().to_owned())
                };
                if let Some(rel) = rel {
                    targets.push((*ino, self.mount_point.join(rel)));
                }
            }
        }
        targets
            .into_iter()
            .filter_map(|(ino, path)| {
                let attr = mount.with(|m| m.attr(ino)).ok()?;
                self.bump(ino);
                Some((path, attr.mode & 0o7777))
            })
            .collect()
    }
}

/// Asks the kernel about each target through the mount — a chmod to the mode it has, which changes
/// nothing — `REFRESH_THREADS` at a time; its reply carries the moved times, and the kernel drops
/// what it cached. A path gone since is fine: its directory's refresh tells the kernel so.
pub fn ask(targets: &[Target]) {
    let trace = std::env::var_os("OKFS_NFS_TRACE").is_some();
    let chunk = targets.len().div_ceil(REFRESH_THREADS).max(1);
    std::thread::scope(|scope| {
        for part in targets.chunks(chunk) {
            scope.spawn(move || {
                for (path, mode) in part {
                    let Ok(c) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
                        continue;
                    };
                    let r = unsafe { libc::chmod(c.as_ptr(), *mode as libc::mode_t) };
                    if trace {
                        let e = if r == 0 {
                            "ok".to_owned()
                        } else {
                            io::Error::last_os_error().to_string()
                        };
                        eprintln!("okfs-nfs: refresh {}: {e}", path.display());
                    }
                }
            });
        }
    });
}

/// A directory's entries as a listing serves them (`Mount::dir_entries`).
type Entries = Arc<Vec<(crate::model::Listed, Ino)>>;

/// A workspace served over NFS.
pub struct NfsMount {
    mount: Arc<Mount>,
    refresh: Arc<Refresh>,
    /// Overlay files open, least recently used first.
    open: Mutex<VecDeque<Ino>>,
    /// Inodes no name reaches, and since when.
    detached: Mutex<HashMap<Ino, Instant>>,
    /// Directories' listings, by the model's change count they were made at.
    listings: Mutex<HashMap<Ino, (u64, Entries)>>,
    /// How long an inode no name reaches is kept: `DETACHED_GRACE`, but in tests.
    grace: Duration,
}

fn time(t: Time) -> nfs::Time {
    nfs::Time {
        secs: t.0,
        nanos: t.1,
    }
}

fn kind_of(mode: u32) -> nfs::Kind {
    match mode & fuse::abi::S_IFMT {
        fuse::abi::S_IFDIR => nfs::Kind::Dir,
        fuse::abi::S_IFLNK => nfs::Kind::Symlink,
        fuse::abi::S_IFIFO => nfs::Kind::Fifo,
        fuse::abi::S_IFSOCK => nfs::Kind::Socket,
        _ => nfs::Kind::File,
    }
}

fn attr_out(a: &fuse::Attr) -> nfs::Attr {
    nfs::Attr {
        kind: kind_of(a.mode),
        perm: a.mode & 0o7777,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        size: a.size,
        used: a.size.div_ceil(4096) * 4096,
        rdev: (0, 0),
        fileid: a.ino,
        atime: time(a.atime),
        mtime: time(a.mtime),
        ctime: time(a.ctime),
    }
}

fn set_time(t: nfs::SetTime) -> fuse::TimeOrNow {
    match t {
        nfs::SetTime::Now => fuse::TimeOrNow::Now,
        nfs::SetTime::At(t) => fuse::TimeOrNow::Time((t.secs, t.nanos)),
    }
}

fn os(name: &[u8]) -> &std::ffi::OsStr {
    std::ffi::OsStr::from_bytes(name)
}

impl NfsMount {
    pub fn new(mount: Arc<Mount>, refresh: Arc<Refresh>) -> Self {
        mount.set_refresh(refresh.clone());
        Self {
            mount,
            refresh,
            open: Mutex::new(VecDeque::new()),
            detached: Mutex::new(HashMap::new()),
            listings: Mutex::new(HashMap::new()),
            grace: DETACHED_GRACE,
        }
    }

    /// Keeps an inode no name reaches for `grace` instead.
    pub fn with_grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    pub fn mount(&self) -> &Arc<Mount> {
        &self.mount
    }

    /// Records that the kernel now has a handle to `ino`, which is kept from now on.
    fn handed(&self, ino: Ino) {
        if self.refresh.handed.lock().unwrap().insert(ino) {
            self.mount.counted(ino);
        }
    }

    /// Lets an inode go: removed, or detached for the grace period.
    fn release(&self, ino: Ino) {
        self.refresh.handed.lock().unwrap().remove(&ino);
        self.open.lock().unwrap().retain(|i| *i != ino);
        self.refresh.bumps.lock().unwrap().remove(&ino);
        let mut m = self.mount.model();
        m.close_idle(ino);
        m.forget(ino, u64::MAX);
    }

    /// Notes an overlay file a write or read used, closing the least recently used beyond
    /// `OPEN_FILES`.
    fn used(&self, ino: Ino) {
        let evicted = {
            let mut open = self.open.lock().unwrap();
            if open.back() == Some(&ino) {
                return;
            }
            open.retain(|i| *i != ino);
            open.push_back(ino);
            if open.len() > OPEN_FILES {
                open.pop_front()
            } else {
                None
            }
        };
        if let Some(old) = evicted {
            self.mount.model().close_idle(old);
        }
    }

    /// Lets go the inodes a checkout left with no name for the grace period: the daemon calls this
    /// now and then.
    pub fn sweep(&self) {
        let detached: HashSet<Ino> = self.mount.model().detached().into_iter().collect();
        let now = Instant::now();
        let expired: Vec<Ino> = {
            let mut d = self.detached.lock().unwrap();
            d.retain(|ino, _| detached.contains(ino));
            for ino in &detached {
                d.entry(*ino).or_insert(now);
            }
            let expired: Vec<Ino> = d
                .iter()
                .filter(|(_, since)| now.duration_since(**since) >= self.grace)
                .map(|(ino, _)| *ino)
                .collect();
            for ino in &expired {
                d.remove(ino);
            }
            expired
        };
        for ino in expired {
            self.release(ino);
        }
    }

    fn attr(&self, ino: Ino) -> nfs::Result<nfs::Attr> {
        let mut a = self.mount.with(|m| m.attr(ino))?;
        self.refresh.apply(&mut a);
        Ok(attr_out(&a))
    }

    /// A directory's listing, kept while the model has not changed since.
    fn listing(&self, dir: Ino) -> nfs::Result<Entries> {
        let seq = self.mount.model().seq();
        if let Some((at, l)) = self.listings.lock().unwrap().get(&dir)
            && *at == seq
        {
            return Ok(l.clone());
        }
        let l = self.mount.dir_entries(dir)?;
        let mut listings = self.listings.lock().unwrap();
        if listings.len() >= LISTINGS {
            listings.clear();
        }
        listings.insert(dir, (seq, l.clone()));
        Ok(l)
    }
}

impl nfs::Filesystem for NfsMount {
    fn getattr(&self, _: &nfs::Ctx, ino: u64) -> nfs::Result<nfs::Attr> {
        self.attr(ino)
    }

    fn setattr(&self, _: &nfs::Ctx, ino: u64, set: &nfs::SetAttr) -> nfs::Result<()> {
        // A SETATTR that changes nothing — a refresh's, or a chmod to the mode a file has —
        // changes nothing: a directory stays one that follows the base, a file stays ok's.
        let cur = self.mount.with(|m| m.attr(ino))?;
        let unchanged = set.mode.is_none_or(|m| m & 0o7777 == cur.mode & 0o7777)
            && set.uid.is_none_or(|u| u == cur.uid)
            && set.gid.is_none_or(|g| g == cur.gid)
            && set.size.is_none_or(|s| s == cur.size)
            && set.atime.is_none()
            && set.mtime.is_none();
        if unchanged {
            return Ok(());
        }
        self.mount.writable()?;
        let fset = fuse::SetAttr {
            mode: set.mode,
            uid: set.uid,
            gid: set.gid,
            size: set.size,
            atime: set.atime.map(set_time),
            mtime: set.mtime.map(set_time),
            ctime: None,
            fh: None,
        };
        self.mount.with(|m| m.setattr(ino, &fset))?;
        self.mount.changed();
        Ok(())
    }

    fn lookup(&self, _: &nfs::Ctx, dir: u64, name: &[u8]) -> nfs::Result<u64> {
        match name {
            b"." => {
                self.mount.with(|m| m.attr(dir))?;
                Ok(dir)
            }
            b".." => self
                .mount
                .with(|m| m.attr(dir).map(|_| m.parent_of(dir).unwrap_or(ROOT))),
            _ => {
                let ino = self
                    .mount
                    .with(|m| m.lookup(dir, os(name), false))?
                    .ok_or(libc::ENOENT)?;
                self.handed(ino);
                Ok(ino)
            }
        }
    }

    fn readlink(&self, _: &nfs::Ctx, ino: u64) -> nfs::Result<Vec<u8>> {
        self.mount.with(|m| m.readlink(ino))
    }

    fn read(&self, _: &nfs::Ctx, ino: u64, offset: u64, count: u32) -> nfs::Result<Vec<u8>> {
        match self.mount.with(|m| m.current(ino))? {
            Opened::Overlay => {
                let data = self.mount.with(|m| m.read_overlay(ino, offset, count))?;
                self.used(ino);
                Ok(data)
            }
            Opened::Value { id, size } => {
                if offset == 0 {
                    // A file read from its start is opened, as the sibling prefetch counts opens.
                    self.mount.sibling_opened(ino);
                }
                if offset >= size {
                    return Ok(Vec::new());
                }
                let n = u64::from(count).min(size - offset);
                self.mount.read_value(&id, size, offset, n)
            }
        }
    }

    fn write(&self, _: &nfs::Ctx, ino: u64, offset: u64, data: &[u8]) -> nfs::Result<u32> {
        self.mount.writable()?;
        let n = self.mount.with(|m| m.write(ino, offset, data))?;
        self.mount.changed();
        self.used(ino);
        Ok(n)
    }

    fn create(
        &self,
        _: &nfs::Ctx,
        dir: u64,
        name: &[u8],
        kind: nfs::Kind,
        mode: u32,
        target: &[u8],
    ) -> nfs::Result<u64> {
        self.mount.writable()?;
        let kind = match kind {
            nfs::Kind::File => Kind::File,
            nfs::Kind::Dir => Kind::Dir,
            nfs::Kind::Symlink => Kind::Symlink,
            nfs::Kind::Fifo => Kind::Fifo,
            nfs::Kind::Socket => Kind::Socket,
            nfs::Kind::Block | nfs::Kind::Char => return Err(libc::EPERM),
        };
        let ino = self.mount.with(|m| match kind {
            Kind::Symlink => m.create(dir, os(name), kind, mode, target),
            // NFS's client has applied its umask to `mode`.
            _ => m.create_as(dir, os(name), kind, mode, 0),
        })?;
        self.mount.changed();
        self.handed(ino);
        Ok(ino)
    }

    fn remove(&self, _: &nfs::Ctx, dir: u64, name: &[u8]) -> nfs::Result<()> {
        self.mount.writable()?;
        let ino = self.mount.with(|m| m.unlink(dir, os(name)))?;
        self.mount.changed();
        self.release(ino);
        Ok(())
    }

    fn rmdir(&self, _: &nfs::Ctx, dir: u64, name: &[u8]) -> nfs::Result<()> {
        self.mount.writable()?;
        let ino = self.mount.with(|m| m.rmdir(dir, os(name)))?;
        self.mount.changed();
        self.release(ino);
        Ok(())
    }

    fn rename(
        &self,
        _: &nfs::Ctx,
        from: u64,
        name: &[u8],
        to: u64,
        to_name: &[u8],
    ) -> nfs::Result<()> {
        self.mount.writable()?;
        let replaced = self
            .mount
            .with(|m| m.rename(from, os(name), to, os(to_name), 0))?;
        self.mount.changed();
        if let Some(r) = replaced {
            self.release(r);
        }
        Ok(())
    }

    fn link(&self, _: &nfs::Ctx, ino: u64, dir: u64, name: &[u8]) -> nfs::Result<()> {
        self.mount.writable()?;
        self.mount.with(|m| m.link(ino, dir, os(name)))?;
        self.mount.changed();
        Ok(())
    }

    fn readdir(
        &self,
        _: &nfs::Ctx,
        dir: u64,
        cookie: u64,
        plus: bool,
        max: usize,
    ) -> nfs::Result<nfs::Listing> {
        let entries = self.listing(dir)?;
        if plus && (cookie as usize) < entries.len() {
            self.mount.prefetch_sizes(dir);
        }
        let mut out: Vec<nfs::Entry> = Vec::new();
        let partial = |out: Vec<nfs::Entry>| {
            Ok(nfs::Listing {
                entries: out,
                eof: false,
            })
        };
        for (i, (l, known)) in entries.iter().enumerate().skip(cookie as usize) {
            if out.len() == max {
                return partial(out);
            }
            let ino = if i < 2 {
                *known
            } else {
                match self.mount.with(|m| m.lookup(dir, &l.name, false)) {
                    Ok(Some(c)) => c,
                    // Gone since the listing was made.
                    Ok(None) => continue,
                    Err(e) if out.is_empty() => return Err(e),
                    Err(_) => return partial(out),
                }
            };
            if i >= 2 {
                self.handed(ino);
            }
            let attr = if plus {
                match self.attr(ino) {
                    Ok(a) => Some(a),
                    Err(e) if out.is_empty() => return Err(e),
                    Err(_) => return partial(out),
                }
            } else {
                None
            };
            out.push(nfs::Entry {
                name: l.name.as_bytes().to_vec(),
                fileid: ino,
                cookie: i as u64 + 1,
                attr,
            });
        }
        Ok(nfs::Listing {
            entries: out,
            eof: true,
        })
    }

    fn fsstat(&self, _: &nfs::Ctx, _: u64) -> nfs::Result<nfs::FsStat> {
        let st = self.mount.statvfs()?;
        let bsize = u64::from(st.bsize);
        Ok(nfs::FsStat {
            tbytes: st.blocks * bsize,
            fbytes: st.bfree * bsize,
            abytes: st.bavail * bsize,
            tfiles: st.files,
            ffiles: st.ffree,
            afiles: st.ffree,
        })
    }

    fn commit(&self, _: &nfs::Ctx, ino: u64) -> nfs::Result<()> {
        self.mount.with(|m| m.fsync(ino))
    }
}
