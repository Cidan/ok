//! okfs's FUSE layer (docs/okfs.md, "Decisions" 1): the kernel protocol over `/dev/fuse`,
//! mounting (directly where the process may, through `fusermount3` otherwise), and a session that
//! serves a [`Filesystem`] from one thread per cloned device descriptor.
//!
//! The crate knows nothing of jj or ok: it is the transport. Replies are built here from what a
//! [`Filesystem`] returns; notifications (cache invalidation, resending lost requests) go through a
//! [`Notifier`].

pub mod abi;
#[cfg(target_os = "linux")]
mod mount;
#[cfg(not(target_os = "linux"))]
#[path = "unsupported.rs"]
mod mount;
mod session;
#[cfg(target_os = "linux")]
pub mod testns;

use std::ffi::OsStr;
use std::ffi::OsString;
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt as _;
use std::sync::Arc;
use std::time::Duration;

pub use mount::can_mount;
pub use mount::mount;
pub use mount::unmount;
pub use session::Negotiated;
pub use session::Session;

/// How a file system is mounted.
#[derive(Clone, Debug)]
pub struct MountOptions {
    /// The mount's source, as /proc/mounts shows it.
    pub fsname: String,
    /// The file system's type after `fuse.`.
    pub subtype: String,
    /// Let the kernel check permissions from the mode bits, as on a local file system.
    pub default_permissions: bool,
    /// Mount read-only: the kernel refuses every change before asking the file system.
    pub read_only: bool,
}

impl Default for MountOptions {
    fn default() -> Self {
        Self {
            fsname: "okfs".to_owned(),
            subtype: "okfs".to_owned(),
            default_permissions: true,
            read_only: false,
        }
    }
}

/// A FUSE error: an errno.
pub type Errno = i32;
pub type Result<T> = std::result::Result<T, Errno>;

/// Seconds and nanoseconds since the epoch.
pub type Time = (i64, u32);

/// An inode's attributes, as the kernel caches them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Attr {
    pub ino: u64,
    pub size: u64,
    pub atime: Time,
    pub mtime: Time,
    pub ctime: Time,
    /// The file type bits and permissions (`st_mode`).
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
}

/// A name's inode, as LOOKUP and the creating operations return it.
#[derive(Clone, Copy, Debug)]
pub struct Entry {
    pub attr: Attr,
    pub generation: u64,
    pub entry_ttl: Duration,
    pub attr_ttl: Duration,
}

/// Who made a request.
#[derive(Clone, Copy, Debug)]
pub struct Ctx {
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

/// What a SETATTR changes.
#[derive(Clone, Copy, Debug, Default)]
pub struct SetAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<TimeOrNow>,
    pub mtime: Option<TimeOrNow>,
    pub ctime: Option<Time>,
    pub fh: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub enum TimeOrNow {
    Time(Time),
    Now,
}

/// OPEN's, OPENDIR's, and CREATE's handle and its flags (`FOPEN_*`).
#[derive(Clone, Copy, Debug, Default)]
pub struct Open {
    pub fh: u64,
    pub flags: u32,
}

/// A directory entry READDIR returns. `offset` is the cookie of the entry after it.
#[derive(Clone, Debug)]
pub struct DirEntry {
    pub ino: u64,
    pub offset: u64,
    /// The file type, as `st_mode`'s type bits (`S_IFREG`, ...).
    pub kind: u32,
    pub name: OsString,
}

/// A READDIRPLUS entry: the entry and, unless it is `.` or `..`, its lookup.
#[derive(Clone, Debug)]
pub struct DirEntryPlus {
    pub entry: DirEntry,
    /// None for `.` and `..`, which the kernel does not look up.
    pub lookup: Option<Entry>,
}

/// A file system's statistics.
#[derive(Clone, Copy, Debug, Default)]
pub struct Statfs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
}

/// Where READDIRPLUS's buffer is full: a file system stops adding entries when `push`
/// returns false, and the entry it was offered was not taken (and, for READDIRPLUS, not looked
/// up: the file system must not count it).
pub struct DirBuf<'a> {
    w: &'a mut abi::Writer,
    max: usize,
    plus: bool,
}

impl DirBuf<'_> {
    /// Adds a READDIR entry; false when it does not fit.
    pub fn push(&mut self, e: &DirEntry) -> bool {
        debug_assert!(!self.plus);
        let need = (abi::DIRENT_LEN + e.name.len()).next_multiple_of(8);
        if self.w.buf.len() - abi::OUT_HEADER_LEN + need > self.max {
            return false;
        }
        write_dirent(self.w, e);
        true
    }

    /// Whether a READDIRPLUS entry for `name` would fit: a file system asks before it looks
    /// the entry up, so it counts only the lookups it returns.
    pub fn fits_plus(&self, name: &OsStr) -> bool {
        let need = (abi::ENTRY_OUT_LEN + abi::DIRENT_LEN + name.len()).next_multiple_of(8);
        self.w.buf.len() - abi::OUT_HEADER_LEN + need <= self.max
    }

    /// Adds a READDIRPLUS entry; false when it does not fit.
    pub fn push_plus(&mut self, e: &DirEntryPlus) -> bool {
        debug_assert!(self.plus);
        if !self.fits_plus(&e.entry.name) {
            return false;
        }
        match &e.lookup {
            Some(entry) => abi::write_entry(self.w, entry),
            None => {
                self.w.zeros(abi::ENTRY_OUT_LEN);
            }
        }
        write_dirent(self.w, &e.entry);
        true
    }

    pub fn is_plus(&self) -> bool {
        self.plus
    }
}

fn write_dirent(w: &mut abi::Writer, e: &DirEntry) {
    w.u64(e.ino)
        .u64(e.offset)
        .u32(e.name.len() as u32)
        .u32((e.kind & abi::S_IFMT) >> 12)
        .bytes(e.name.as_bytes())
        .align8();
}

/// A file system the session serves. Every method has a default that refuses with `ENOSYS`.
/// Methods are called concurrently from the session's threads.
#[allow(unused_variables)]
pub trait Filesystem: Send + Sync + 'static {
    /// Whether the file system keeps POSIX ACLs (`system.posix_acl_access` and
    /// `system.posix_acl_default`) as a disk file system does: an access ACL setting the mode and
    /// kept in step with it, and a directory's default ACL inherited by what is made in it, with
    /// the umask applied only where there is none. INIT then has the kernel enforce ACLs
    /// (`FUSE_POSIX_ACL`) and leave the umask to the file system (`FUSE_DONT_MASK`); otherwise the
    /// kernel applies the umask and ignores ACLs.
    fn posix_acl(&self) -> bool {
        false
    }
    /// Called once the kernel's INIT is answered (not again for a session adopted after a
    /// restart), with the notifier for the connection.
    fn init(&self, notifier: Notifier) {}
    fn destroy(&self) {}
    fn lookup(&self, ctx: &Ctx, parent: u64, name: &OsStr) -> Result<Option<Entry>> {
        Err(libc::ENOSYS)
    }
    fn forget(&self, ino: u64, nlookup: u64) {}
    fn getattr(&self, ctx: &Ctx, ino: u64, fh: Option<u64>) -> Result<(Attr, Duration)> {
        Err(libc::ENOSYS)
    }
    fn setattr(&self, ctx: &Ctx, ino: u64, set: &SetAttr) -> Result<(Attr, Duration)> {
        Err(libc::ENOSYS)
    }
    fn readlink(&self, ctx: &Ctx, ino: u64) -> Result<Vec<u8>> {
        Err(libc::ENOSYS)
    }
    fn symlink(&self, ctx: &Ctx, parent: u64, name: &OsStr, target: &[u8]) -> Result<Entry> {
        Err(libc::ENOSYS)
    }
    fn mknod(
        &self,
        ctx: &Ctx,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
    ) -> Result<Entry> {
        Err(libc::ENOSYS)
    }
    fn mkdir(&self, ctx: &Ctx, parent: u64, name: &OsStr, mode: u32, umask: u32) -> Result<Entry> {
        Err(libc::ENOSYS)
    }
    fn unlink(&self, ctx: &Ctx, parent: u64, name: &OsStr) -> Result<()> {
        Err(libc::ENOSYS)
    }
    fn rmdir(&self, ctx: &Ctx, parent: u64, name: &OsStr) -> Result<()> {
        Err(libc::ENOSYS)
    }
    /// `flags` are renameat2's (`RENAME_NOREPLACE`, `RENAME_EXCHANGE`, ...).
    fn rename(
        &self,
        ctx: &Ctx,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
    ) -> Result<()> {
        Err(libc::ENOSYS)
    }
    fn link(&self, ctx: &Ctx, ino: u64, newparent: u64, newname: &OsStr) -> Result<Entry> {
        Err(libc::ENOSYS)
    }
    fn open(&self, ctx: &Ctx, ino: u64, flags: i32) -> Result<Open> {
        Err(libc::ENOSYS)
    }
    fn create(
        &self,
        ctx: &Ctx,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<(Entry, Open)> {
        Err(libc::ENOSYS)
    }
    fn read(&self, ctx: &Ctx, ino: u64, fh: u64, offset: u64, size: u32) -> Result<Vec<u8>> {
        Err(libc::ENOSYS)
    }
    fn write(
        &self,
        ctx: &Ctx,
        ino: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        flags: u32,
    ) -> Result<u32> {
        Err(libc::ENOSYS)
    }
    fn flush(&self, ctx: &Ctx, ino: u64, fh: u64) -> Result<()> {
        Ok(())
    }
    fn release(&self, ctx: &Ctx, ino: u64, fh: u64, flags: i32) {}
    fn fsync(&self, ctx: &Ctx, ino: u64, fh: u64, datasync: bool) -> Result<()> {
        Ok(())
    }
    fn opendir(&self, ctx: &Ctx, ino: u64, flags: i32) -> Result<Open> {
        Ok(Open::default())
    }
    /// Fills `buf` with entries after the cookie `offset` (0: from the start): plain entries for
    /// READDIR, entries with their lookups for READDIRPLUS (`buf.is_plus()`).
    fn readdir(
        &self,
        ctx: &Ctx,
        ino: u64,
        fh: u64,
        offset: u64,
        buf: &mut DirBuf<'_>,
    ) -> Result<()> {
        Err(libc::ENOSYS)
    }
    fn releasedir(&self, ctx: &Ctx, ino: u64, fh: u64) {}
    fn fsyncdir(&self, ctx: &Ctx, ino: u64, fh: u64, datasync: bool) -> Result<()> {
        Ok(())
    }
    fn statfs(&self, ctx: &Ctx, ino: u64) -> Result<Statfs> {
        Ok(Statfs {
            bsize: 4096,
            namelen: 255,
            ..Statfs::default()
        })
    }
    /// `kill_sgid`: setting `system.posix_acl_access` clears the inode's setgid bit, the caller
    /// being neither in its group nor privileged (`FUSE_SETXATTR_ACL_KILL_SGID`).
    fn setxattr(
        &self,
        ctx: &Ctx,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        kill_sgid: bool,
    ) -> Result<()> {
        Err(libc::ENOTSUP)
    }
    fn getxattr(&self, ctx: &Ctx, ino: u64, name: &OsStr) -> Result<Vec<u8>> {
        Err(libc::ENODATA)
    }
    /// The NUL-terminated names, concatenated.
    fn listxattr(&self, ctx: &Ctx, ino: u64) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn removexattr(&self, ctx: &Ctx, ino: u64, name: &OsStr) -> Result<()> {
        Err(libc::ENODATA)
    }
    fn fallocate(
        &self,
        ctx: &Ctx,
        ino: u64,
        fh: u64,
        offset: u64,
        length: u64,
        mode: i32,
    ) -> Result<()> {
        Err(libc::EOPNOTSUPP)
    }
    fn lseek(&self, ctx: &Ctx, ino: u64, fh: u64, offset: i64, whence: u32) -> Result<i64> {
        Err(libc::ENOSYS)
    }
}

/// Sends notifications on a connection: cache invalidations, and asking the kernel to resend
/// requests a previous server read and never answered.
#[derive(Clone)]
pub struct Notifier {
    fd: Arc<OwnedFd>,
    resend: bool,
    minor: u32,
}

impl Notifier {
    fn send(&self, code: i32, payload: &[&[u8]]) -> io::Result<()> {
        let len = abi::OUT_HEADER_LEN + payload.iter().map(|p| p.len()).sum::<usize>();
        let mut w = abi::Writer::with_capacity(len);
        w.u32(len as u32).i32(code).u64(0);
        for p in payload {
            w.bytes(p);
        }
        let n = unsafe { libc::write(self.fd.as_raw_fd(), w.buf.as_ptr().cast(), w.buf.len()) };
        if n < 0 {
            let err = io::Error::last_os_error();
            // The kernel no longer knows the inode or entry: nothing to invalidate.
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    /// Drops the kernel's cached pages of an inode in [offset, offset+len) (len 0: to the end;
    /// offset -1: only its attributes).
    pub fn inval_inode(&self, ino: u64, offset: i64, len: i64) -> io::Result<()> {
        let mut w = abi::Writer::with_capacity(24);
        w.u64(ino).i64(offset).i64(len);
        self.send(abi::NOTIFY_INVAL_INODE, &[&w.buf])
    }

    /// Drops the kernel's cached entry `name` in `parent`, positive or negative.
    pub fn inval_entry(&self, parent: u64, name: &OsStr) -> io::Result<()> {
        let mut w = abi::Writer::with_capacity(16);
        w.u64(parent).u32(name.len() as u32).u32(0);
        self.send(abi::NOTIFY_INVAL_ENTRY, &[&w.buf, name.as_bytes(), &[0]])
    }

    /// Tells the kernel `child` is gone from `parent` under `name`, as if deleted there.
    pub fn delete(&self, parent: u64, child: u64, name: &OsStr) -> io::Result<()> {
        let mut w = abi::Writer::with_capacity(24);
        w.u64(parent).u64(child).u32(name.len() as u32).u32(0);
        self.send(abi::NOTIFY_DELETE, &[&w.buf, name.as_bytes(), &[0]])
    }

    /// Whether the kernel resends requests on [`Notifier::resend`] (`FUSE_HAS_RESEND`).
    pub fn can_resend(&self) -> bool {
        self.resend
    }

    /// Invalidates every entry the kernel has cached for the connection (`FUSE_NOTIFY_INC_EPOCH`,
    /// protocol 7.44): each is looked up again before its next use. False when the kernel is too
    /// old to be asked.
    pub fn inc_epoch(&self) -> io::Result<bool> {
        if self.minor < 44 {
            return Ok(false);
        }
        self.send(abi::NOTIFY_INC_EPOCH, &[])?;
        Ok(true)
    }

    /// Asks the kernel to queue again every request a server read and did not answer: what a
    /// restarted server does for the requests its predecessor died holding.
    pub fn resend(&self) -> io::Result<()> {
        if !self.resend {
            return Err(io::Error::from_raw_os_error(libc::ENOSYS));
        }
        self.send(abi::NOTIFY_RESEND, &[])
    }
}
