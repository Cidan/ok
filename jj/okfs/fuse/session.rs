//! A FUSE session: the INIT handshake, then one thread per device descriptor reading requests,
//! dispatching each to the [`Filesystem`], and writing its reply on the descriptor it came from.

use std::ffi::OsStr;
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::fd::FromRawFd as _;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt as _;
use std::sync::Arc;
use std::time::Duration;

use crate::Ctx;
use crate::DirBuf;
use crate::Filesystem;
use crate::Notifier;
use crate::SetAttr;
use crate::TimeOrNow;
use crate::abi;
use crate::abi::Reader;
use crate::abi::Short;
use crate::abi::Writer;

/// The largest write the session accepts, and the largest read it asks for.
pub const MAX_WRITE: u32 = 1 << 20;
/// A request buffer: the largest write, its headers, and room to spare.
const BUF_LEN: usize = MAX_WRITE as usize + 64 * 1024;

/// What INIT settled with the kernel: kept by a server that may be restarted on the same
/// connection, which receives no second INIT.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Negotiated {
    pub minor: u32,
    pub flags: u64,
}

/// A FUSE connection, ready to serve.
pub struct Session {
    fd: Arc<OwnedFd>,
    negotiated: Option<Negotiated>,
}

impl Session {
    /// A fresh connection: the kernel's INIT comes first.
    pub fn new(fd: OwnedFd) -> Self {
        Self {
            fd: Arc::new(fd),
            negotiated: None,
        }
    }

    /// A connection a previous server negotiated: its INIT is answered, and requests may be
    /// waiting.
    pub fn adopt(fd: OwnedFd, negotiated: Negotiated) -> Self {
        Self {
            fd: Arc::new(fd),
            negotiated: Some(negotiated),
        }
    }

    /// The connection's device descriptor (to keep it across a restart).
    pub fn fd(&self) -> &Arc<OwnedFd> {
        &self.fd
    }

    /// Serves `fs` until the file system is unmounted: answers INIT (unless adopted), calls
    /// `on_init` with what was negotiated and [`Filesystem::init`] with a notifier, then serves
    /// from `threads` threads, each on its own clone of the device. Returns once every thread has
    /// seen the connection end, after [`Filesystem::destroy`].
    pub fn run<F: Filesystem>(
        self,
        fs: Arc<F>,
        threads: usize,
        on_init: impl FnOnce(Negotiated),
    ) -> io::Result<()> {
        let negotiated = match self.negotiated {
            Some(n) => n,
            None => match handshake(&self.fd, fs.posix_acl())? {
                Some(n) => n,
                None => return Ok(()),
            },
        };
        on_init(negotiated);
        fs.init(Notifier {
            fd: self.fd.clone(),
            resend: negotiated.flags & abi::HAS_RESEND != 0,
            minor: negotiated.minor,
        });
        let mut handles = Vec::new();
        for i in 0..threads.max(1) {
            let fd = if i == 0 {
                self.fd.clone()
            } else {
                Arc::new(clone_fd(&self.fd)?)
            };
            let fs = fs.clone();
            handles.push(
                std::thread::Builder::new()
                    .name(format!("okfs-fuse-{i}"))
                    .spawn(move || serve(&fd, &*fs, negotiated))?,
            );
        }
        let mut first_err = None;
        for h in handles {
            if let Err(e) = h.join().expect("a FUSE thread panicked") {
                first_err.get_or_insert(e);
            }
        }
        fs.destroy();
        first_err.map_or(Ok(()), Err)
    }
}

/// Opens a new descriptor on the connection (`FUSE_DEV_IOC_CLONE`), with its own queue of
/// requests in flight.
fn clone_fd(fd: &OwnedFd) -> io::Result<OwnedFd> {
    let raw = unsafe { libc::open(c"/dev/fuse".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    let new = unsafe { OwnedFd::from_raw_fd(raw) };
    let src: u32 = fd.as_raw_fd() as u32;
    if unsafe { libc::ioctl(new.as_raw_fd(), abi::DEV_IOC_CLONE, &src) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(new)
}

/// Reads one request into buf: its length, None once the connection has ended.
fn read_request(fd: &OwnedFd, buf: &mut [u8]) -> io::Result<Option<usize>> {
    loop {
        let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
        if n >= 0 {
            return Ok(Some(n as usize));
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            // Interrupted, or a request that was interrupted before it was read.
            Some(libc::EINTR) | Some(libc::EAGAIN) | Some(libc::ENOENT) => continue,
            // Unmounted, or aborted.
            Some(libc::ENODEV) => return Ok(None),
            _ => return Err(err),
        }
    }
}

fn write_all(fd: &OwnedFd, buf: &[u8]) {
    let n = unsafe { libc::write(fd.as_raw_fd(), buf.as_ptr().cast(), buf.len()) };
    // ENOENT: the request was interrupted and the kernel gave up on it. Anything else is a reply
    // the kernel refused, which no retry would fix.
    if n < 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::ENOENT) {
            eprintln!("okfs: writing a FUSE reply: {err}");
        }
    }
}

fn reply_bytes(fd: &OwnedFd, unique: u64, payload: &[u8]) {
    let mut w = Writer::with_capacity(abi::OUT_HEADER_LEN + payload.len());
    w.u32((abi::OUT_HEADER_LEN + payload.len()) as u32)
        .i32(0)
        .u64(unique)
        .bytes(payload);
    write_all(fd, &w.buf);
}

fn reply_err(fd: &OwnedFd, unique: u64, errno: i32) {
    let mut w = Writer::with_capacity(abi::OUT_HEADER_LEN);
    w.u32(abi::OUT_HEADER_LEN as u32).i32(-errno).u64(unique);
    write_all(fd, &w.buf);
}

/// Answers the kernel's INIT: None if the connection ended first. `posix_acl`: the file system
/// keeps POSIX ACLs ([`Filesystem::posix_acl`]).
fn handshake(fd: &OwnedFd, posix_acl: bool) -> io::Result<Option<Negotiated>> {
    let mut buf = vec![0u8; BUF_LEN];
    loop {
        let Some(n) = read_request(fd, &mut buf)? else {
            return Ok(None);
        };
        let mut r = Reader::new(&buf[..n]);
        let h = r
            .header()
            .map_err(|_| io::Error::other("a short FUSE request"))?;
        if h.opcode != abi::INIT {
            reply_err(fd, h.unique, libc::EIO);
            continue;
        }
        let parse = |r: &mut Reader<'_>| -> Result<(u32, u32, u32, u64), Short> {
            let major = r.u32()?;
            let minor = r.u32()?;
            let readahead = r.u32()?;
            let flags = r.u32()? as u64;
            let flags2 = if flags & abi::INIT_EXT != 0 {
                r.u32()? as u64
            } else {
                0
            };
            Ok((major, minor, readahead, flags | flags2 << 32))
        };
        let (major, minor, readahead, offered) =
            parse(&mut r).map_err(|_| io::Error::other("a short FUSE INIT"))?;
        if major > abi::KERNEL_VERSION {
            // A newer major protocol: say ours, and the kernel asks again.
            let mut w = Writer::default();
            w.u32(abi::KERNEL_VERSION)
                .u32(abi::KERNEL_MINOR_VERSION)
                .zeros(56);
            reply_bytes(fd, h.unique, &w.buf);
            continue;
        }
        if major < abi::KERNEL_VERSION || minor < abi::MIN_KERNEL_MINOR {
            reply_err(fd, h.unique, libc::EPROTO);
            return Err(io::Error::other(format!(
                "the kernel speaks FUSE {major}.{minor}; okfs needs 7.{} or newer",
                abi::MIN_KERNEL_MINOR
            )));
        }
        let mut wanted = abi::ASYNC_READ
            | abi::ATOMIC_O_TRUNC
            | abi::BIG_WRITES
            | abi::AUTO_INVAL_DATA
            | abi::DO_READDIRPLUS
            | abi::READDIRPLUS_AUTO
            | abi::PARALLEL_DIROPS
            | abi::MAX_PAGES
            | abi::CACHE_SYMLINKS
            | abi::SETXATTR_EXT
            | abi::INIT_EXT
            | abi::HAS_RESEND;
        if posix_acl {
            wanted |= abi::POSIX_ACL | abi::DONT_MASK;
        }
        let flags = wanted & offered;
        let mut w = Writer::with_capacity(64);
        w.u32(abi::KERNEL_VERSION)
            .u32(abi::KERNEL_MINOR_VERSION)
            .u32(readahead.min(MAX_WRITE))
            .u32(flags as u32)
            .u16(64) // max_background
            .u16(48) // congestion_threshold
            .u32(MAX_WRITE)
            .u32(1) // time_gran: nanoseconds
            .u16((MAX_WRITE / 4096) as u16) // max_pages
            .u16(0) // map_alignment
            .u32((flags >> 32) as u32)
            .u32(0) // max_stack_depth
            .u16(0) // request_timeout
            .zeros(22);
        reply_bytes(fd, h.unique, &w.buf);
        return Ok(Some(Negotiated {
            minor: minor.min(abi::KERNEL_MINOR_VERSION),
            flags,
        }));
    }
}

/// One thread's loop: read, dispatch, reply, until the connection ends.
fn serve(fd: &OwnedFd, fs: &dyn Filesystem, negotiated: Negotiated) -> io::Result<()> {
    let mut buf = vec![0u8; BUF_LEN];
    while let Some(n) = read_request(fd, &mut buf)? {
        let mut r = Reader::new(&buf[..n]);
        let Ok(h) = r.header() else {
            continue;
        };
        // Extensions (security contexts, supplementary groups) follow the arguments; okfs
        // negotiates none, but a request carrying them is parsed without them.
        let body_len = (n - abi::IN_HEADER_LEN).saturating_sub(h.total_extlen as usize * 8);
        let mut body = Reader::new(&buf[abi::IN_HEADER_LEN..abi::IN_HEADER_LEN + body_len]);
        let ctx = Ctx {
            uid: h.uid,
            gid: h.gid,
            pid: h.pid,
        };
        match dispatch(fs, &ctx, &h, &mut body, negotiated) {
            Ok(Reply::None) => {}
            Ok(Reply::Empty) => reply_bytes(fd, h.unique, &[]),
            Ok(Reply::Bytes(b)) => reply_bytes(fd, h.unique, &b),
            Err(Failed::Errno(e)) => reply_err(fd, h.unique, e),
            Err(Failed::Short) => reply_err(fd, h.unique, libc::EIO),
        }
    }
    Ok(())
}

enum Reply {
    /// FORGET and INTERRUPT: no reply.
    None,
    Empty,
    Bytes(Vec<u8>),
}

enum Failed {
    Errno(i32),
    Short,
}

impl From<i32> for Failed {
    fn from(e: i32) -> Self {
        Failed::Errno(e)
    }
}

impl From<Short> for Failed {
    fn from(_: Short) -> Self {
        Failed::Short
    }
}

fn entry_reply(e: &crate::Entry) -> Reply {
    let mut w = Writer::with_capacity(abi::ENTRY_OUT_LEN);
    abi::write_entry(&mut w, e);
    Reply::Bytes(w.buf)
}

fn attr_reply(a: &crate::Attr, ttl: Duration) -> Reply {
    let (s, ns) = abi::split_ttl(ttl);
    let mut w = Writer::with_capacity(16 + abi::ATTR_LEN);
    w.u64(s).u32(ns).u32(0);
    abi::write_attr(&mut w, a);
    Reply::Bytes(w.buf)
}

fn open_reply(o: &crate::Open) -> Reply {
    let mut w = Writer::with_capacity(16);
    w.u64(o.fh).u32(o.flags).i32(0);
    Reply::Bytes(w.buf)
}

fn name(b: &[u8]) -> &OsStr {
    OsStr::from_bytes(b)
}

/// The reply to an xattr read of `size` bytes at most: its length when size is 0, the value when
/// it fits, ERANGE when it does not.
fn sized(value: Vec<u8>, size: u32) -> Result<Reply, Failed> {
    if size == 0 {
        let mut w = Writer::with_capacity(8);
        w.u32(value.len() as u32).u32(0);
        return Ok(Reply::Bytes(w.buf));
    }
    if value.len() > size as usize {
        return Err(Failed::Errno(libc::ERANGE));
    }
    Ok(Reply::Bytes(value))
}

fn dispatch(
    fs: &dyn Filesystem,
    ctx: &Ctx,
    h: &abi::InHeader,
    r: &mut Reader<'_>,
    negotiated: Negotiated,
) -> Result<Reply, Failed> {
    let ino = h.nodeid;
    Ok(match h.opcode {
        abi::LOOKUP => {
            let n = r.cstr()?;
            match fs.lookup(ctx, ino, name(n))? {
                Some(e) => entry_reply(&e),
                None => {
                    let mut w = Writer::with_capacity(abi::ENTRY_OUT_LEN);
                    abi::write_negative_entry(&mut w, Duration::from_secs(1));
                    Reply::Bytes(w.buf)
                }
            }
        }
        abi::FORGET => {
            let nlookup = r.u64()?;
            fs.forget(ino, nlookup);
            Reply::None
        }
        abi::BATCH_FORGET => {
            let count = r.u32()?;
            r.skip(4)?;
            for _ in 0..count {
                let node = r.u64()?;
                let nlookup = r.u64()?;
                fs.forget(node, nlookup);
            }
            Reply::None
        }
        abi::GETATTR => {
            let flags = r.u32()?;
            r.skip(4)?;
            let fh = r.u64()?;
            let (a, ttl) = fs.getattr(ctx, ino, (flags & abi::GETATTR_FH != 0).then_some(fh))?;
            attr_reply(&a, ttl)
        }
        abi::SETATTR => {
            let valid = r.u32()?;
            r.skip(4)?;
            let fh = r.u64()?;
            let size = r.u64()?;
            r.skip(8)?; // lock_owner
            let atime = r.u64()?;
            let mtime = r.u64()?;
            let ctime = r.u64()?;
            let atimensec = r.u32()?;
            let mtimensec = r.u32()?;
            let ctimensec = r.u32()?;
            let mode = r.u32()?;
            r.skip(4)?;
            let uid = r.u32()?;
            let gid = r.u32()?;
            let time = |v: u32, now: u32, t: u64, ns: u32| {
                if valid & now != 0 {
                    Some(TimeOrNow::Now)
                } else if valid & v != 0 {
                    Some(TimeOrNow::Time((t as i64, ns)))
                } else {
                    None
                }
            };
            let set = SetAttr {
                mode: (valid & abi::FATTR_MODE != 0).then_some(mode),
                uid: (valid & abi::FATTR_UID != 0).then_some(uid),
                gid: (valid & abi::FATTR_GID != 0).then_some(gid),
                size: (valid & abi::FATTR_SIZE != 0).then_some(size),
                atime: time(abi::FATTR_ATIME, abi::FATTR_ATIME_NOW, atime, atimensec),
                mtime: time(abi::FATTR_MTIME, abi::FATTR_MTIME_NOW, mtime, mtimensec),
                ctime: (valid & abi::FATTR_CTIME != 0).then_some((ctime as i64, ctimensec)),
                fh: (valid & abi::FATTR_FH != 0).then_some(fh),
            };
            let (a, ttl) = fs.setattr(ctx, ino, &set)?;
            attr_reply(&a, ttl)
        }
        abi::READLINK => Reply::Bytes(fs.readlink(ctx, ino)?),
        abi::SYMLINK => {
            let n = r.cstr()?;
            let target = r.cstr()?;
            entry_reply(&fs.symlink(ctx, ino, name(n), target)?)
        }
        abi::MKNOD => {
            let mode = r.u32()?;
            let rdev = r.u32()?;
            let umask = r.u32()?;
            r.skip(4)?;
            let n = r.cstr()?;
            entry_reply(&fs.mknod(ctx, ino, name(n), mode, umask, rdev)?)
        }
        abi::MKDIR => {
            let mode = r.u32()?;
            let umask = r.u32()?;
            let n = r.cstr()?;
            entry_reply(&fs.mkdir(ctx, ino, name(n), mode, umask)?)
        }
        abi::UNLINK => {
            fs.unlink(ctx, ino, name(r.cstr()?))?;
            Reply::Empty
        }
        abi::RMDIR => {
            fs.rmdir(ctx, ino, name(r.cstr()?))?;
            Reply::Empty
        }
        abi::RENAME | abi::RENAME2 => {
            let newdir = r.u64()?;
            let flags = if h.opcode == abi::RENAME2 {
                let f = r.u32()?;
                r.skip(4)?;
                f
            } else {
                0
            };
            let old = r.cstr()?;
            let new = r.cstr()?;
            fs.rename(ctx, ino, name(old), newdir, name(new), flags)?;
            Reply::Empty
        }
        abi::LINK => {
            let old = r.u64()?;
            let n = r.cstr()?;
            entry_reply(&fs.link(ctx, old, ino, name(n))?)
        }
        abi::OPEN => {
            let flags = r.u32()?;
            open_reply(&fs.open(ctx, ino, flags as i32)?)
        }
        abi::CREATE => {
            let flags = r.u32()?;
            let mode = r.u32()?;
            let umask = r.u32()?;
            r.skip(4)?;
            let n = r.cstr()?;
            let (e, o) = fs.create(ctx, ino, name(n), mode, umask, flags as i32)?;
            let mut w = Writer::with_capacity(abi::ENTRY_OUT_LEN + 16);
            abi::write_entry(&mut w, &e);
            w.u64(o.fh).u32(o.flags).i32(0);
            Reply::Bytes(w.buf)
        }
        abi::READ => {
            let fh = r.u64()?;
            let offset = r.u64()?;
            let size = r.u32()?;
            Reply::Bytes(fs.read(ctx, ino, fh, offset, size)?)
        }
        abi::WRITE => {
            let fh = r.u64()?;
            let offset = r.u64()?;
            let size = r.u32()?;
            let flags = r.u32()?;
            r.skip(16)?; // lock_owner, flags, padding
            let data = r.bytes(size as usize)?;
            let n = fs.write(ctx, ino, fh, offset, data, flags)?;
            let mut w = Writer::with_capacity(8);
            w.u32(n).u32(0);
            Reply::Bytes(w.buf)
        }
        abi::STATFS => {
            let s = fs.statfs(ctx, ino)?;
            let mut w = Writer::with_capacity(80);
            w.u64(s.blocks)
                .u64(s.bfree)
                .u64(s.bavail)
                .u64(s.files)
                .u64(s.ffree)
                .u32(s.bsize)
                .u32(s.namelen)
                .u32(s.bsize)
                .u32(0)
                .zeros(24);
            Reply::Bytes(w.buf)
        }
        abi::RELEASE => {
            let fh = r.u64()?;
            let flags = r.u32()?;
            fs.release(ctx, ino, fh, flags as i32);
            Reply::Empty
        }
        abi::FSYNC | abi::FSYNCDIR => {
            let fh = r.u64()?;
            let datasync = r.u32()? & abi::FSYNC_FDATASYNC != 0;
            if h.opcode == abi::FSYNC {
                fs.fsync(ctx, ino, fh, datasync)?;
            } else {
                fs.fsyncdir(ctx, ino, fh, datasync)?;
            }
            Reply::Empty
        }
        abi::SETXATTR => {
            let size = r.u32()?;
            let flags = r.u32()?;
            let mut setxattr_flags = 0;
            if negotiated.flags & abi::SETXATTR_EXT != 0 {
                setxattr_flags = r.u32()?;
                r.skip(4)?;
            }
            let n = r.cstr()?;
            let value = r.bytes(size as usize)?;
            let kill_sgid = setxattr_flags & abi::SETXATTR_ACL_KILL_SGID != 0;
            fs.setxattr(ctx, ino, name(n), value, flags as i32, kill_sgid)?;
            Reply::Empty
        }
        abi::GETXATTR => {
            let size = r.u32()?;
            r.skip(4)?;
            let n = r.cstr()?;
            sized(fs.getxattr(ctx, ino, name(n))?, size)?
        }
        abi::LISTXATTR => {
            let size = r.u32()?;
            sized(fs.listxattr(ctx, ino)?, size)?
        }
        abi::REMOVEXATTR => {
            fs.removexattr(ctx, ino, name(r.cstr()?))?;
            Reply::Empty
        }
        abi::FLUSH => {
            let fh = r.u64()?;
            fs.flush(ctx, ino, fh)?;
            Reply::Empty
        }
        abi::OPENDIR => {
            let flags = r.u32()?;
            open_reply(&fs.opendir(ctx, ino, flags as i32)?)
        }
        abi::READDIR | abi::READDIRPLUS => {
            let fh = r.u64()?;
            let offset = r.u64()?;
            let size = r.u32()?;
            let mut w = Writer::with_capacity(abi::OUT_HEADER_LEN + size as usize);
            // DirBuf measures from after the reply's header, which reply_bytes adds.
            w.zeros(abi::OUT_HEADER_LEN);
            let mut buf = DirBuf {
                w: &mut w,
                max: size as usize,
                plus: h.opcode == abi::READDIRPLUS,
            };
            fs.readdir(ctx, ino, fh, offset, &mut buf)?;
            Reply::Bytes(w.buf.split_off(abi::OUT_HEADER_LEN))
        }
        abi::RELEASEDIR => {
            let fh = r.u64()?;
            fs.releasedir(ctx, ino, fh);
            Reply::Empty
        }
        abi::ACCESS => Reply::Empty,
        abi::INTERRUPT | abi::DESTROY => {
            if h.opcode == abi::DESTROY {
                return Ok(Reply::Empty);
            }
            Reply::None
        }
        abi::FALLOCATE => {
            let fh = r.u64()?;
            let offset = r.u64()?;
            let length = r.u64()?;
            let mode = r.u32()?;
            fs.fallocate(ctx, ino, fh, offset, length, mode as i32)?;
            Reply::Empty
        }
        abi::LSEEK => {
            let fh = r.u64()?;
            let offset = r.u64()?;
            let whence = r.u32()?;
            let at = fs.lseek(ctx, ino, fh, offset as i64, whence)?;
            let mut w = Writer::with_capacity(8);
            w.i64(at);
            Reply::Bytes(w.buf)
        }
        _ => return Err(Failed::Errno(libc::ENOSYS)),
    })
}
