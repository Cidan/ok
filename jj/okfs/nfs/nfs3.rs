//! NFSv3's procedures (RFC 1813) and MOUNT v3's: each call's arguments decoded, answered from its
//! export's [`Filesystem`], and the reply encoded — with the attributes before and after a change
//! (weak cache consistency) that let a client tell its own changes from another's.

use crate::Attr;
use crate::Ctx;
use crate::Filesystem;
use crate::Kind;
use crate::MAX_IO;
use crate::Server;
use crate::SetAttr;
use crate::SetTime;
use crate::Time;
use crate::rpc::Accept;
use crate::xdr;
use crate::xdr::Reader;
use crate::xdr::Writer;

// nfsstat3.
pub const OK: u32 = 0;
pub const PERM: u32 = 1;
pub const NOENT: u32 = 2;
pub const IO: u32 = 5;
pub const NXIO: u32 = 6;
pub const ACCES: u32 = 13;
pub const EXIST: u32 = 17;
pub const XDEV: u32 = 18;
pub const NODEV: u32 = 19;
pub const NOTDIR: u32 = 20;
pub const ISDIR: u32 = 21;
pub const INVAL: u32 = 22;
pub const FBIG: u32 = 27;
pub const NOSPC: u32 = 28;
pub const ROFS: u32 = 30;
pub const MLINK: u32 = 31;
pub const NAMETOOLONG: u32 = 63;
pub const NOTEMPTY: u32 = 66;
pub const DQUOT: u32 = 69;
pub const STALE: u32 = 70;
pub const BADHANDLE: u32 = 10001;
pub const NOT_SYNC: u32 = 10002;
pub const NOTSUPP: u32 = 10004;
pub const TOOSMALL: u32 = 10005;
pub const BADTYPE: u32 = 10007;
pub const JUKEBOX: u32 = 10008;

// Procedures.
pub const NULL: u32 = 0;
pub const GETATTR: u32 = 1;
pub const SETATTR: u32 = 2;
pub const LOOKUP: u32 = 3;
pub const ACCESS: u32 = 4;
pub const READLINK: u32 = 5;
pub const READ: u32 = 6;
pub const WRITE: u32 = 7;
pub const CREATE: u32 = 8;
pub const MKDIR: u32 = 9;
pub const SYMLINK: u32 = 10;
pub const MKNOD: u32 = 11;
pub const REMOVE: u32 = 12;
pub const RMDIR: u32 = 13;
pub const RENAME: u32 = 14;
pub const LINK: u32 = 15;
pub const READDIR: u32 = 16;
pub const READDIRPLUS: u32 = 17;
pub const FSSTAT: u32 = 18;
pub const FSINFO: u32 = 19;
pub const PATHCONF: u32 = 20;
pub const COMMIT: u32 = 21;

/// MOUNT v3's procedures.
pub const MNT: u32 = 1;
pub const DUMP: u32 = 2;
pub const UMNT: u32 = 3;
pub const UMNTALL: u32 = 4;
pub const EXPORT: u32 = 5;

/// The longest name a directory holds.
pub const NAME_MAX: usize = 255;
/// The longest symlink target.
const PATH_MAX: usize = 4096;

/// The NFS status for an errno.
pub fn status(errno: i32) -> u32 {
    match errno {
        libc::EPERM => PERM,
        libc::ENOENT => NOENT,
        libc::ENXIO => NXIO,
        libc::EACCES => ACCES,
        libc::EEXIST => EXIST,
        libc::EXDEV => XDEV,
        libc::ENODEV => NODEV,
        libc::ENOTDIR => NOTDIR,
        libc::EISDIR => ISDIR,
        libc::EINVAL => INVAL,
        libc::EFBIG => FBIG,
        libc::ENOSPC => NOSPC,
        libc::EROFS => ROFS,
        libc::EMLINK => MLINK,
        libc::ENAMETOOLONG => NAMETOOLONG,
        libc::ENOTEMPTY => NOTEMPTY,
        libc::EDQUOT => DQUOT,
        // A file the server no longer knows, or a file system being unmounted.
        libc::ESTALE | libc::ENOTCONN => STALE,
        libc::EAGAIN => JUKEBOX,
        e if e == libc::ENOTSUP || e == libc::EOPNOTSUPP => NOTSUPP,
        _ => IO,
    }
}

fn ftype(k: Kind) -> u32 {
    match k {
        Kind::File => 1,
        Kind::Dir => 2,
        Kind::Block => 3,
        Kind::Char => 4,
        Kind::Symlink => 5,
        Kind::Socket => 6,
        Kind::Fifo => 7,
    }
}

fn kind_of(t: u32) -> Option<Kind> {
    Some(match t {
        1 => Kind::File,
        2 => Kind::Dir,
        3 => Kind::Block,
        4 => Kind::Char,
        5 => Kind::Symlink,
        6 => Kind::Socket,
        7 => Kind::Fifo,
        _ => return None,
    })
}

/// A time as nfstime3 holds it: unsigned seconds.
fn wire_time(t: Time) -> (u32, u32) {
    (
        t.secs.clamp(0, i64::from(u32::MAX)) as u32,
        t.nanos.min(999_999_999),
    )
}

fn time(w: &mut Writer, t: Time) {
    let (s, n) = wire_time(t);
    w.u32(s).u32(n);
}

fn read_time(r: &mut Reader) -> xdr::Result<Time> {
    Ok(Time {
        secs: i64::from(r.u32()?),
        nanos: r.u32()?,
    })
}

fn fattr(w: &mut Writer, a: &Attr, fsid: u64) {
    w.u32(ftype(a.kind))
        .u32(a.perm & 0o7777)
        .u32(a.nlink)
        .u32(a.uid)
        .u32(a.gid)
        .u64(a.size)
        .u64(a.used)
        .u32(a.rdev.0)
        .u32(a.rdev.1)
        .u64(fsid)
        .u64(a.fileid);
    time(w, a.atime);
    time(w, a.mtime);
    time(w, a.ctime);
}

/// Reads a fattr3 (the client's).
pub fn read_fattr(r: &mut Reader) -> xdr::Result<Attr> {
    let kind = kind_of(r.u32()?).ok_or(xdr::Malformed)?;
    let perm = r.u32()?;
    let nlink = r.u32()?;
    let uid = r.u32()?;
    let gid = r.u32()?;
    let size = r.u64()?;
    let used = r.u64()?;
    let rdev = (r.u32()?, r.u32()?);
    let _fsid = r.u64()?;
    let fileid = r.u64()?;
    Ok(Attr {
        kind,
        perm,
        nlink,
        uid,
        gid,
        size,
        used,
        rdev,
        fileid,
        atime: read_time(r)?,
        mtime: read_time(r)?,
        ctime: read_time(r)?,
    })
}

fn post_attr(w: &mut Writer, a: Option<&Attr>, fsid: u64) {
    match a {
        Some(a) => {
            w.bool(true);
            fattr(w, a, fsid);
        }
        None => {
            w.bool(false);
        }
    }
}

/// Reads a post_op_attr (the client's).
pub fn read_post_attr(r: &mut Reader) -> xdr::Result<Option<Attr>> {
    if r.bool()? {
        Ok(Some(read_fattr(r)?))
    } else {
        Ok(None)
    }
}

fn wcc(w: &mut Writer, before: Option<&Attr>, after: Option<&Attr>, fsid: u64) {
    match before {
        Some(b) => {
            w.bool(true).u64(b.size);
            time(w, b.mtime);
            time(w, b.ctime);
        }
        None => {
            w.bool(false);
        }
    }
    post_attr(w, after, fsid);
}

/// Before and after a change, as a client reads wcc_data: the size, mtime, and ctime before.
pub type Wcc = (Option<(u64, Time, Time)>, Option<Attr>);

/// Reads a wcc_data (the client's).
pub fn read_wcc(r: &mut Reader) -> xdr::Result<Wcc> {
    let before = if r.bool()? {
        Some((r.u64()?, read_time(r)?, read_time(r)?))
    } else {
        None
    };
    Ok((before, read_post_attr(r)?))
}

fn read_set_time(r: &mut Reader) -> xdr::Result<Option<SetTime>> {
    Ok(match r.u32()? {
        0 => None,
        1 => Some(SetTime::Now),
        2 => Some(SetTime::At(read_time(r)?)),
        _ => return Err(xdr::Malformed),
    })
}

fn read_sattr(r: &mut Reader) -> xdr::Result<SetAttr> {
    let mode = if r.bool()? { Some(r.u32()?) } else { None };
    let uid = if r.bool()? { Some(r.u32()?) } else { None };
    let gid = if r.bool()? { Some(r.u32()?) } else { None };
    let size = if r.bool()? { Some(r.u64()?) } else { None };
    Ok(SetAttr {
        mode,
        uid,
        gid,
        size,
        atime: read_set_time(r)?,
        mtime: read_set_time(r)?,
    })
}

/// Writes a sattr3 (the client's).
pub fn write_sattr(w: &mut Writer, s: &SetAttr) {
    let opt32 = |w: &mut Writer, v: Option<u32>| match v {
        Some(v) => {
            w.bool(true).u32(v);
        }
        None => {
            w.bool(false);
        }
    };
    opt32(w, s.mode);
    opt32(w, s.uid);
    opt32(w, s.gid);
    match s.size {
        Some(v) => {
            w.bool(true).u64(v);
        }
        None => {
            w.bool(false);
        }
    }
    for t in [s.atime, s.mtime] {
        match t {
            None => {
                w.u32(0);
            }
            Some(SetTime::Now) => {
                w.u32(1);
            }
            Some(SetTime::At(t)) => {
                w.u32(2);
                time(w, t);
            }
        }
    }
}

fn fh<'a>(r: &mut Reader<'a>) -> xdr::Result<&'a [u8]> {
    r.opaque(64)
}

fn name<'a>(r: &mut Reader<'a>) -> xdr::Result<&'a [u8]> {
    r.opaque(4096)
}

/// A failure's reply: `status`, then the procedure's failure results with no attributes.
fn fail(w: &mut Writer, proc_: u32, status: u32) {
    w.u32(status);
    match proc_ {
        GETATTR => {}
        SETATTR | WRITE | CREATE | MKDIR | SYMLINK | MKNOD | REMOVE | RMDIR | COMMIT => {
            w.bool(false).bool(false);
        }
        RENAME => {
            w.bool(false).bool(false).bool(false).bool(false);
        }
        LINK => {
            w.bool(false).bool(false).bool(false);
        }
        _ => {
            w.bool(false);
        }
    }
}

/// Opens a handle, or answers with the status it fails with.
macro_rules! open {
    ($s:expr, $w:expr, $proc:expr, $fh:expr) => {
        match $s.open($fh) {
            Ok(x) => x,
            Err(st) => {
                fail($w, $proc, st);
                return Ok(());
            }
        }
    };
}

/// Answers an NFSv3 call. `ctx` is set for every procedure but NULL.
pub(crate) fn nfs(
    s: &Server,
    ctx: Option<&Ctx>,
    proc_: u32,
    args: &[u8],
) -> Result<Vec<u8>, Accept> {
    if proc_ == NULL {
        return Ok(Vec::new());
    }
    if proc_ > COMMIT {
        return Err(Accept::ProcUnavail);
    }
    let ctx = ctx.ok_or(Accept::SystemErr)?;
    let mut w = Writer::new();
    // Only the kernel holds handles, so a call on one says truly who the process making it is:
    // anyone but the owner is refused, as FUSE refuses others by default.
    if ctx.uid != s.owner {
        fail(&mut w, proc_, ACCES);
        return Ok(w.into_vec());
    }
    let mut r = Reader::new(args);
    let done = match proc_ {
        GETATTR => getattr(s, ctx, &mut r, &mut w),
        SETATTR => setattr(s, ctx, &mut r, &mut w),
        LOOKUP => lookup(s, ctx, &mut r, &mut w),
        ACCESS => access(s, ctx, &mut r, &mut w),
        READLINK => readlink(s, ctx, &mut r, &mut w),
        READ => read(s, ctx, &mut r, &mut w),
        WRITE => write(s, ctx, &mut r, &mut w),
        CREATE => create(s, ctx, &mut r, &mut w),
        MKDIR | SYMLINK | MKNOD => make(s, ctx, proc_, &mut r, &mut w),
        REMOVE | RMDIR => remove(s, ctx, proc_, &mut r, &mut w),
        RENAME => rename(s, ctx, &mut r, &mut w),
        LINK => link(s, ctx, &mut r, &mut w),
        READDIR => readdir(s, ctx, false, &mut r, &mut w),
        READDIRPLUS => readdir(s, ctx, true, &mut r, &mut w),
        FSSTAT => fsstat(s, ctx, &mut r, &mut w),
        FSINFO => fsinfo(s, ctx, &mut r, &mut w),
        PATHCONF => pathconf(s, ctx, &mut r, &mut w),
        COMMIT => commit(s, ctx, &mut r, &mut w),
        _ => unreachable!(),
    };
    done.map_err(|_| Accept::GarbageArgs)?;
    Ok(w.into_vec())
}

fn getattr(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let (fs, id, ino) = open!(s, w, GETATTR, fh(r)?);
    match fs.getattr(ctx, ino) {
        Ok(a) => {
            w.u32(OK);
            fattr(w, &a, id);
        }
        Err(e) => fail(w, GETATTR, status(e)),
    }
    Ok(())
}

fn setattr(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let handle = fh(r)?;
    let set = read_sattr(r)?;
    let guard = if r.bool()? { Some(read_time(r)?) } else { None };
    let (fs, id, ino) = open!(s, w, SETATTR, handle);
    let before = fs.getattr(ctx, ino).ok();
    if let (Some(g), Some(b)) = (guard, before.as_ref())
        && wire_time(g) != wire_time(b.ctime)
    {
        w.u32(NOT_SYNC);
        wcc(w, before.as_ref(), before.as_ref(), id);
        return Ok(());
    }
    let res = fs.setattr(ctx, ino, &set);
    let after = fs.getattr(ctx, ino).ok();
    w.u32(res.err().map_or(OK, status));
    wcc(w, before.as_ref(), after.as_ref(), id);
    Ok(())
}

fn lookup(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let dir_fh = fh(r)?;
    let n = name(r)?;
    let (fs, id, dir) = open!(s, w, LOOKUP, dir_fh);
    let dir_attr = fs.getattr(ctx, dir).ok();
    if n.len() > NAME_MAX {
        w.u32(NAMETOOLONG);
        post_attr(w, dir_attr.as_ref(), id);
        return Ok(());
    }
    match fs.lookup(ctx, dir, n) {
        Ok(ino) => {
            let a = fs.getattr(ctx, ino).ok();
            w.u32(OK).opaque(&s.handle(id, ino));
            post_attr(w, a.as_ref(), id);
            post_attr(w, dir_attr.as_ref(), id);
        }
        Err(e) => {
            w.u32(status(e));
            post_attr(w, dir_attr.as_ref(), id);
        }
    }
    Ok(())
}

// ACCESS3 bits.
const ACCESS_READ: u32 = 0x1;
const ACCESS_LOOKUP: u32 = 0x2;
const ACCESS_MODIFY: u32 = 0x4;
const ACCESS_EXTEND: u32 = 0x8;
const ACCESS_DELETE: u32 = 0x10;
const ACCESS_EXECUTE: u32 = 0x20;

/// What the caller may do to a file, by its permission bits.
fn granted(ctx: &Ctx, a: &Attr) -> u32 {
    let bits = if ctx.uid == a.uid {
        a.perm >> 6
    } else if ctx.gid == a.gid || ctx.gids.contains(&a.gid) {
        a.perm >> 3
    } else {
        a.perm
    } & 7;
    let (r, wr, x) = (bits & 4 != 0, bits & 2 != 0, bits & 1 != 0);
    let mut g = 0;
    if r {
        g |= ACCESS_READ;
    }
    if wr {
        g |= ACCESS_MODIFY | ACCESS_EXTEND;
        if a.kind == Kind::Dir {
            g |= ACCESS_DELETE;
        }
    }
    if x {
        g |= if a.kind == Kind::Dir {
            ACCESS_LOOKUP
        } else {
            ACCESS_EXECUTE
        };
    }
    g
}

fn access(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let handle = fh(r)?;
    let want = r.u32()?;
    let (fs, id, ino) = open!(s, w, ACCESS, handle);
    match fs.getattr(ctx, ino) {
        Ok(a) => {
            w.u32(OK);
            post_attr(w, Some(&a), id);
            w.u32(want & granted(ctx, &a));
        }
        Err(e) => fail(w, ACCESS, status(e)),
    }
    Ok(())
}

fn readlink(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let (fs, id, ino) = open!(s, w, READLINK, fh(r)?);
    match fs.readlink(ctx, ino) {
        Ok(target) => {
            let a = fs.getattr(ctx, ino).ok();
            w.u32(OK);
            post_attr(w, a.as_ref(), id);
            w.opaque(&target);
        }
        Err(e) => {
            let a = fs.getattr(ctx, ino).ok();
            w.u32(status(e));
            post_attr(w, a.as_ref(), id);
        }
    }
    Ok(())
}

fn read(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let handle = fh(r)?;
    let offset = r.u64()?;
    let count = r.u32()?.min(MAX_IO);
    let (fs, id, ino) = open!(s, w, READ, handle);
    match fs.read(ctx, ino, offset, count) {
        Ok(data) => {
            let a = fs.getattr(ctx, ino).ok();
            let eof = a
                .as_ref()
                .map_or(data.is_empty(), |a| offset + data.len() as u64 >= a.size);
            w.u32(OK);
            post_attr(w, a.as_ref(), id);
            w.u32(data.len() as u32).bool(eof).opaque(&data);
        }
        Err(e) => {
            let a = fs.getattr(ctx, ino).ok();
            w.u32(status(e));
            post_attr(w, a.as_ref(), id);
        }
    }
    Ok(())
}

/// stable_how: every write is committed when answered.
const FILE_SYNC: u32 = 2;

fn write(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let handle = fh(r)?;
    let offset = r.u64()?;
    let count = r.u32()? as usize;
    let _stable = r.u32()?;
    let data = r.opaque(MAX_IO as usize)?;
    let data = &data[..count.min(data.len())];
    let (fs, id, ino) = open!(s, w, WRITE, handle);
    let before = fs.getattr(ctx, ino).ok();
    let res = fs.write(ctx, ino, offset, data);
    let after = fs.getattr(ctx, ino).ok();
    match res {
        Ok(n) => {
            w.u32(OK);
            wcc(w, before.as_ref(), after.as_ref(), id);
            w.u32(n).u32(FILE_SYNC).fixed(&s.verifier);
        }
        Err(e) => {
            w.u32(status(e));
            wcc(w, before.as_ref(), after.as_ref(), id);
        }
    }
    Ok(())
}

/// Applies what a create's attributes set besides its mode.
fn set_rest(fs: &dyn Filesystem, ctx: &Ctx, ino: u64, set: &SetAttr) -> crate::Result<()> {
    let rest = SetAttr { mode: None, ..*set };
    if rest != SetAttr::default() {
        fs.setattr(ctx, ino, &rest)?;
    }
    Ok(())
}

/// The directory a create made something in: its export, its file, and its attributes before.
struct Made {
    id: u64,
    dir: u64,
    before: Option<Attr>,
}

/// A create's reply: the made file's handle and attributes, and its directory's before and after.
fn made(
    s: &Server,
    w: &mut Writer,
    fs: &dyn Filesystem,
    ctx: &Ctx,
    at: Made,
    res: crate::Result<u64>,
) {
    let Made { id, dir, before } = at;
    let after = fs.getattr(ctx, dir).ok();
    match res {
        Ok(ino) => {
            let a = fs.getattr(ctx, ino).ok();
            w.u32(OK).bool(true).opaque(&s.handle(id, ino));
            post_attr(w, a.as_ref(), id);
            wcc(w, before.as_ref(), after.as_ref(), id);
        }
        Err(e) => {
            w.u32(status(e));
            wcc(w, before.as_ref(), after.as_ref(), id);
        }
    }
}

// createmode3.
const UNCHECKED: u32 = 0;
const GUARDED: u32 = 1;
const EXCLUSIVE: u32 = 2;

fn create(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let dir_fh = fh(r)?;
    let n = name(r)?;
    let how = r.u32()?;
    let (set, verf) = match how {
        UNCHECKED | GUARDED => (read_sattr(r)?, None),
        EXCLUSIVE => (
            SetAttr::default(),
            Some(<[u8; 8]>::try_from(r.fixed(8)?).unwrap()),
        ),
        _ => return Err(xdr::Malformed),
    };
    let (fs, id, dir) = open!(s, w, CREATE, dir_fh);
    let before = fs.getattr(ctx, dir).ok();
    if n.len() > NAME_MAX {
        made(
            s,
            w,
            &*fs,
            ctx,
            Made { id, dir, before },
            Err(libc::ENAMETOOLONG),
        );
        return Ok(());
    }
    let new = |mode: u32| -> crate::Result<u64> {
        let ino = fs.create(ctx, dir, n, Kind::File, mode & 0o7777, b"")?;
        set_rest(&*fs, ctx, ino, &set)?;
        Ok(ino)
    };
    let res = match (how, verf) {
        (UNCHECKED, _) => match fs.lookup(ctx, dir, n) {
            // Unchecked: an existing file is opened as it is, the attributes given applied.
            Ok(existing) => fs.getattr(ctx, existing).and_then(|a| {
                if a.kind != Kind::File {
                    return Err(libc::EEXIST);
                }
                set_rest(&*fs, ctx, existing, &set)?;
                Ok(existing)
            }),
            Err(libc::ENOENT) => new(set.mode.unwrap_or(0o644)),
            Err(e) => Err(e),
        },
        (EXCLUSIVE, Some(verf)) => match fs.create(ctx, dir, n, Kind::File, 0o644, b"") {
            Ok(ino) => {
                s.created_exclusive(id, ino, verf);
                Ok(ino)
            }
            // A retransmission of the create that made it succeeds again.
            Err(libc::EEXIST) => fs.lookup(ctx, dir, n).and_then(|existing| {
                if s.exclusive_verifier(id, existing) == Some(verf) {
                    Ok(existing)
                } else {
                    Err(libc::EEXIST)
                }
            }),
            Err(e) => Err(e),
        },
        _ => new(set.mode.unwrap_or(0o644)),
    };
    made(s, w, &*fs, ctx, Made { id, dir, before }, res);
    Ok(())
}

/// MKDIR, SYMLINK, and MKNOD.
fn make(s: &Server, ctx: &Ctx, proc_: u32, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let dir_fh = fh(r)?;
    let n = name(r)?;
    let (kind, set, target) = match proc_ {
        MKDIR => (Some(Kind::Dir), read_sattr(r)?, &[][..]),
        SYMLINK => {
            let set = read_sattr(r)?;
            (Some(Kind::Symlink), set, r.opaque(PATH_MAX)?)
        }
        _ => {
            let t = kind_of(r.u32()?).ok_or(xdr::Malformed)?;
            match t {
                Kind::Block | Kind::Char => {
                    let set = read_sattr(r)?;
                    r.u32()?;
                    r.u32()?;
                    (None, set, &[][..])
                }
                Kind::Socket | Kind::Fifo => (Some(t), read_sattr(r)?, &[][..]),
                _ => (Some(t), SetAttr::default(), &[][..]),
            }
        }
    };
    let (fs, id, dir) = open!(s, w, proc_, dir_fh);
    let before = fs.getattr(ctx, dir).ok();
    let res = match kind {
        _ if n.len() > NAME_MAX => Err(libc::ENAMETOOLONG),
        // Devices are not made here.
        None => Err(libc::EPERM),
        Some(Kind::File | Kind::Dir | Kind::Symlink) if proc_ == MKNOD => {
            w.u32(BADTYPE).bool(false).bool(false);
            return Ok(());
        }
        Some(k) => {
            let mode = match k {
                Kind::Dir => set.mode.unwrap_or(0o755),
                Kind::Symlink => 0o777,
                _ => set.mode.unwrap_or(0o644),
            };
            fs.create(ctx, dir, n, k, mode & 0o7777, target)
                .and_then(|ino| set_rest(&*fs, ctx, ino, &set).map(|()| ino))
        }
    };
    made(s, w, &*fs, ctx, Made { id, dir, before }, res);
    Ok(())
}

/// REMOVE and RMDIR.
fn remove(s: &Server, ctx: &Ctx, proc_: u32, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let dir_fh = fh(r)?;
    let n = name(r)?;
    let (fs, id, dir) = open!(s, w, proc_, dir_fh);
    let before = fs.getattr(ctx, dir).ok();
    let res = if proc_ == REMOVE {
        fs.remove(ctx, dir, n)
    } else {
        fs.rmdir(ctx, dir, n)
    };
    let after = fs.getattr(ctx, dir).ok();
    w.u32(res.err().map_or(OK, status));
    wcc(w, before.as_ref(), after.as_ref(), id);
    Ok(())
}

fn rename(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let from_fh = fh(r)?;
    let from_name = name(r)?;
    let to_fh = fh(r)?;
    let to_name = name(r)?;
    let (fs, id, from) = open!(s, w, RENAME, from_fh);
    let (_, to_id, to) = open!(s, w, RENAME, to_fh);
    if to_id != id {
        fail(w, RENAME, XDEV);
        return Ok(());
    }
    let (fb, tb) = (fs.getattr(ctx, from).ok(), fs.getattr(ctx, to).ok());
    let res = if from_name.len() > NAME_MAX || to_name.len() > NAME_MAX {
        Err(libc::ENAMETOOLONG)
    } else {
        fs.rename(ctx, from, from_name, to, to_name)
    };
    let (fa, ta) = (fs.getattr(ctx, from).ok(), fs.getattr(ctx, to).ok());
    w.u32(res.err().map_or(OK, status));
    wcc(w, fb.as_ref(), fa.as_ref(), id);
    wcc(w, tb.as_ref(), ta.as_ref(), id);
    Ok(())
}

fn link(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let file_fh = fh(r)?;
    let dir_fh = fh(r)?;
    let n = name(r)?;
    let (fs, id, ino) = open!(s, w, LINK, file_fh);
    let (_, dir_id, dir) = open!(s, w, LINK, dir_fh);
    if dir_id != id {
        fail(w, LINK, XDEV);
        return Ok(());
    }
    let before = fs.getattr(ctx, dir).ok();
    let res = if n.len() > NAME_MAX {
        Err(libc::ENAMETOOLONG)
    } else {
        fs.link(ctx, ino, dir, n)
    };
    let a = fs.getattr(ctx, ino).ok();
    let after = fs.getattr(ctx, dir).ok();
    w.u32(res.err().map_or(OK, status));
    post_attr(w, a.as_ref(), id);
    wcc(w, before.as_ref(), after.as_ref(), id);
    Ok(())
}

/// The bytes of a READDIR(PLUS) reply around its entries: status, the directory's attributes, the
/// cookie verifier, the end of the list, and eof.
const LISTING_OVERHEAD: usize = 4 + 4 + 84 + 8 + 4 + 4;
/// An entry's attributes and handle, as READDIRPLUS adds them.
const PLUS_OVERHEAD: usize = 4 + 84 + 4 + 4 + crate::handle::LEN;

/// READDIR and READDIRPLUS: as many entries as fit in the size the client asked for.
fn readdir(s: &Server, ctx: &Ctx, plus: bool, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let proc_ = if plus { READDIRPLUS } else { READDIR };
    let dir_fh = fh(r)?;
    let cookie = r.u64()?;
    r.fixed(8)?;
    let (dircount, maxcount) = if plus {
        (r.u32()? as usize, r.u32()? as usize)
    } else {
        let c = r.u32()? as usize;
        (c, c)
    };
    let (fs, id, dir) = open!(s, w, proc_, dir_fh);
    let dir_attr = fs.getattr(ctx, dir).ok();
    let per = if plus { 24 + PLUS_OVERHEAD } else { 24 };
    let max = (maxcount / per).clamp(1, 4096);
    let listing = match fs.readdir(ctx, dir, cookie, plus, max) {
        Ok(l) => l,
        Err(e) => {
            w.u32(status(e));
            post_attr(w, dir_attr.as_ref(), id);
            return Ok(());
        }
    };
    let mut body = Writer::new();
    let (mut total, mut dirbytes, mut written) = (LISTING_OVERHEAD, 0usize, 0usize);
    for e in &listing.entries {
        let dirpart = 8 + xdr::opaque_len(&e.name) + 8;
        let n = 4 + dirpart + if plus { PLUS_OVERHEAD } else { 0 };
        if total + n > maxcount || (plus && dircount > 0 && dirbytes + dirpart > dircount) {
            break;
        }
        total += n;
        dirbytes += dirpart;
        body.bool(true).u64(e.fileid).opaque(&e.name).u64(e.cookie);
        if plus {
            post_attr(&mut body, e.attr.as_ref(), id);
            body.bool(true).opaque(&s.handle(id, e.fileid));
        }
        written += 1;
    }
    if written == 0 && !listing.entries.is_empty() {
        w.u32(TOOSMALL);
        post_attr(w, dir_attr.as_ref(), id);
        return Ok(());
    }
    let eof = listing.eof && written == listing.entries.len();
    w.u32(OK);
    post_attr(w, dir_attr.as_ref(), id);
    w.fixed(&[0; 8]).raw(&body.into_vec()).bool(false).bool(eof);
    Ok(())
}

fn fsstat(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let (fs, id, ino) = open!(s, w, FSSTAT, fh(r)?);
    let a = fs.getattr(ctx, ino).ok();
    match fs.fsstat(ctx, ino) {
        Ok(st) => {
            w.u32(OK);
            post_attr(w, a.as_ref(), id);
            w.u64(st.tbytes)
                .u64(st.fbytes)
                .u64(st.abytes)
                .u64(st.tfiles)
                .u64(st.ffiles)
                .u64(st.afiles)
                .u32(0);
        }
        Err(e) => {
            w.u32(status(e));
            post_attr(w, a.as_ref(), id);
        }
    }
    Ok(())
}

// FSINFO's properties.
const FSF_LINK: u32 = 0x1;
const FSF_SYMLINK: u32 = 0x2;
const FSF_HOMOGENEOUS: u32 = 0x8;
const FSF_CANSETTIME: u32 = 0x10;

fn fsinfo(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let (fs, id, ino) = open!(s, w, FSINFO, fh(r)?);
    let a = fs.getattr(ctx, ino).ok();
    w.u32(OK);
    post_attr(w, a.as_ref(), id);
    w.u32(MAX_IO) // rtmax
        .u32(MAX_IO) // rtpref
        .u32(4096) // rtmult
        .u32(MAX_IO) // wtmax
        .u32(MAX_IO) // wtpref
        .u32(4096) // wtmult
        .u32(64 << 10) // dtpref
        .u64(i64::MAX as u64); // maxfilesize
    time(w, Time { secs: 0, nanos: 1 });
    w.u32(FSF_LINK | FSF_SYMLINK | FSF_HOMOGENEOUS | FSF_CANSETTIME);
    Ok(())
}

fn pathconf(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let (fs, id, ino) = open!(s, w, PATHCONF, fh(r)?);
    let a = fs.getattr(ctx, ino).ok();
    w.u32(OK);
    post_attr(w, a.as_ref(), id);
    w.u32(32767) // linkmax
        .u32(NAME_MAX as u32)
        .bool(true) // no_trunc
        .bool(true) // chown_restricted
        .bool(false) // case_insensitive
        .bool(true); // case_preserving
    Ok(())
}

fn commit(s: &Server, ctx: &Ctx, r: &mut Reader, w: &mut Writer) -> xdr::Result<()> {
    let handle = fh(r)?;
    r.u64()?;
    r.u32()?;
    let (fs, id, ino) = open!(s, w, COMMIT, handle);
    let before = fs.getattr(ctx, ino).ok();
    let res = fs.commit(ctx, ino);
    let after = fs.getattr(ctx, ino).ok();
    w.u32(res.err().map_or(OK, status));
    wcc(w, before.as_ref(), after.as_ref(), id);
    if res.is_ok() {
        w.fixed(&s.verifier);
    }
    Ok(())
}

// mountstat3.
const MNT3_OK: u32 = 0;
const MNT3ERR_ACCES: u32 = 13;

/// Answers a MOUNT v3 call: MNT of an armed path, once, gives its export's root handle. Who asks
/// is not checked — mount_nfs asks as root, whoever runs it — so a path is a one-time secret: a
/// mount that fails because another MNT used its path first tells the server's user to serve the
/// file system under a new export, whose handles the other caller does not hold.
pub(crate) fn mount(
    s: &Server,
    _: Option<&Ctx>,
    proc_: u32,
    args: &[u8],
) -> Result<Vec<u8>, Accept> {
    let mut r = Reader::new(args);
    let mut w = Writer::new();
    match proc_ {
        NULL => {}
        MNT => {
            let path = r.opaque(1024).map_err(|_| Accept::GarbageArgs)?;
            match s.mount_path(path) {
                Some(fh) => {
                    // The handle, and the one flavor served: AUTH_SYS.
                    w.u32(MNT3_OK).opaque(&fh).u32(1).u32(1);
                }
                None => {
                    w.u32(MNT3ERR_ACCES);
                }
            }
        }
        UMNT => {
            r.opaque(1024).map_err(|_| Accept::GarbageArgs)?;
        }
        UMNTALL => {}
        DUMP | EXPORT => {
            w.bool(false);
        }
        _ => return Err(Accept::ProcUnavail),
    }
    Ok(w.into_vec())
}
