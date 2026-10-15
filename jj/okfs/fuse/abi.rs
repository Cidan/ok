//! The FUSE kernel protocol (`include/uapi/linux/fuse.h`, protocol 7.46): the opcodes,
//! flags, and the layouts of the structures okfs reads and writes, decoded and encoded by
//! hand in the machine's byte order (FUSE is always native-endian).

/// The protocol version this crate speaks.
pub const KERNEL_VERSION: u32 = 7;
pub const KERNEL_MINOR_VERSION: u32 = 46;
/// The oldest kernel minor version this crate accepts: 7.31 (Linux 5.4) has every feature
/// the session negotiates but `FUSE_HAS_RESEND`, which it uses only when offered.
pub const MIN_KERNEL_MINOR: u32 = 31;

pub const ROOT_ID: u64 = 1;

/// The file type bits of a mode, as FUSE's attributes and directory entries carry them: POSIX's
/// values, the same on every platform (libc's are `u16` on macOS).
pub const S_IFMT: u32 = 0o170000;
pub const S_IFSOCK: u32 = 0o140000;
pub const S_IFLNK: u32 = 0o120000;
pub const S_IFREG: u32 = 0o100000;
pub const S_IFDIR: u32 = 0o040000;
pub const S_IFIFO: u32 = 0o010000;

// Opcodes.
pub const LOOKUP: u32 = 1;
pub const FORGET: u32 = 2;
pub const GETATTR: u32 = 3;
pub const SETATTR: u32 = 4;
pub const READLINK: u32 = 5;
pub const SYMLINK: u32 = 6;
pub const MKNOD: u32 = 8;
pub const MKDIR: u32 = 9;
pub const UNLINK: u32 = 10;
pub const RMDIR: u32 = 11;
pub const RENAME: u32 = 12;
pub const LINK: u32 = 13;
pub const OPEN: u32 = 14;
pub const READ: u32 = 15;
pub const WRITE: u32 = 16;
pub const STATFS: u32 = 17;
pub const RELEASE: u32 = 18;
pub const FSYNC: u32 = 20;
pub const SETXATTR: u32 = 21;
pub const GETXATTR: u32 = 22;
pub const LISTXATTR: u32 = 23;
pub const REMOVEXATTR: u32 = 24;
pub const FLUSH: u32 = 25;
pub const INIT: u32 = 26;
pub const OPENDIR: u32 = 27;
pub const READDIR: u32 = 28;
pub const RELEASEDIR: u32 = 29;
pub const FSYNCDIR: u32 = 30;
pub const ACCESS: u32 = 34;
pub const CREATE: u32 = 35;
pub const INTERRUPT: u32 = 36;
pub const DESTROY: u32 = 38;
pub const BATCH_FORGET: u32 = 42;
pub const FALLOCATE: u32 = 43;
pub const READDIRPLUS: u32 = 44;
pub const RENAME2: u32 = 45;
pub const LSEEK: u32 = 46;

// Notification codes (sent with unique 0 and the code as the error).
pub const NOTIFY_INVAL_INODE: i32 = 2;
pub const NOTIFY_INVAL_ENTRY: i32 = 3;
pub const NOTIFY_DELETE: i32 = 6;
pub const NOTIFY_RESEND: i32 = 7;
pub const NOTIFY_INC_EPOCH: i32 = 8;

// INIT flags.
pub const ASYNC_READ: u64 = 1 << 0;
pub const ATOMIC_O_TRUNC: u64 = 1 << 3;
pub const BIG_WRITES: u64 = 1 << 5;
pub const AUTO_INVAL_DATA: u64 = 1 << 12;
pub const DO_READDIRPLUS: u64 = 1 << 13;
pub const READDIRPLUS_AUTO: u64 = 1 << 14;
pub const PARALLEL_DIROPS: u64 = 1 << 18;
pub const MAX_PAGES: u64 = 1 << 22;
pub const CACHE_SYMLINKS: u64 = 1 << 23;
pub const SETXATTR_EXT: u64 = 1 << 29;
pub const INIT_EXT: u64 = 1 << 30;
pub const HAS_RESEND: u64 = 1 << 39;

// open_out flags.
pub const FOPEN_DIRECT_IO: u32 = 1 << 0;
pub const FOPEN_KEEP_CACHE: u32 = 1 << 1;
pub const FOPEN_CACHE_DIR: u32 = 1 << 3;

// setattr_in.valid.
pub const FATTR_MODE: u32 = 1 << 0;
pub const FATTR_UID: u32 = 1 << 1;
pub const FATTR_GID: u32 = 1 << 2;
pub const FATTR_SIZE: u32 = 1 << 3;
pub const FATTR_ATIME: u32 = 1 << 4;
pub const FATTR_MTIME: u32 = 1 << 5;
pub const FATTR_FH: u32 = 1 << 6;
pub const FATTR_ATIME_NOW: u32 = 1 << 7;
pub const FATTR_MTIME_NOW: u32 = 1 << 8;
pub const FATTR_CTIME: u32 = 1 << 10;

pub const GETATTR_FH: u32 = 1 << 0;
pub const FSYNC_FDATASYNC: u32 = 1 << 0;

/// `FUSE_DEV_IOC_CLONE`: `_IOR(229, 0, uint32_t)`.
pub const DEV_IOC_CLONE: libc::c_ulong = (2 << 30) | (4 << 16) | (229 << 8);

pub const IN_HEADER_LEN: usize = 40;
pub const OUT_HEADER_LEN: usize = 16;
pub const ATTR_LEN: usize = 88;
pub const ENTRY_OUT_LEN: usize = 40 + ATTR_LEN;
pub const DIRENT_LEN: usize = 24;

/// A request's fixed header.
#[derive(Clone, Copy, Debug, Default)]
pub struct InHeader {
    pub len: u32,
    pub opcode: u32,
    pub unique: u64,
    pub nodeid: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    pub total_extlen: u16,
}

/// A cursor over a request's bytes.
pub struct Reader<'a> {
    buf: &'a [u8],
}

/// A request the kernel sent shorter than its opcode's arguments.
#[derive(Debug)]
pub struct Short;

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8], Short> {
        if self.buf.len() < n {
            return Err(Short);
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    pub fn u16(&mut self) -> Result<u16, Short> {
        Ok(u16::from_ne_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    pub fn u32(&mut self) -> Result<u32, Short> {
        Ok(u32::from_ne_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Result<u64, Short> {
        Ok(u64::from_ne_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    pub fn i64(&mut self) -> Result<i64, Short> {
        Ok(i64::from_ne_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    pub fn skip(&mut self, n: usize) -> Result<(), Short> {
        self.bytes(n).map(|_| ())
    }

    /// A NUL-terminated string, without its NUL.
    pub fn cstr(&mut self) -> Result<&'a [u8], Short> {
        let end = self.buf.iter().position(|&b| b == 0).ok_or(Short)?;
        let s = &self.buf[..end];
        self.buf = &self.buf[end + 1..];
        Ok(s)
    }

    pub fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.buf)
    }

    pub fn header(&mut self) -> Result<InHeader, Short> {
        let h = InHeader {
            len: self.u32()?,
            opcode: self.u32()?,
            unique: self.u64()?,
            nodeid: self.u64()?,
            uid: self.u32()?,
            gid: self.u32()?,
            pid: self.u32()?,
            total_extlen: self.u16()?,
        };
        self.skip(2)?;
        Ok(h)
    }
}

/// A reply being built.
#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            buf: Vec::with_capacity(n),
        }
    }

    pub fn u16(&mut self, v: u16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_ne_bytes());
        self
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_ne_bytes());
        self
    }

    pub fn i32(&mut self, v: i32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_ne_bytes());
        self
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_ne_bytes());
        self
    }

    pub fn i64(&mut self, v: i64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_ne_bytes());
        self
    }

    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(b);
        self
    }

    pub fn zeros(&mut self, n: usize) -> &mut Self {
        self.buf.resize(self.buf.len() + n, 0);
        self
    }

    /// Pads to a multiple of 8 bytes, as directory entries are.
    pub fn align8(&mut self) -> &mut Self {
        let pad = (8 - self.buf.len() % 8) % 8;
        self.zeros(pad)
    }
}

/// `struct fuse_attr`.
pub fn write_attr(w: &mut Writer, a: &crate::Attr) {
    w.u64(a.ino)
        .u64(a.size)
        .u64(a.size.div_ceil(512))
        .u64(a.atime.0 as u64)
        .u64(a.mtime.0 as u64)
        .u64(a.ctime.0 as u64)
        .u32(a.atime.1)
        .u32(a.mtime.1)
        .u32(a.ctime.1)
        .u32(a.mode)
        .u32(a.nlink)
        .u32(a.uid)
        .u32(a.gid)
        .u32(a.rdev)
        .u32(4096)
        .u32(0);
}

/// `struct fuse_entry_out`.
pub fn write_entry(w: &mut Writer, e: &crate::Entry) {
    let (ev, evn) = split_ttl(e.entry_ttl);
    let (av, avn) = split_ttl(e.attr_ttl);
    w.u64(e.attr.ino)
        .u64(e.generation)
        .u64(ev)
        .u64(av)
        .u32(evn)
        .u32(avn);
    write_attr(w, &e.attr);
}

/// `struct fuse_entry_out` for a name that does not exist, cached for ttl.
pub fn write_negative_entry(w: &mut Writer, ttl: std::time::Duration) {
    let (ev, evn) = split_ttl(ttl);
    w.u64(0)
        .u64(0)
        .u64(ev)
        .u64(0)
        .u32(evn)
        .u32(0)
        .zeros(ATTR_LEN);
}

pub fn split_ttl(d: std::time::Duration) -> (u64, u32) {
    (d.as_secs(), d.subsec_nanos())
}
