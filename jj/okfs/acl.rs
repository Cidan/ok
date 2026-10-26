//! POSIX.1e ACLs as Linux keeps them in extended attributes (`system.posix_acl_access` and
//! `system.posix_acl_default`, `include/uapi/linux/posix_acl_xattr.h`), and the rules a file system
//! that keeps them follows, after the kernel's own (`fs/posix_acl.c`): setting an access ACL sets
//! the mode, a chmod changes the access ACL, and what is made in a directory with a default ACL
//! inherits it. okfs keeps ACLs so, and over FUSE the kernel enforces them (`FUSE_POSIX_ACL`).

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::ffi::OsString;

/// An inode's extended attributes, by name.
pub type Xattrs = BTreeMap<OsString, Vec<u8>>;

pub const ACCESS: &str = "system.posix_acl_access";
pub const DEFAULT: &str = "system.posix_acl_default";

const VERSION: u32 = 2;
const HEADER_LEN: usize = 4;
const ENTRY_LEN: usize = 8;
const UNDEFINED_ID: u32 = u32::MAX;

const USER_OBJ: u16 = 0x01;
const USER: u16 = 0x02;
const GROUP_OBJ: u16 = 0x04;
const GROUP: u16 = 0x08;
const MASK: u16 = 0x10;
const OTHER: u16 = 0x20;

const RWX: u16 = 0o7;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    tag: u16,
    perm: u16,
    id: u32,
}

/// A valid ACL: the owner's entry, named users', the owning group's, named groups', the mask,
/// and others', in that order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acl(Vec<Entry>);

impl Acl {
    /// An ACL from an xattr's value: None for one with no entries, which is no ACL. An error is
    /// the errno the kernel gives such a value (`posix_acl_from_xattr`, `posix_acl_valid`).
    pub fn parse(value: &[u8]) -> Result<Option<Acl>, i32> {
        if value.len() < HEADER_LEN {
            return Err(libc::EINVAL);
        }
        if u32::from_le_bytes(value[..HEADER_LEN].try_into().unwrap()) != VERSION {
            return Err(libc::EOPNOTSUPP);
        }
        let body = &value[HEADER_LEN..];
        if !body.len().is_multiple_of(ENTRY_LEN) {
            return Err(libc::EINVAL);
        }
        if body.is_empty() {
            return Ok(None);
        }
        let entries: Vec<Entry> = body
            .as_chunks::<ENTRY_LEN>()
            .0
            .iter()
            .map(|e| Entry {
                tag: u16::from_le_bytes([e[0], e[1]]),
                perm: u16::from_le_bytes([e[2], e[3]]),
                id: u32::from_le_bytes([e[4], e[5], e[6], e[7]]),
            })
            .collect();
        valid(&entries)?;
        Ok(Some(Acl(entries)))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + ENTRY_LEN * self.0.len());
        out.extend_from_slice(&VERSION.to_le_bytes());
        for e in &self.0 {
            out.extend_from_slice(&e.tag.to_le_bytes());
            out.extend_from_slice(&e.perm.to_le_bytes());
            out.extend_from_slice(&e.id.to_le_bytes());
        }
        out
    }

    /// The permission bits the ACL grants, and whether it grants more than they can say (named
    /// users or groups, under a mask): `posix_acl_equiv_mode`.
    fn mode(&self) -> (u32, bool) {
        let mut mode = 0;
        let mut more = false;
        for e in &self.0 {
            let perm = u32::from(e.perm & RWX);
            match e.tag {
                USER_OBJ => mode |= perm << 6,
                GROUP_OBJ => mode |= perm << 3,
                OTHER => mode |= perm,
                MASK => {
                    mode = (mode & !0o070) | perm << 3;
                    more = true;
                }
                _ => more = true,
            }
        }
        (mode, more)
    }

    /// The ACL after a chmod to `mode` (`__posix_acl_chmod_masq`): the owner's and others'
    /// entries take the mode's bits, and so does the mask, or the owning group's entry where
    /// there is no mask.
    fn chmod(&mut self, mode: u32) {
        let mut group = None;
        let mut mask = None;
        for (i, e) in self.0.iter_mut().enumerate() {
            match e.tag {
                USER_OBJ => e.perm = (mode >> 6) as u16 & RWX,
                GROUP_OBJ => group = Some(i),
                MASK => mask = Some(i),
                OTHER => e.perm = mode as u16 & RWX,
                _ => {}
            }
        }
        if let Some(i) = mask.or(group) {
            self.0[i].perm = (mode >> 3) as u16 & RWX;
        }
    }

    /// The ACL an inode made with `mode` inherits from this default ACL, and the inode's mode
    /// (`posix_acl_create_masq`): each of the owner's, the group's (the mask's, where there is
    /// one), and others' entries limited by the mode's bits, and those bits by the entries. Also
    /// whether the ACL grants more than the mode can say.
    fn create(&mut self, mode: u32) -> (u32, bool) {
        let mut m = mode;
        let mut more = false;
        let mut group = None;
        let mut mask = None;
        for (i, e) in self.0.iter_mut().enumerate() {
            match e.tag {
                USER_OBJ => {
                    e.perm &= (m >> 6) as u16 & RWX;
                    m &= u32::from(e.perm) << 6 | !0o700;
                }
                GROUP_OBJ => group = Some(i),
                OTHER => {
                    e.perm &= m as u16 & RWX;
                    m &= u32::from(e.perm) | !0o007;
                }
                MASK => {
                    mask = Some(i);
                    more = true;
                }
                _ => more = true,
            }
        }
        if let Some(i) = mask.or(group) {
            let e = &mut self.0[i];
            e.perm &= (m >> 3) as u16 & RWX;
            m &= u32::from(e.perm) << 3 | !0o070;
        }
        (m, more)
    }
}

/// Whether entries make an ACL the kernel accepts (`posix_acl_valid`): permissions only read,
/// write, and execute; the owner's, the owning group's, and others' entries each once, in order,
/// with named users after the owner's and named groups after the group's; and a mask, before
/// others', exactly when there is a named entry.
fn valid(entries: &[Entry]) -> Result<(), i32> {
    #[derive(PartialEq)]
    enum State {
        UserObj,
        User,
        Group,
        Other,
        Done,
    }
    let mut state = State::UserObj;
    let mut needs_mask = false;
    for e in entries {
        if e.perm & !RWX != 0 {
            return Err(libc::EINVAL);
        }
        match e.tag {
            USER_OBJ if state == State::UserObj => state = State::User,
            USER if state == State::User && e.id != UNDEFINED_ID => needs_mask = true,
            GROUP_OBJ if state == State::User => state = State::Group,
            GROUP if state == State::Group && e.id != UNDEFINED_ID => needs_mask = true,
            MASK if state == State::Group => state = State::Other,
            OTHER if state == State::Other || (state == State::Group && !needs_mask) => {
                state = State::Done
            }
            _ => return Err(libc::EINVAL),
        }
    }
    if state == State::Done {
        Ok(())
    } else {
        Err(libc::EINVAL)
    }
}

fn parsed(xattrs: &Xattrs, name: &str) -> Option<Result<Option<Acl>, i32>> {
    xattrs.get(OsStr::new(name)).map(|v| Acl::parse(v))
}

/// Sets an inode's access ACL, `value` already parsed by [`Acl::parse`], as the kernel has a file
/// system do (`posix_acl_update_mode`): the inode's permissions become what the ACL grants, the
/// ACL is kept only where it grants more than they say, and `kill_sgid` clears the setgid bit.
/// An empty ACL removes it, the mode unchanged. Returns the inode's new mode.
pub fn set_access(
    xattrs: &mut Xattrs,
    mode: u32,
    value: &[u8],
    kill_sgid: bool,
) -> Result<u32, i32> {
    let Some(acl) = Acl::parse(value)? else {
        xattrs.remove(OsStr::new(ACCESS));
        return Ok(mode);
    };
    let (bits, more) = acl.mode();
    if more {
        xattrs.insert(ACCESS.into(), acl.to_bytes());
    } else {
        xattrs.remove(OsStr::new(ACCESS));
    }
    let mut mode = (mode & !0o777) | bits;
    if kill_sgid {
        mode &= !libc::S_ISGID;
    }
    Ok(mode)
}

/// Sets a directory's default ACL; an empty one removes it. Anything else may hold no default
/// ACL (`set_posix_acl`).
pub fn set_default(xattrs: &mut Xattrs, dir: bool, value: &[u8]) -> Result<(), i32> {
    match Acl::parse(value)? {
        Some(_) if !dir => Err(libc::EACCES),
        Some(acl) => {
            xattrs.insert(DEFAULT.into(), acl.to_bytes());
            Ok(())
        }
        None => {
            xattrs.remove(OsStr::new(DEFAULT));
            Ok(())
        }
    }
}

/// Keeps an inode's access ACL in step with a chmod to `mode` (`posix_acl_chmod`).
pub fn chmod(xattrs: &mut Xattrs, mode: u32) {
    if let Some(Ok(Some(mut acl))) = parsed(xattrs, ACCESS) {
        acl.chmod(mode);
        xattrs.insert(ACCESS.into(), acl.to_bytes());
    }
}

/// The permissions and ACLs of an inode made with `mode` and `umask` in a directory with
/// `parent`'s xattrs (`posix_acl_create`): under a default ACL, what it inherits from that ACL,
/// the umask not applied, and a directory keeps the default ACL as its own; elsewhere, the mode
/// less the umask, and no ACL.
pub fn inherit(parent: &Xattrs, dir: bool, mode: u32, umask: u32) -> (u32, Xattrs) {
    let mut xattrs = Xattrs::new();
    let Some(Ok(Some(default))) = parsed(parent, DEFAULT) else {
        return (mode & !umask, xattrs);
    };
    let mut access = default.clone();
    let (mode, more) = access.create(mode);
    if more {
        xattrs.insert(ACCESS.into(), access.to_bytes());
    }
    if dir {
        xattrs.insert(DEFAULT.into(), default.to_bytes());
    }
    (mode, xattrs)
}

/// An inode's ACLs as a disk file system holds them with the permissions in `mode`: an access ACL
/// in step with the mode, kept only where it grants more; a default ACL only on a directory; and
/// none that does not parse. okfs once kept ACLs verbatim, as any other xattr, and each one so
/// kept is made this when a model loads: the mode, which a chmod after the ACL changed and the
/// ACL did not follow, is what the inode has shown since.
pub fn reconcile(xattrs: &mut Xattrs, mode: u32, dir: bool) {
    match parsed(xattrs, ACCESS) {
        Some(Ok(Some(mut acl))) => {
            acl.chmod(mode);
            if acl.mode().1 {
                xattrs.insert(ACCESS.into(), acl.to_bytes());
            } else {
                xattrs.remove(OsStr::new(ACCESS));
            }
        }
        Some(_) => {
            xattrs.remove(OsStr::new(ACCESS));
        }
        None => {}
    }
    match parsed(xattrs, DEFAULT) {
        Some(Ok(Some(_))) if dir => {}
        Some(_) => {
            xattrs.remove(OsStr::new(DEFAULT));
        }
        None => {}
    }
}
