//! POSIX ACLs as okfs keeps them, each rule against what the kernel's own file systems do
//! (`fs/posix_acl.c`): parsing, setting an access or default ACL, a chmod, inheritance, and the
//! reconciling of ACLs once kept verbatim.

use std::ffi::OsStr;

use crate::acl;
use crate::acl::Acl;
use crate::acl::Xattrs;

const USER_OBJ: u16 = 0x01;
const USER: u16 = 0x02;
const GROUP_OBJ: u16 = 0x04;
const GROUP: u16 = 0x08;
const MASK: u16 = 0x10;
const OTHER: u16 = 0x20;
const NO_ID: u32 = u32::MAX;
/// The named user of [`extended`]: uid 0, which a test's user namespace maps.
const NAMED: u32 = 0;

/// An ACL's xattr value from its entries: tag, permissions, and id.
pub fn acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut out = 2u32.to_le_bytes().to_vec();
    for (tag, perm, id) in entries {
        out.extend_from_slice(&tag.to_le_bytes());
        out.extend_from_slice(&perm.to_le_bytes());
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

/// An ACL the mode can say: the owner's, the owning group's, and others' permissions.
pub fn minimal(user: u16, group: u16, other: u16) -> Vec<u8> {
    acl(&[
        (USER_OBJ, user, NO_ID),
        (GROUP_OBJ, group, NO_ID),
        (OTHER, other, NO_ID),
    ])
}

/// An ACL the mode cannot say: a named user's entry, and the mask.
pub fn extended(user: u16, named: u16, group: u16, mask: u16, other: u16) -> Vec<u8> {
    acl(&[
        (USER_OBJ, user, NO_ID),
        (USER, named, NAMED),
        (GROUP_OBJ, group, NO_ID),
        (MASK, mask, NO_ID),
        (OTHER, other, NO_ID),
    ])
}

fn access(x: &Xattrs) -> Option<&Vec<u8>> {
    x.get(OsStr::new(acl::ACCESS))
}

fn default(x: &Xattrs) -> Option<&Vec<u8>> {
    x.get(OsStr::new(acl::DEFAULT))
}

// A value parses as the kernel parses it, and is refused where the kernel refuses it.
#[test]
fn parses_as_the_kernel_does() {
    let e = extended(6, 4, 4, 5, 0);
    assert_eq!(Acl::parse(&e).unwrap().unwrap().to_bytes(), e);
    let groups = acl(&[
        (USER_OBJ, 7, NO_ID),
        (GROUP_OBJ, 5, NO_ID),
        (GROUP, 7, 100),
        (MASK, 7, NO_ID),
        (OTHER, 0, NO_ID),
    ]);
    assert!(Acl::parse(&groups).unwrap().is_some());
    assert_eq!(Acl::parse(&acl(&[])), Ok(None), "no entries is no ACL");
    assert_eq!(Acl::parse(&[2, 0]), Err(libc::EINVAL), "a short header");
    let mut v1 = minimal(6, 4, 4);
    v1[0] = 1;
    assert_eq!(Acl::parse(&v1), Err(libc::EOPNOTSUPP), "another version");
    assert_eq!(
        Acl::parse(&minimal(6, 4, 4)[..10]),
        Err(libc::EINVAL),
        "a partial entry"
    );
    for (why, entries) in [
        (
            "out of order",
            vec![
                (GROUP_OBJ, 4, NO_ID),
                (USER_OBJ, 6, NO_ID),
                (OTHER, 4, NO_ID),
            ],
        ),
        (
            "a named user and no mask",
            vec![
                (USER_OBJ, 6, NO_ID),
                (USER, 4, NAMED),
                (GROUP_OBJ, 4, NO_ID),
                (OTHER, 4, NO_ID),
            ],
        ),
        (
            "a named user with no id",
            vec![
                (USER_OBJ, 6, NO_ID),
                (USER, 4, NO_ID),
                (GROUP_OBJ, 4, NO_ID),
                (MASK, 4, NO_ID),
                (OTHER, 4, NO_ID),
            ],
        ),
        (
            "more than read, write, and execute",
            vec![
                (USER_OBJ, 0o17, NO_ID),
                (GROUP_OBJ, 4, NO_ID),
                (OTHER, 4, NO_ID),
            ],
        ),
        (
            "no entry for others",
            vec![(USER_OBJ, 6, NO_ID), (GROUP_OBJ, 4, NO_ID)],
        ),
        (
            "an entry after others'",
            vec![
                (USER_OBJ, 6, NO_ID),
                (GROUP_OBJ, 4, NO_ID),
                (OTHER, 4, NO_ID),
                (OTHER, 4, NO_ID),
            ],
        ),
    ] {
        assert_eq!(Acl::parse(&acl(&entries)), Err(libc::EINVAL), "{why}");
    }
}

// Setting an access ACL sets the mode, and the ACL is kept only where it says more than the mode:
// setfacl's minimal ACL, or `setfacl -b`, leaves none.
#[test]
fn an_access_acl_sets_the_mode() {
    let mut x = Xattrs::new();
    assert_eq!(
        acl::set_access(&mut x, 0o644, &minimal(7, 5, 0), false),
        Ok(0o750)
    );
    assert_eq!(access(&x), None);
    let e = extended(6, 4, 4, 5, 0);
    assert_eq!(acl::set_access(&mut x, 0o750, &e, false), Ok(0o650));
    assert_eq!(access(&x), Some(&e));
    assert_eq!(
        acl::set_access(&mut x, 0o650, &minimal(6, 4, 0), false),
        Ok(0o640)
    );
    assert_eq!(access(&x), None);
    // The setgid bit stays, unless the setter may not keep it.
    assert_eq!(
        acl::set_access(&mut x, 0o2644, &minimal(6, 4, 4), false),
        Ok(0o2644)
    );
    assert_eq!(
        acl::set_access(&mut x, 0o2644, &minimal(6, 4, 4), true),
        Ok(0o644)
    );
    // An empty ACL removes it, the mode as it was.
    acl::set_access(&mut x, 0o650, &e, false).unwrap();
    assert_eq!(acl::set_access(&mut x, 0o650, &acl(&[]), true), Ok(0o650));
    assert_eq!(access(&x), None);
    assert_eq!(
        acl::set_access(&mut x, 0o644, b"junk", false),
        Err(libc::EOPNOTSUPP)
    );
}

// A chmod changes the access ACL: the owner's and others' entries, and the mask; named entries,
// and the owning group's under a mask, stay.
#[test]
fn a_chmod_changes_the_access_acl() {
    let mut x = Xattrs::new();
    x.insert(acl::ACCESS.into(), extended(6, 4, 4, 5, 0));
    acl::chmod(&mut x, 0o751);
    assert_eq!(access(&x), Some(&extended(7, 4, 4, 5, 1)));
    // Without a mask, the owning group's entry takes the group's bits.
    x.insert(acl::ACCESS.into(), minimal(6, 4, 4));
    acl::chmod(&mut x, 0o750);
    assert_eq!(access(&x), Some(&minimal(7, 5, 0)));
    let mut none = Xattrs::new();
    acl::chmod(&mut none, 0o755);
    assert!(none.is_empty());
}

// What is made under a default ACL inherits it, limited by the mode it was made with, and the
// umask is not applied; a directory keeps the default ACL as its own. Elsewhere the umask is.
#[test]
fn inherits_a_default_acl() {
    let mut parent = Xattrs::new();
    assert_eq!(
        acl::inherit(&parent, false, 0o666, 0o022),
        (0o644, Xattrs::new())
    );
    let d = extended(7, 7, 5, 7, 5);
    parent.insert(acl::DEFAULT.into(), d.clone());
    let (mode, x) = acl::inherit(&parent, false, 0o666, 0o077);
    assert_eq!(mode, 0o664);
    assert_eq!(access(&x), Some(&extended(6, 7, 5, 6, 4)));
    assert_eq!(default(&x), None, "a file has no default ACL");
    let (mode, x) = acl::inherit(&parent, true, 0o777, 0o077);
    assert_eq!(mode, 0o775);
    assert_eq!(access(&x), Some(&d));
    assert_eq!(default(&x), Some(&d), "a directory keeps it");
    // A default ACL the mode can say: no access ACL, and still no umask.
    parent.insert(acl::DEFAULT.into(), minimal(7, 7, 7));
    let (mode, x) = acl::inherit(&parent, false, 0o666, 0o077);
    assert_eq!((mode, access(&x)), (0o666, None));
}

// A default ACL is a directory's alone.
#[test]
fn a_default_acl_is_a_directorys() {
    let mut x = Xattrs::new();
    assert_eq!(
        acl::set_default(&mut x, false, &minimal(7, 5, 5)),
        Err(libc::EACCES)
    );
    assert_eq!(acl::set_default(&mut x, false, &acl(&[])), Ok(()));
    assert_eq!(acl::set_default(&mut x, true, &minimal(7, 5, 5)), Ok(()));
    assert_eq!(default(&x), Some(&minimal(7, 5, 5)));
    assert_eq!(acl::set_default(&mut x, true, &acl(&[])), Ok(()));
    assert_eq!(default(&x), None);
}

// ACLs once kept verbatim are made what a disk file system would hold under the mode the inode
// shows: a minimal one goes, an extended one follows the mode, and one that does not parse, or a
// file's default ACL, goes. Other xattrs stay.
#[test]
fn reconciles_acls_kept_verbatim() {
    let mut x = Xattrs::new();
    // What a writer set before a chmod +x it did not follow.
    x.insert(acl::ACCESS.into(), minimal(6, 4, 4));
    acl::reconcile(&mut x, 0o755, false);
    assert_eq!(access(&x), None);
    x.insert(acl::ACCESS.into(), extended(6, 4, 4, 4, 4));
    acl::reconcile(&mut x, 0o750, false);
    assert_eq!(access(&x), Some(&extended(7, 4, 4, 5, 0)));
    x.insert(acl::ACCESS.into(), b"junk".to_vec());
    x.insert(acl::DEFAULT.into(), minimal(7, 5, 5));
    x.insert("user.other".into(), b"kept".to_vec());
    acl::reconcile(&mut x, 0o644, false);
    assert_eq!(x.keys().collect::<Vec<_>>(), ["user.other"]);
    let mut d = Xattrs::new();
    d.insert(acl::DEFAULT.into(), minimal(7, 5, 5));
    acl::reconcile(&mut d, 0o755, true);
    assert_eq!(default(&d), Some(&minimal(7, 5, 5)));
}
