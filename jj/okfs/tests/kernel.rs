//! A mount against the kernel: the model served by the FUSE layer from the fixture's trees, in a
//! user and mount namespace of the test's own, driven by ordinary file-system calls. The binary
//! enters the namespace first thing in its `main` (okfs_fuse::testns), so it runs on a harness of
//! its own, and reaches okfs's test fixtures through the `testkit` feature.
#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;

use okfs::acl;
use okfs::fs::Mount;
use okfs::model::resolved_terms;
use okfs::testfix::Entry;
use okfs::testfix::Fixture;
use okfs::testfix::FixtureFetch;
use okfs::testfix::base_tree;
use okfs_fuse as fuse;

/// The model served through the kernel: reads, prefetching, writes of every kind, and a checkout
/// under the kernel's caches.
#[cfg(target_os = "linux")]
fn kernel() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let big: Vec<u8> = (0..5_000_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect();
    let with_big = fx.tree(vec![
        ("README", Entry::File(b"readme\n")),
        ("big.bin", Entry::File(&big)),
    ]);
    let (model, _log) = fx.model(&base);
    let dir = fx.dir.as_ref().unwrap().path().to_owned();
    let mnt = dir.join("mnt");
    fs::create_dir(&mnt).unwrap();
    let fx = Arc::new(fx);
    let prefetched = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mount = Arc::new(
        Mount::new(
            model,
            Arc::new(FixtureFetch(fx.clone(), prefetched.clone())),
            dir.join("staging"),
            Box::new(|| {}),
        )
        .unwrap(),
    );
    let fd = fuse::mount(&mnt, &fuse::MountOptions::default()).expect("mount");
    let session = fuse::Session::new(fd);
    let server = {
        let mount = mount.clone();
        std::thread::spawn(move || session.run(mount, 4, |_| {}))
    };

    // The base, read through the kernel.
    let mut names: Vec<String> = fs::read_dir(&mnt)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, [".jj", "README", "docs", "link", "run.sh", "src"]);
    assert_eq!(fs::read(mnt.join("src/deep/c.rs")).unwrap(), b"fn c() {}\n");
    assert_eq!(fs::metadata(mnt.join("src/a.rs")).unwrap().len(), 10);
    assert_eq!(
        fs::metadata(mnt.join("run.sh")).unwrap().mode() & 0o777,
        0o755
    );
    assert_eq!(
        fs::read_link(mnt.join("link")).unwrap(),
        std::path::Path::new("README")
    );
    assert_eq!(
        fs::read_link(mnt.join(".jj")).unwrap(),
        std::path::Path::new("/state/jj")
    );
    assert!(!mnt.join("nope").exists());

    // Reading a third file of a directory soon after two others prefetches its small files.
    assert!(prefetched.lock().unwrap().is_empty());
    let mut names: Vec<String> = fs::read_dir(mnt.join("src"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["a.rs", "b.rs", "deep"]);
    fs::read(mnt.join("src/a.rs")).unwrap();
    fs::read(mnt.join("src/b.rs")).unwrap();
    assert!(
        prefetched.lock().unwrap().is_empty(),
        "two opens are not a pattern"
    );
    fs::read(mnt.join("src/a.rs")).unwrap();
    assert_eq!(
        prefetched.lock().unwrap().len(),
        1,
        "the third open prefetches src's other file"
    );

    // Writing: an edit, a new file, a directory, a rename, a deletion, a hard link, a chmod.
    fs::write(mnt.join("src/a.rs"), b"fn a2() {}\n").unwrap();
    assert_eq!(fs::read(mnt.join("src/a.rs")).unwrap(), b"fn a2() {}\n");
    fs::create_dir(mnt.join("new")).unwrap();
    fs::write(mnt.join("new/file"), b"hello\n").unwrap();
    fs::rename(mnt.join("src/b.rs"), mnt.join("new/b.rs")).unwrap();
    assert_eq!(fs::read(mnt.join("new/b.rs")).unwrap(), b"fn b() {}\n");
    assert!(!mnt.join("src/b.rs").exists());
    fs::remove_file(mnt.join("docs/guide.md")).unwrap();
    fs::remove_dir(mnt.join("docs")).unwrap();
    fs::hard_link(mnt.join("new/file"), mnt.join("new/again")).unwrap();
    assert_eq!(fs::metadata(mnt.join("new/again")).unwrap().nlink(), 2);
    fs::set_permissions(mnt.join("README"), fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        fs::metadata(mnt.join("README")).unwrap().mode() & 0o777,
        0o600
    );
    std::os::unix::fs::symlink("new/file", mnt.join("ln")).unwrap();
    assert_eq!(fs::read(mnt.join("ln")).unwrap(), b"hello\n");
    let mut names: Vec<String> = fs::read_dir(&mnt)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(
        names,
        [".jj", "README", "link", "ln", "new", "run.sh", "src"]
    );
    // .jj is not ours to change.
    assert!(fs::remove_file(mnt.join(".jj")).is_err());

    // ACLs, through the kernel. In a user namespace the kernel refuses ACLs from a file system
    // that did not say it keeps them (`fuse_no_acl`), so each call working is FUSE_POSIX_ACL.
    let acls = mnt.join("acl");
    let f = acls.join("f");
    fs::create_dir(&acls).unwrap();
    fs::write(&f, b"f\n").unwrap();
    set_xattr(&f, acl::ACCESS, &minimal(7, 5, 0)).unwrap();
    assert_eq!(mode(&f), 0o750);
    assert_eq!(get_xattr(&f, acl::ACCESS), Err(libc::ENODATA));
    let e = extended(6, 4, 4, 4, 0);
    set_xattr(&f, acl::ACCESS, &e).unwrap();
    assert_eq!(mode(&f), 0o640);
    assert_eq!(get_xattr(&f, acl::ACCESS), Ok(e));
    fs::set_permissions(&f, fs::Permissions::from_mode(0o750)).unwrap();
    assert_eq!(get_xattr(&f, acl::ACCESS), Ok(extended(7, 4, 4, 5, 0)));
    assert_eq!(
        set_xattr(&f, acl::DEFAULT, &minimal(7, 5, 5)),
        Err(libc::EACCES)
    );
    // Under a default ACL, a new file and directory inherit it and the umask does not apply
    // (FUSE_DONT_MASK: the kernel left it to okfs); elsewhere it does.
    let d = extended(7, 7, 5, 7, 5);
    set_xattr(&acls, acl::DEFAULT, &d).unwrap();
    let umask = unsafe { libc::umask(0o077) };
    fs::write(acls.join("g"), b"g\n").unwrap();
    fs::create_dir(acls.join("sub")).unwrap();
    fs::write(mnt.join("new/umasked"), b"u\n").unwrap();
    unsafe { libc::umask(umask) };
    assert_eq!(mode(&acls.join("g")), 0o664);
    assert_eq!(
        get_xattr(&acls.join("g"), acl::ACCESS),
        Ok(extended(6, 7, 5, 6, 4))
    );
    assert_eq!(mode(&acls.join("sub")), 0o775);
    assert_eq!(get_xattr(&acls.join("sub"), acl::DEFAULT), Ok(d));
    assert_eq!(mode(&mnt.join("new/umasked")), 0o600);

    // A checkout under the kernel's caches: the new tree's files appear, the old ones' cached
    // pages and entries drop. Nothing was snapshotted, so the recorded tree is still the base.
    let readme = fs::read(mnt.join("README")).unwrap();
    assert_eq!(readme, b"readme\n");
    let recorded = mount.model().recorded().0.clone();
    let (_, stale) = mount
        .with(|m| m.check_out(recorded.clone(), resolved_terms(with_big.clone()), &[]))
        .unwrap();
    mount.invalidate(&stale);
    assert!(!mnt.join("run.sh").exists());
    assert!(!mnt.join("src/a.rs").exists());
    assert_eq!(
        fs::metadata(mnt.join("big.bin")).unwrap().len(),
        big.len() as u64
    );
    // A large file read through the kernel, by ranges from ok.
    assert_eq!(fs::read(mnt.join("big.bin")).unwrap(), big);
    // What was written and not recorded stays.
    assert_eq!(fs::read(mnt.join("new/file")).unwrap(), b"hello\n");

    fuse::unmount(&mnt).unwrap();
    server.join().unwrap().unwrap();
}

/// A source whose file sizes cannot be read while `down` is set: ok, unreachable.
struct Outage {
    inner: FixtureFetch,
    down: std::sync::atomic::AtomicBool,
}

impl okfs::fs::Fetch for Outage {
    fn tree(&self, id: &jj_lib::backend::TreeId) -> std::io::Result<Arc<jj_lib::backend::Tree>> {
        self.inner.tree(id)
    }
    fn sizes(
        &self,
        ids: &[jj_lib::backend::FileId],
    ) -> std::io::Result<std::collections::HashMap<jj_lib::backend::FileId, u64>> {
        if self.down.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }
        self.inner.sizes(ids)
    }
    fn symlink(&self, id: &jj_lib::backend::SymlinkId) -> std::io::Result<Vec<u8>> {
        self.inner.symlink(id)
    }
    fn content(&self, id: &jj_lib::backend::FileId, to: &std::path::Path) -> std::io::Result<()> {
        self.inner.content(id, to)
    }
    fn read(
        &self,
        id: &jj_lib::backend::FileId,
        size: u64,
        offset: u64,
        len: u64,
    ) -> std::io::Result<Vec<u8>> {
        self.inner.read(id, size, offset, len)
    }
}

/// A directory listed while its files cannot be read fails, and never lists without them: the
/// kernel keeps a listing for as long as the directory lives, so one with a file missing would
/// stay that way after ok came back.
fn listing_failure() {
    let mut fx = Fixture::new();
    let base = fx.tree(vec![(
        "pkg",
        Entry::Dir(vec![
            ("a.go", Entry::File(b"package a\n")),
            ("build.yaml", Entry::File(b"name: a\n")),
        ]),
    )]);
    let (model, _log) = fx.model(&base);
    let dir = fx.dir.as_ref().unwrap().path().to_owned();
    let mnt = dir.join("mnt");
    fs::create_dir(&mnt).unwrap();
    let fx = Arc::new(fx);
    let source = Arc::new(Outage {
        inner: FixtureFetch(fx.clone(), Arc::new(std::sync::Mutex::new(Vec::new()))),
        down: std::sync::atomic::AtomicBool::new(false),
    });
    let mount =
        Arc::new(Mount::new(model, source.clone(), dir.join("staging"), Box::new(|| {})).unwrap());
    let fd = fuse::mount(&mnt, &fuse::MountOptions::default()).expect("mount");
    let session = fuse::Session::new(fd);
    let server = {
        let mount = mount.clone();
        std::thread::spawn(move || session.run(mount, 4, |_| {}))
    };

    let listing = |p: &std::path::Path| -> std::io::Result<Vec<String>> {
        let mut names = Vec::new();
        for e in fs::read_dir(p)? {
            names.push(e?.file_name().into_string().unwrap());
        }
        names.sort();
        Ok(names)
    };
    // One file's size is read; then ok goes down, so the other's cannot be. Entries after a
    // failure must not be listed past it: the kernel would never ask for it again.
    assert_eq!(fs::metadata(mnt.join("pkg/build.yaml")).unwrap().len(), 8);
    source.down.store(true, std::sync::atomic::Ordering::SeqCst);
    // ok is down: the listing fails rather than leave a file out.
    match listing(&mnt.join("pkg")) {
        Err(e) => assert_eq!(e.raw_os_error(), Some(libc::EIO), "{e}"),
        Ok(names) => assert_eq!(names, ["a.go", "build.yaml"], "a listing while ok is down"),
    }
    // ok is back: the whole listing, though the kernel kept what it had.
    source
        .down
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(listing(&mnt.join("pkg")).unwrap(), ["a.go", "build.yaml"]);
    assert_eq!(fs::read(mnt.join("pkg/a.go")).unwrap(), b"package a\n");

    fuse::unmount(&mnt).unwrap();
    server.join().unwrap().unwrap();
}

/// An ACL's xattr value from its entries: tag, permissions, and id.
fn acl_value(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut out = 2u32.to_le_bytes().to_vec();
    for (tag, perm, id) in entries {
        out.extend_from_slice(&tag.to_le_bytes());
        out.extend_from_slice(&perm.to_le_bytes());
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

/// An ACL the mode can say: the owner's, the owning group's, and others' permissions.
fn minimal(user: u16, group: u16, other: u16) -> Vec<u8> {
    acl_value(&[
        (0x01, user, u32::MAX),
        (0x04, group, u32::MAX),
        (0x20, other, u32::MAX),
    ])
}

/// An ACL the mode cannot say: uid 0's entry (the namespace's), and the mask.
fn extended(user: u16, named: u16, group: u16, mask: u16, other: u16) -> Vec<u8> {
    acl_value(&[
        (0x01, user, u32::MAX),
        (0x02, named, 0),
        (0x04, group, u32::MAX),
        (0x10, mask, u32::MAX),
        (0x20, other, u32::MAX),
    ])
}

fn mode(p: &Path) -> u32 {
    fs::metadata(p).unwrap().mode() & 0o7777
}

#[cfg(target_os = "linux")]
fn c_path(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).unwrap()
}

#[cfg(target_os = "linux")]
fn set_xattr(p: &Path, name: &str, value: &[u8]) -> Result<(), i32> {
    let (p, name) = (c_path(p), CString::new(name).unwrap());
    let r = unsafe {
        libc::setxattr(
            p.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    };
    if r == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().raw_os_error().unwrap())
    }
}

#[cfg(target_os = "linux")]
fn get_xattr(p: &Path, name: &str) -> Result<Vec<u8>, i32> {
    let (p, name) = (c_path(p), CString::new(name).unwrap());
    let mut buf = vec![0u8; 4096];
    let n = unsafe {
        libc::getxattr(
            p.as_ptr(),
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            buf.len(),
        )
    };
    if n < 0 {
        return Err(std::io::Error::last_os_error().raw_os_error().unwrap());
    }
    buf.truncate(n as usize);
    Ok(buf)
}

#[cfg(target_os = "linux")]
fn main() {
    fuse::testns::enter().expect("enter a user and mount namespace of the test's own");
    assert!(
        std::path::Path::new("/dev/fuse").exists(),
        "no /dev/fuse: the test target declares devices: [fuse] (docs/building.md, Tests)"
    );
    fuse::testns::run(&[("kernel", kernel), ("listing_failure", listing_failure)]);
}

#[cfg(not(target_os = "linux"))]
fn main() {}
