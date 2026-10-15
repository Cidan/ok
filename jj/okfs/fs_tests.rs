//! A mount against the kernel: the model served by the FUSE layer from the fixture's trees, in a
//! user and mount namespace of the test's own, driven by ordinary file-system calls.

use std::fs;
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::process::Command;
use std::sync::Arc;

use okfs_fuse as fuse;

use crate::fs::Mount;
use crate::model::resolved_terms;
use crate::model_tests::base_tree;
use crate::testfix::Entry;
use crate::testfix::Fixture;
use crate::testfix::FixtureFetch;

const CHILD: &str = "OKFS_MOUNT_KERNEL_TEST";

#[test]
fn kernel() {
    assert!(
        std::path::Path::new("/dev/fuse").exists(),
        "no /dev/fuse: the test target declares devices: [fuse] (docs/building.md, Tests)"
    );
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([
        "--exact",
        "fs_tests::kernel_child",
        "--nocapture",
        "--test-threads=1",
    ])
    .env(CHILD, "1");
    fuse::testns::unshare(&mut cmd);
    let out = cmd.output().expect("run the child");
    assert!(
        out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("okfs-mount-scenario-done"),
        "the child: {}\n{}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn kernel_child() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
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
    println!("okfs-mount-scenario-done");
}

const LISTING_CHILD: &str = "OKFS_LISTING_FAILURE_TEST";

/// A source whose file sizes cannot be read while `down` is set: ok, unreachable.
struct Outage {
    inner: FixtureFetch,
    down: std::sync::atomic::AtomicBool,
}

impl crate::fs::Fetch for Outage {
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

#[test]
fn listing_failure() {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([
        "--exact",
        "fs_tests::listing_failure_child",
        "--nocapture",
        "--test-threads=1",
    ])
    .env(LISTING_CHILD, "1");
    fuse::testns::unshare(&mut cmd);
    let out = cmd.output().expect("run the child");
    assert!(
        out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("okfs-listing-failure-done"),
        "the child: {}\n{}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A directory listed while its files cannot be read fails, and never lists without them: the
/// kernel keeps a listing for as long as the directory lives, so one with a file missing would
/// stay that way after ok came back.
#[test]
fn listing_failure_child() {
    if std::env::var_os(LISTING_CHILD).is_none() {
        return;
    }
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
    println!("okfs-listing-failure-done");
}

/// A source that answers trees' sizes records when `records` is set, counting how sizes were
/// asked for: per file, and per tree.
struct Sized {
    inner: FixtureFetch,
    records: bool,
    per_file: std::sync::atomic::AtomicUsize,
    per_tree: std::sync::atomic::AtomicUsize,
}

impl crate::fs::Fetch for Sized {
    fn tree(&self, id: &jj_lib::backend::TreeId) -> std::io::Result<Arc<jj_lib::backend::Tree>> {
        self.inner.tree(id)
    }
    fn sizes(
        &self,
        ids: &[jj_lib::backend::FileId],
    ) -> std::io::Result<std::collections::HashMap<jj_lib::backend::FileId, u64>> {
        self.per_file
            .fetch_add(ids.len(), std::sync::atomic::Ordering::SeqCst);
        self.inner.sizes(ids)
    }
    fn tree_sizes(
        &self,
        trees: &[(jj_lib::backend::TreeId, Arc<jj_lib::backend::Tree>)],
    ) -> std::io::Result<std::collections::HashMap<jj_lib::backend::FileId, u64>> {
        if !self.records {
            return Ok(Default::default());
        }
        self.per_tree
            .fetch_add(trees.len(), std::sync::atomic::Ordering::SeqCst);
        let mut out = std::collections::HashMap::new();
        for (_, t) in trees {
            for e in t.entries() {
                if let jj_lib::backend::TreeValue::File { id, .. } = e.value() {
                    out.insert(id.clone(), self.inner.0.files[id].len() as u64);
                }
            }
        }
        Ok(out)
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

/// Stats every file under `src`, returning their sizes and the source's counts.
fn stat_src(records: bool) -> (Vec<u64>, usize, usize) {
    use crate::model::ROOT;
    use crate::testfix::name;
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (model, _log) = fx.model(&base);
    let staging = fx.dir.as_ref().unwrap().path().join("staging");
    let fx = Arc::new(fx);
    let fetch = Arc::new(Sized {
        inner: FixtureFetch(fx, Arc::default()),
        records,
        per_file: Default::default(),
        per_tree: Default::default(),
    });
    let mount = Mount::new(model, fetch.clone(), staging, Box::new(|| {})).unwrap();
    let lookup = |dir, n: &str| {
        mount
            .with(|m| m.lookup(dir, name(n), true))
            .unwrap()
            .unwrap()
    };
    let src = lookup(ROOT, "src");
    let deep = lookup(src, "deep");
    let mut sizes = Vec::new();
    for (dir, file) in [(src, "a.rs"), (src, "b.rs"), (deep, "c.rs")] {
        let ino = lookup(dir, file);
        sizes.push(mount.with(|m| m.attr(ino)).unwrap().size);
    }
    let count = |c: &std::sync::atomic::AtomicUsize| c.load(std::sync::atomic::Ordering::SeqCst);
    (sizes, count(&fetch.per_file), count(&fetch.per_tree))
}

// A directory's sizes come with it, one record a tree: a stat of its files asks for no file's size.
// Without records, each file's size is read as before.
#[test]
fn a_directorys_sizes_come_with_it() {
    let (sizes, per_file, per_tree) = stat_src(true);
    assert_eq!(sizes, [10, 10, 10]);
    assert_eq!(per_file, 0, "a file's size was read alone");
    assert!(per_tree >= 2, "src's and deep's records: {per_tree} trees");

    let (sizes, per_file, per_tree) = stat_src(false);
    assert_eq!(sizes, [10, 10, 10]);
    assert_eq!(per_tree, 0);
    assert!(per_file >= 3, "without records, {per_file} files sized");
}
