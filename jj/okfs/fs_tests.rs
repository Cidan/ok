//! The FUSE layer over the model without the kernel: requests answered in process. (The mount
//! against the kernel is tests/kernel.rs.)

use std::sync::Arc;

use crate::fs::Mount;
use crate::testfix::Fixture;
use crate::testfix::FixtureFetch;
use crate::testfix::base_tree;

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

/// A source whose reads of file contents fail, or, with `short`, come back half their length.
struct Failing {
    inner: FixtureFetch,
    short: bool,
}

impl crate::fs::Fetch for Failing {
    fn tree(&self, id: &jj_lib::backend::TreeId) -> std::io::Result<Arc<jj_lib::backend::Tree>> {
        self.inner.tree(id)
    }
    fn sizes(
        &self,
        ids: &[jj_lib::backend::FileId],
    ) -> std::io::Result<std::collections::HashMap<jj_lib::backend::FileId, u64>> {
        self.inner.sizes(ids)
    }
    fn tree_sizes(
        &self,
        _: &[(jj_lib::backend::TreeId, Arc<jj_lib::backend::Tree>)],
    ) -> std::io::Result<std::collections::HashMap<jj_lib::backend::FileId, u64>> {
        Ok(Default::default())
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
        if !self.short {
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }
        let mut data = self.inner.read(id, size, offset, len)?;
        data.truncate(data.len() / 2);
        Ok(data)
    }
}

// A read ok cannot answer fails with EIO, and so does one it answers short, which the kernel would
// take for the end of the file; the root's user.ok.fetch_failures counts both.
#[test]
fn a_failed_or_short_read_is_an_error_and_is_counted() {
    use crate::fs::FETCH_FAILURES_XATTR;
    use crate::model::ROOT;
    use crate::testfix::name;
    use okfs_fuse::Filesystem as _;
    let ctx = okfs_fuse::Ctx {
        uid: 0,
        gid: 0,
        pid: 0,
    };
    for short in [false, true] {
        let mut fx = Fixture::new();
        let base = base_tree(&mut fx);
        let (model, _log) = fx.model(&base);
        let staging = fx.dir.as_ref().unwrap().path().join("staging");
        let fx = Arc::new(fx);
        let fetch = Arc::new(Failing {
            inner: FixtureFetch(fx, Arc::default()),
            short,
        });
        let mount = Mount::new(model, fetch, staging, Box::new(|| {})).unwrap();
        let failures = || {
            let b = mount
                .getxattr(&ctx, ROOT, std::ffi::OsStr::new(FETCH_FAILURES_XATTR))
                .unwrap();
            String::from_utf8(b).unwrap().parse::<u64>().unwrap()
        };
        assert_eq!(failures(), 0);
        let src = mount
            .with(|m| m.lookup(ROOT, name("src"), true))
            .unwrap()
            .unwrap();
        let file = mount
            .with(|m| m.lookup(src, name("a.rs"), true))
            .unwrap()
            .unwrap();
        let opened = mount.open(&ctx, file, libc::O_RDONLY).unwrap();
        assert_eq!(
            mount.read(&ctx, file, opened.fh, 0, 4096),
            Err(libc::EIO),
            "short={short}"
        );
        assert_eq!(failures(), 1, "short={short}");
    }
}
