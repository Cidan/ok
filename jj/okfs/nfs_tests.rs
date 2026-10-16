//! A workspace served over NFS: the model behind okfs-nfs's server, from the fixture's trees,
//! called over TCP as the kernel's NFS client calls it — on Linux too, as nothing is mounted.

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use okfs_nfs as nfs;
use okfs_nfs::SetAttr;
use okfs_nfs::client::Client;

use crate::fs::Mount;
use crate::model::ROOT;
use crate::model::resolved_terms;
use crate::model_tests::base_tree;
use crate::nfs::NfsMount;
use crate::nfs::Refresh;
use crate::testfix::Entry;
use crate::testfix::Fixture;
use crate::testfix::FixtureFetch;

const EXPORT: u64 = 1;

struct Served {
    _fx: Arc<Fixture>,
    mount: Arc<Mount>,
    nfs: Arc<NfsMount>,
    changes: Arc<AtomicUsize>,
    client: Client,
    root: Vec<u8>,
    uid: u32,
}

/// The fixture's base tree, served over NFS and mounted by the test client.
fn served(fx: Fixture, base: &jj_lib::backend::TreeId, grace: Duration) -> Served {
    let mut fx = fx;
    let (model, _log) = fx.model(base);
    let dir = fx.dir.as_ref().unwrap().path().to_owned();
    let fx = Arc::new(fx);
    let changes = Arc::new(AtomicUsize::new(0));
    let counted = changes.clone();
    let mount = Arc::new(
        Mount::new(
            model,
            Arc::new(FixtureFetch(
                fx.clone(),
                Arc::new(std::sync::Mutex::new(Vec::new())),
            )),
            dir.join("staging"),
            Box::new(move || {
                counted.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap(),
    );
    // Nothing is mounted at this path: a refresh's requests through it fail, and its times move.
    let refresh = Arc::new(Refresh::new(dir.join("not-mounted")));
    let nfs = Arc::new(NfsMount::new(mount.clone(), refresh).with_grace(grace));
    let uid = unsafe { libc::geteuid() };
    let server = nfs::Server::new(nfs::Options {
        key: [3; 32],
        owner: uid,
        workers: 4,
        trace: false,
        starting: false,
    });
    server.export(EXPORT, ROOT, nfs.clone());
    server.arm(b"/test", EXPORT);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || server.serve(listener));
    let mut client = Client::connect(addr, uid, unsafe { libc::getegid() }).unwrap();
    let root = client.mnt(b"/test").unwrap().unwrap();
    Served {
        _fx: fx,
        mount,
        nfs,
        changes,
        client,
        root,
        uid,
    }
}

fn walk(c: &mut Client, root: &[u8], path: &str) -> Vec<u8> {
    let mut fh = root.to_vec();
    for part in path.split('/') {
        fh = c.lookup(&fh, part.as_bytes()).unwrap().unwrap().0;
    }
    fh
}

fn read_all(c: &mut Client, fh: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let (data, eof) = c.read(fh, out.len() as u64, 1 << 20).unwrap().unwrap();
        out.extend_from_slice(&data);
        if eof {
            return out;
        }
    }
}

// The base, read over NFS: listings with attributes and handles, contents, modes, symlinks.
#[test]
fn reads_the_base() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let mut s = served(fx, &base, Duration::ZERO);
    let c = &mut s.client;
    let names: Vec<String> = c
        .readdirplus(&s.root, 4096)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|e| String::from_utf8(e.name).unwrap())
        .collect();
    assert_eq!(
        names,
        [".", "..", ".jj", "README", "docs", "link", "run.sh", "src"]
    );
    let src = walk(c, &s.root, "src");
    let plus = c.readdirplus(&src, 4096).unwrap().unwrap();
    let a = plus.iter().find(|e| e.name == b"a.rs").unwrap();
    assert_eq!(a.attr.unwrap().size, 10);
    assert_eq!(read_all(c, a.fh.as_ref().unwrap()), b"fn a() {}\n");
    let c_rs = walk(c, &s.root, "src/deep/c.rs");
    assert_eq!(read_all(c, &c_rs), b"fn c() {}\n");
    let run = walk(c, &s.root, "run.sh");
    let attr = c.getattr(&run).unwrap().unwrap();
    assert_eq!((attr.perm, attr.uid), (0o755, s.uid));
    let link = walk(c, &s.root, "link");
    assert_eq!(c.readlink(&link).unwrap().unwrap(), b"README");
    assert_eq!(c.lookup(&s.root, b"nope").unwrap(), Err(nfs::nfs3::NOENT));
    // `..` of a directory is its parent, and of the root the root.
    let (up, _) = c.lookup(&src, b"..").unwrap().unwrap();
    assert_eq!(c.getattr(&up).unwrap().unwrap().fileid, ROOT);
    let (up, _) = c.lookup(&s.root, b"..").unwrap().unwrap();
    assert_eq!(c.getattr(&up).unwrap().unwrap().fileid, ROOT);
    // Nothing was written.
    assert_eq!(s.changes.load(Ordering::SeqCst), 0);
}

// Writing over NFS: an edit, a new file, a directory, a rename, a deletion, a hard link, a chmod,
// a truncation — each as the model records it.
#[test]
fn writes_reach_the_model() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let mut s = served(fx, &base, Duration::ZERO);
    let c = &mut s.client;
    let readme = walk(c, &s.root, "README");
    let (n, _) = c.write(&readme, 0, b"README!\n").unwrap().unwrap();
    assert_eq!(n, 8);
    assert_eq!(read_all(c, &readme), b"README!\n");
    let new = c.create(&s.root, b"new.txt", 0o644).unwrap().unwrap();
    c.write(&new, 0, b"made over NFS\n").unwrap().unwrap();
    let dir = c.mkdir(&s.root, b"made", 0o755).unwrap().unwrap();
    c.rename(&s.root, b"new.txt", &dir, b"moved.txt")
        .unwrap()
        .unwrap();
    let moved = walk(c, &s.root, "made/moved.txt");
    c.link(&moved, &s.root, b"hard").unwrap().unwrap();
    let src = walk(c, &s.root, "src");
    c.remove(&src, b"b.rs").unwrap().unwrap();
    let run = walk(c, &s.root, "run.sh");
    c.setattr(
        &run,
        &SetAttr {
            mode: Some(0o644),
            ..SetAttr::default()
        },
    )
    .unwrap()
    .unwrap();
    let a = walk(c, &s.root, "src/a.rs");
    c.setattr(
        &a,
        &SetAttr {
            size: Some(3),
            ..SetAttr::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(read_all(c, &a), b"fn ");
    assert!(s.changes.load(Ordering::SeqCst) > 0);

    let path = |p: &str| crate::testfix::path(p);
    let m = &mut s.mount.model();
    assert!(m.resolve(&path("made/moved.txt")).unwrap().is_some());
    assert!(m.resolve(&path("hard")).unwrap().is_some());
    assert!(m.resolve(&path("src/b.rs")).unwrap().is_none());
    assert!(m.resolve(&path("new.txt")).unwrap().is_none());
    let run = m.resolve(&path("run.sh")).unwrap().unwrap();
    assert_eq!(m.attr(run).unwrap().mode & 0o777, 0o644);
}

// A SETATTR that changes nothing — a refresh's — leaves the model as it was: no record written,
// nothing changed for a checkpoint to take.
#[test]
fn a_setattr_that_changes_nothing_changes_nothing() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let mut s = served(fx, &base, Duration::ZERO);
    let c = &mut s.client;
    let src = walk(c, &s.root, "src");
    let a = walk(c, &s.root, "src/a.rs");
    let logged = s.mount.model().log_len();
    for (fh, mode) in [(&src, 0o755), (&a, 0o644), (&s.root, 0o755)] {
        let (_, after) = c
            .setattr(
                fh,
                &SetAttr {
                    mode: Some(mode),
                    ..SetAttr::default()
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(after.unwrap().perm, mode);
    }
    assert_eq!(s.mount.model().log_len(), logged, "a record was written");
    assert_eq!(s.changes.load(Ordering::SeqCst), 0);
    assert_eq!(s.mount.model().overlay_stats(), (0, 0));
}

// A checkout under the kernel: the directories and files it changed that the kernel has report
// moved times — what makes the kernel drop its cache of them — and read as the new base.
#[test]
fn a_checkout_moves_the_times_of_what_it_changed() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let next = fx.tree(vec![
        ("README", Entry::File(b"readme\n")),
        ("run.sh", Entry::Exec(b"#!/bin/sh\n")),
        ("link", Entry::Link("README")),
        (
            "src",
            Entry::Dir(vec![
                ("a.rs", Entry::File(b"fn a() { changed() }\n")),
                ("b.rs", Entry::File(b"fn b() {}\n")),
                (
                    "deep",
                    Entry::Dir(vec![("c.rs", Entry::File(b"fn c() {}\n"))]),
                ),
            ]),
        ),
        (
            "docs",
            Entry::Dir(vec![("guide.md", Entry::File(b"# guide\n"))]),
        ),
    ]);
    let mut s = served(fx, &base, Duration::ZERO);
    let c = &mut s.client;
    let src = walk(c, &s.root, "src");
    let a = walk(c, &s.root, "src/a.rs");
    let before = |c: &mut Client, fh: &[u8]| c.getattr(fh).unwrap().unwrap();
    let (src0, a0) = (before(c, &src), before(c, &a));
    std::thread::sleep(Duration::from_millis(5));

    let recorded = s.mount.model().recorded().0.clone();
    let (_, stale) = s
        .mount
        .with(|m| m.check_out(recorded.clone(), resolved_terms(next.clone()), &[]))
        .unwrap();
    s.mount.invalidate(&stale);

    let (src1, a1) = (before(c, &src), before(c, &a));
    assert!(src1.mtime > src0.mtime, "the changed directory's time");
    assert!(a1.mtime > a0.mtime, "the changed file's time");
    assert_eq!(a1.size, 21);
    assert_eq!(read_all(c, &a), b"fn a() { changed() }\n");
}

// A removed file's handle is stale at once; one a checkout left with no name, after the grace.
#[test]
fn removed_and_detached_files_are_let_go() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let empty = fx.tree(vec![("README", Entry::File(b"readme\n"))]);
    let mut s = served(fx, &base, Duration::ZERO);
    let c = &mut s.client;
    let made = c.create(&s.root, b"scratch", 0o644).unwrap().unwrap();
    c.write(&made, 0, b"x").unwrap().unwrap();
    c.remove(&s.root, b"scratch").unwrap().unwrap();
    assert_eq!(c.getattr(&made).unwrap(), Err(nfs::nfs3::STALE));

    let a = walk(c, &s.root, "src/a.rs");
    let recorded = s.mount.model().recorded().0.clone();
    let (_, stale) = s
        .mount
        .with(|m| m.check_out(recorded.clone(), resolved_terms(empty.clone()), &[]))
        .unwrap();
    s.mount.invalidate(&stale);
    assert_eq!(c.lookup(&s.root, b"src").unwrap(), Err(nfs::nfs3::NOENT));
    // The first sweep notes it detached; the next, past the grace, lets it go.
    s.nfs.sweep();
    s.nfs.sweep();
    assert_eq!(c.getattr(&a).unwrap(), Err(nfs::nfs3::STALE));
}

// Files written over NFS, which never closes them, are closed once others are written since.
#[test]
fn files_written_are_closed_when_idle() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let mut s = served(fx, &base, Duration::ZERO);
    let c = &mut s.client;
    for i in 0..200 {
        let fh = c
            .create(&s.root, format!("f{i}").as_bytes(), 0o644)
            .unwrap()
            .unwrap();
        c.write(&fh, 0, b"data").unwrap().unwrap();
    }
    let open = s.mount.model().overlay_fds();
    assert!(open <= 128, "{open} overlay files open");
    // Each still reads.
    let f0 = walk(c, &s.root, "f0");
    assert_eq!(read_all(c, &f0), b"data");
}
