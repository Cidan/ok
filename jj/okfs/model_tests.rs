//! The model in process: trees and contents from a fixture, answering its needs.

use jj_lib::backend::TreeId;
use jj_lib::merge::Merge;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use ok_proto::ok::okfs::v1 as pb;

use crate::model::Kind;
use crate::model::Model;
use crate::model::Opened;
use crate::model::ROOT;
use crate::model::Stale;
use crate::model::Terms;
use crate::model::resolved_terms;
use crate::persist::MemLog;
use crate::testfix::Entry;
use crate::testfix::Fixture;
use crate::testfix::name;
use crate::testfix::path;

pub fn base_tree(fx: &mut Fixture) -> TreeId {
    fx.tree(vec![
        ("README", Entry::File(b"readme\n")),
        ("run.sh", Entry::Exec(b"#!/bin/sh\n")),
        ("link", Entry::Link("README")),
        (
            "src",
            Entry::Dir(vec![
                ("a.rs", Entry::File(b"fn a() {}\n")),
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
    ])
}

// Reading the base: listings, attributes, contents, symlinks, and the .jj link.
#[test]
fn reads_the_base() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, _) = fx.model(&base);
    assert_eq!(
        fx.names(&mut m, ""),
        [".jj", "README", "docs", "link", "run.sh", "src"]
    );
    assert_eq!(fx.names(&mut m, "src"), ["a.rs", "b.rs", "deep"]);
    assert_eq!(fx.read(&mut m, "src/deep/c.rs").unwrap(), b"fn c() {}\n");
    let run = fx.at(&mut m, "run.sh").unwrap();
    let a = fx.run(&mut m, |m| m.attr(run)).unwrap();
    assert_eq!(a.mode & 0o777, 0o755);
    assert_eq!(a.size, 10);
    let link = fx.at(&mut m, "link").unwrap();
    assert_eq!(fx.run(&mut m, |m| m.readlink(link)).unwrap(), b"README");
    let jj = fx.at(&mut m, ".jj").unwrap();
    assert_eq!(fx.run(&mut m, |m| m.readlink(jj)).unwrap(), b"/state/jj");
    assert_eq!(fx.at(&mut m, "src/a.rs"), fx.at(&mut m, "src/a.rs"));
    // Nothing is written: the root's overlay lists nothing.
    let root = fx.run(&mut m, |m| m.list_dir(RepoPath::root())).unwrap();
    assert!(root.entries.is_empty());
}

// Writing materializes a file with its old contents first; the snapshot listing shows it, its
// content ID known until it is written again; unwritten siblings stay derived.
#[test]
fn writing_materializes() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, _) = fx.model(&base);
    let a = fx.at(&mut m, "src/a.rs").unwrap();
    assert_eq!(
        fx.run(&mut m, |m| m.open(a, libc::O_RDWR)).unwrap(),
        Opened::Overlay
    );
    assert_eq!(
        fx.run(&mut m, |m| m.read_overlay(a, 0, 100)).unwrap(),
        b"fn a() {}\n"
    );
    let src = fx.run(&mut m, |m| m.list_dir(&path("src"))).unwrap();
    assert!(src.materialized);
    assert_eq!(src.entries.len(), 1);
    let e = &src.entries[0];
    assert_eq!(e.name, b"a.rs");
    assert_eq!(e.kind(), pb::dir_entry::Kind::File);
    assert!(
        !e.file_id.is_empty(),
        "a file materialized unchanged has its content ID"
    );
    fx.run(&mut m, |m| m.write(a, 3, b"x")).unwrap();
    m.release(a);
    assert_eq!(fx.read(&mut m, "src/a.rs").unwrap(), b"fn x() {}\n");
    let src = fx.run(&mut m, |m| m.list_dir(&path("src"))).unwrap();
    assert!(
        src.entries[0].file_id.is_empty(),
        "written since: its content ID is not known"
    );
    let root = fx.run(&mut m, |m| m.list_dir(RepoPath::root())).unwrap();
    assert_eq!(root.entries.len(), 1);
    assert_eq!(root.entries[0].kind(), pb::dir_entry::Kind::Dir);
    assert!(
        !fx.run(&mut m, |m| m.list_dir(&path("docs")))
            .unwrap()
            .materialized
    );
    fx.write(&mut m, "README", b"new");
    assert_eq!(fx.read(&mut m, "README").unwrap(), b"new");
}

// Creating, deleting (a whiteout over the source), and the rules of rmdir.
#[test]
fn creates_and_deletes() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, _) = fx.model(&base);
    fx.write(&mut m, "src/new.rs", b"new\n");
    assert_eq!(fx.names(&mut m, "src"), ["a.rs", "b.rs", "deep", "new.rs"]);
    let src = fx.at(&mut m, "src").unwrap();
    fx.run(&mut m, |m| m.unlink(src, name("b.rs"))).unwrap();
    assert_eq!(fx.at(&mut m, "src/b.rs"), None);
    assert_eq!(fx.names(&mut m, "src"), ["a.rs", "deep", "new.rs"]);
    let listing = fx.run(&mut m, |m| m.list_dir(&path("src"))).unwrap();
    let deleted: Vec<_> = listing
        .entries
        .iter()
        .filter(|e| e.kind() == pb::dir_entry::Kind::Deleted)
        .collect();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].name, b"b.rs");
    // Deleting a created file leaves no whiteout: the source never had it.
    fx.run(&mut m, |m| m.unlink(src, name("new.rs"))).unwrap();
    let listing = fx.run(&mut m, |m| m.list_dir(&path("src"))).unwrap();
    assert!(listing.entries.iter().all(|e| e.name != b"new.rs"));
    assert_eq!(
        fx.run(&mut m, |m| m.rmdir(src, name("deep"))),
        Err(libc::ENOTEMPTY)
    );
    let deep = fx.at(&mut m, "src/deep").unwrap();
    fx.run(&mut m, |m| m.unlink(deep, name("c.rs"))).unwrap();
    fx.run(&mut m, |m| m.rmdir(src, name("deep"))).unwrap();
    assert_eq!(fx.at(&mut m, "src/deep"), None);
    // A directory recreated where one was deleted starts empty.
    fx.run(&mut m, |m| {
        m.create(src, name("deep"), Kind::Dir, 0o755, b"")
    })
    .unwrap();
    assert!(fx.names(&mut m, "src/deep").is_empty());
    assert_eq!(
        fx.run(&mut m, |m| m.create(
            ROOT,
            name(".jj"),
            Kind::File,
            0o644,
            b""
        )),
        Err(libc::EPERM)
    );
    assert_eq!(
        fx.run(&mut m, |m| m.create(
            src,
            name("a.rs"),
            Kind::File,
            0o644,
            b""
        )),
        Err(libc::EEXIST)
    );
}

// Renaming moves a file's value without copying it, and a directory with its tree; the old
// name is whited out; renaming over a file replaces it; exchanging swaps.
#[test]
fn renames() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, _) = fx.model(&base);
    let src = fx.at(&mut m, "src").unwrap();
    let docs = fx.at(&mut m, "docs").unwrap();
    fx.run(&mut m, |m| {
        m.rename(src, name("a.rs"), docs, name("a.rs"), 0)
    })
    .unwrap();
    assert_eq!(fx.read(&mut m, "docs/a.rs").unwrap(), b"fn a() {}\n");
    assert_eq!(fx.at(&mut m, "src/a.rs"), None);
    let docs_listing = fx.run(&mut m, |m| m.list_dir(&path("docs"))).unwrap();
    let moved = docs_listing
        .entries
        .iter()
        .find(|e| e.name == b"a.rs")
        .unwrap();
    assert!(
        moved.value.is_some() && moved.overlay_path.is_empty(),
        "moved, not copied"
    );
    fx.run(&mut m, |m| {
        m.rename(src, name("deep"), ROOT, name("moved"), 0)
    })
    .unwrap();
    assert_eq!(fx.read(&mut m, "moved/c.rs").unwrap(), b"fn c() {}\n");
    assert_eq!(fx.at(&mut m, "src/deep"), None);
    fx.run(&mut m, |m| {
        m.rename(src, name("b.rs"), ROOT, name("README"), 0)
    })
    .unwrap();
    assert_eq!(fx.read(&mut m, "README").unwrap(), b"fn b() {}\n");
    let moved = fx.at(&mut m, "moved").unwrap();
    assert_eq!(
        fx.run(&mut m, |m| m.rename(
            ROOT,
            name("moved"),
            moved,
            name("x"),
            0
        )),
        Err(libc::EINVAL)
    );
    fx.run(&mut m, |m| {
        m.rename(ROOT, name("run.sh"), ROOT, name("README"), 2)
    })
    .unwrap();
    assert_eq!(fx.read(&mut m, "README").unwrap(), b"#!/bin/sh\n");
    assert_eq!(fx.read(&mut m, "run.sh").unwrap(), b"fn b() {}\n");
    assert_eq!(
        fx.run(&mut m, |m| m.rename(
            ROOT,
            name("run.sh"),
            ROOT,
            name("README"),
            1
        )),
        Err(libc::EEXIST)
    );
}

// Hard links share content; a mode change pins a file without copying it.
#[test]
fn links_and_modes() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, _) = fx.model(&base);
    let readme = fx.at(&mut m, "README").unwrap();
    let docs = fx.at(&mut m, "docs").unwrap();
    fx.run(&mut m, |m| m.link(readme, docs, name("readme-link")))
        .unwrap();
    assert_eq!(fx.at(&mut m, "docs/readme-link"), Some(readme));
    fx.run(&mut m, |m| m.write(readme, 0, b"R")).unwrap();
    assert_eq!(fx.read(&mut m, "docs/readme-link").unwrap(), b"Readme\n");
    fx.run(&mut m, |m| m.unlink(ROOT, name("README"))).unwrap();
    assert_eq!(fx.read(&mut m, "docs/readme-link").unwrap(), b"Readme\n");
    assert_eq!(fx.run(&mut m, |m| m.attr(readme)).unwrap().nlink, 1);
    let a = fx.at(&mut m, "src/a.rs").unwrap();
    fx.run(&mut m, |m| {
        m.setattr(
            a,
            &okfs_fuse::SetAttr {
                mode: Some(0o755),
                ..Default::default()
            },
        )
    })
    .unwrap();
    let listing = fx.run(&mut m, |m| m.list_dir(&path("src"))).unwrap();
    let e = listing.entries.iter().find(|e| e.name == b"a.rs").unwrap();
    assert!(e.value.as_ref().unwrap().executable);
    assert!(e.overlay_path.is_empty());
    assert_eq!(fx.run(&mut m, |m| m.attr(a)).unwrap().mode & 0o777, 0o755);
}

fn checkout(
    fx: &Fixture,
    m: &mut Model,
    recorded: &TreeId,
    new: &TreeId,
) -> (Vec<RepoPathBuf>, Vec<Stale>) {
    fx.run(m, |m| {
        m.check_out(
            resolved_terms(recorded.clone()),
            resolved_terms(new.clone()),
            &[],
        )
    })
    .unwrap()
}

// A checkout: written files the recorded tree holds come from the new tree; an untracked file
// stays, and where the new tree puts a file of its own it is skipped; what the new tree removes
// goes, and directories it empties with it.
#[test]
fn checks_out() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, _) = fx.model(&base);
    fx.write(&mut m, "README", b"edited\n");
    fx.write(&mut m, "notes.txt", b"mine\n");
    fx.run(&mut m, |m| {
        m.create(ROOT, name("build"), Kind::Dir, 0o755, b"")
    })
    .unwrap();
    fx.write(&mut m, "build/out", b"artifact\n");
    let src = |b: &'static [u8]| -> Entry<'static> {
        Entry::Dir(vec![
            ("a.rs", Entry::File(b"fn a() {}\n")),
            ("b.rs", Entry::File(b)),
            (
                "deep",
                Entry::Dir(vec![("c.rs", Entry::File(b"fn c() {}\n"))]),
            ),
        ])
    };
    let recorded = fx.tree(vec![
        ("README", Entry::File(b"edited\n")),
        ("run.sh", Entry::Exec(b"#!/bin/sh\n")),
        ("link", Entry::Link("README")),
        ("src", src(b"fn b() {}\n")),
        (
            "docs",
            Entry::Dir(vec![("guide.md", Entry::File(b"# guide\n"))]),
        ),
    ]);
    let new = fx.tree(vec![
        ("README", Entry::File(b"edited\n")),
        ("run.sh", Entry::Exec(b"#!/bin/sh\n")),
        ("link", Entry::Link("README")),
        ("notes.txt", Entry::File(b"theirs\n")),
        ("src", src(b"fn b2() {}\n")),
        ("new", Entry::Dir(vec![("x", Entry::File(b"x\n"))])),
    ]);
    let b_before = fx.at(&mut m, "src/b.rs").unwrap();
    let (skipped, stale) = checkout(&fx, &mut m, &recorded, &new);
    assert_eq!(skipped, [path("notes.txt")]);
    assert_eq!(fx.read(&mut m, "README").unwrap(), b"edited\n");
    assert_eq!(fx.read(&mut m, "notes.txt").unwrap(), b"mine\n");
    assert_eq!(fx.read(&mut m, "build/out").unwrap(), b"artifact\n");
    assert_eq!(fx.read(&mut m, "src/b.rs").unwrap(), b"fn b2() {}\n");
    assert_eq!(
        fx.at(&mut m, "src/b.rs"),
        Some(b_before),
        "a changed file keeps its inode"
    );
    assert!(stale.contains(&Stale::Inode(b_before)));
    assert_eq!(fx.at(&mut m, "docs"), None);
    assert_eq!(fx.read(&mut m, "new/x").unwrap(), b"x\n");
    assert_eq!(
        fx.names(&mut m, ""),
        [
            ".jj",
            "README",
            "build",
            "link",
            "new",
            "notes.txt",
            "run.sh",
            "src"
        ]
    );
    assert_eq!(m.base(), &resolved_terms(new));
}

// A directory moved by a rename and snapshotted: a later checkout that changes a file under its
// new path shows the new tree's file there, not the moved tree's.
#[test]
fn checkout_through_a_moved_directory() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, _) = fx.model(&base);
    let src = fx.at(&mut m, "src").unwrap();
    fx.run(&mut m, |m| {
        m.rename(ROOT, name("src"), ROOT, name("lib"), 0)
    })
    .unwrap();
    assert_eq!(fx.at(&mut m, "lib"), Some(src));
    let lib = |a: &'static [u8]| -> Entry<'static> {
        Entry::Dir(vec![
            ("a.rs", Entry::File(a)),
            ("b.rs", Entry::File(b"fn b() {}\n")),
            (
                "deep",
                Entry::Dir(vec![("c.rs", Entry::File(b"fn c() {}\n"))]),
            ),
        ])
    };
    let top = |l: Entry<'static>| -> Vec<(&'static str, Entry<'static>)> {
        vec![
            ("README", Entry::File(b"readme\n")),
            ("run.sh", Entry::Exec(b"#!/bin/sh\n")),
            ("link", Entry::Link("README")),
            ("lib", l),
            (
                "docs",
                Entry::Dir(vec![("guide.md", Entry::File(b"# guide\n"))]),
            ),
        ]
    };
    let recorded = fx.tree(top(lib(b"fn a() {}\n")));
    let new = fx.tree(top(lib(b"fn a2() {}\n")));
    checkout(&fx, &mut m, &recorded, &new);
    assert_eq!(fx.read(&mut m, "lib/a.rs").unwrap(), b"fn a2() {}\n");
    assert_eq!(fx.read(&mut m, "lib/deep/c.rs").unwrap(), b"fn c() {}\n");
    assert_eq!(fx.at(&mut m, "src"), None);
}

// A conflict arrives materialized: a written file, marked so a snapshot keeps the conflict until
// it is edited.
#[test]
fn checkout_conflicts() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, _) = fx.model(&base);
    let text = b"<<<<<<< conflict\n%%%%%%%\n>>>>>>>\n".to_vec();
    fx.run(&mut m, |m| {
        m.check_out(
            resolved_terms(base.clone()),
            resolved_terms(base.clone()),
            &[crate::model::Conflict {
                path: path("src/a.rs"),
                contents: text.clone(),
                executable: false,
                marker_len: 7,
            }],
        )
    })
    .unwrap();
    assert_eq!(fx.read(&mut m, "src/a.rs").unwrap(), text);
    let listing = fx.run(&mut m, |m| m.list_dir(&path("src"))).unwrap();
    let e = listing.entries.iter().find(|e| e.name == b"a.rs").unwrap();
    assert!(e.checkout_conflict);
    assert_eq!(e.conflict_marker_len, 7);
    fx.write(&mut m, "src/a.rs", b"resolved\n");
    let listing = fx.run(&mut m, |m| m.list_dir(&path("src"))).unwrap();
    assert!(
        !listing
            .entries
            .iter()
            .find(|e| e.name == b"a.rs")
            .unwrap()
            .checkout_conflict
    );
}

// Everything survives a reload from the records: contents, whiteouts, moves, modes, and inode
// numbers.
#[test]
fn reloads() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, log) = fx.model(&base);
    fx.write(&mut m, "src/a.rs", b"edited\n");
    let src = fx.at(&mut m, "src").unwrap();
    fx.run(&mut m, |m| m.unlink(src, name("b.rs"))).unwrap();
    fx.run(&mut m, |m| {
        m.rename(src, name("deep"), ROOT, name("moved"), 0)
    })
    .unwrap();
    let run = fx.at(&mut m, "run.sh").unwrap();
    fx.run(&mut m, |m| {
        m.setattr(
            run,
            &okfs_fuse::SetAttr {
                mode: Some(0o700),
                ..Default::default()
            },
        )
    })
    .unwrap();
    let docs = fx.at(&mut m, "docs").unwrap();
    let a = fx.at(&mut m, "src/a.rs").unwrap();
    let overlay = m.overlay_path(0).parent().unwrap().to_owned();
    let records = log.0.lock().unwrap().clone();
    for (fresh, records) in [(false, records.clone()), (true, m.records())] {
        let mut m2 = Model::load(
            records,
            overlay.clone(),
            b"/state/jj".to_vec(),
            Box::new(MemLog::default()),
            crate::model::now,
            fresh,
        )
        .unwrap();
        assert_eq!(fx.read(&mut m2, "src/a.rs").unwrap(), b"edited\n");
        assert_eq!(fx.at(&mut m2, "src/a.rs"), Some(a));
        assert_eq!(fx.at(&mut m2, "src/b.rs"), None);
        assert_eq!(fx.read(&mut m2, "moved/c.rs").unwrap(), b"fn c() {}\n");
        assert_eq!(fx.at(&mut m2, "docs"), Some(docs));
        let run2 = fx.at(&mut m2, "run.sh").unwrap();
        assert_eq!(
            fx.run(&mut m2, |m| m.attr(run2)).unwrap().mode & 0o777,
            0o700
        );
        assert_eq!(
            fx.names(&mut m2, ""),
            [".jj", "README", "docs", "link", "moved", "run.sh", "src"]
        );
    }
}

// Sparse patterns hide what they exclude from the base, but never what was written.
#[test]
fn sparse() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let (mut m, _) = fx.model(&base);
    fx.write(&mut m, "notes.txt", b"mine\n");
    fx.run(&mut m, |m| m.set_sparse(vec![path("src/deep")]))
        .unwrap();
    assert_eq!(fx.names(&mut m, ""), [".jj", "notes.txt", "src"]);
    assert_eq!(fx.names(&mut m, "src"), ["deep"]);
    assert_eq!(fx.read(&mut m, "src/deep/c.rs").unwrap(), b"fn c() {}\n");
    fx.run(&mut m, |m| m.set_sparse(vec![RepoPathBuf::root()]))
        .unwrap();
    assert_eq!(fx.names(&mut m, "src"), ["a.rs", "b.rs", "deep"]);
}

// A base whose root is a conflict of trees lists the merged directory: entries the sides agree
// on, and a directory whose sides are all trees.
#[test]
fn merged_base() {
    let mut fx = Fixture::new();
    let parent = fx.tree(vec![
        ("a", Entry::File(b"a\n")),
        ("d", Entry::Dir(vec![("x", Entry::File(b"x\n"))])),
    ]);
    let left = fx.tree(vec![
        ("a", Entry::File(b"a\n")),
        (
            "d",
            Entry::Dir(vec![("x", Entry::File(b"x\n")), ("l", Entry::File(b"l\n"))]),
        ),
    ]);
    let right = fx.tree(vec![
        ("a", Entry::File(b"a\n")),
        (
            "d",
            Entry::Dir(vec![("x", Entry::File(b"x\n")), ("r", Entry::File(b"r\n"))]),
        ),
    ]);
    let terms: Terms = Merge::from_vec(vec![Some(left), Some(parent), Some(right)]);
    let dir = fx.dir.as_ref().unwrap().path().join("merged");
    let mut m = Model::new(
        terms,
        dir,
        b"/j".to_vec(),
        Box::new(MemLog::default()),
        crate::model::now,
    )
    .unwrap();
    assert_eq!(fx.names(&mut m, ""), [".jj", "a", "d"]);
    assert_eq!(fx.names(&mut m, "d"), ["l", "r", "x"]);
}

// A checkout costs what the workspace holds, not the size of the change: a fresh workspace moved
// to a tree of 30,000 files reads only the root (all the kernel has looked into), and the files
// are there to read afterwards.
#[test]
fn a_large_checkout_reads_only_what_is_looked_at() {
    let mut fx = Fixture::new();
    let empty = fx.tree(Vec::new());
    let contents: Vec<Vec<u8>> = (0..30_000)
        .map(|i| format!("file {i}\n").into_bytes())
        .collect();
    let mut dirs = Vec::new();
    for d in 0..300 {
        let files: Vec<(String, usize)> = (0..100)
            .map(|f| (format!("f{f:03}"), d * 100 + f))
            .collect();
        dirs.push((format!("d{d:03}"), files));
    }
    let entries: Vec<(&str, Entry<'_>)> = dirs
        .iter()
        .map(|(d, files)| {
            (
                d.as_str(),
                Entry::Dir(
                    files
                        .iter()
                        .map(|(f, i)| (f.as_str(), Entry::File(&contents[*i])))
                        .collect(),
                ),
            )
        })
        .collect();
    let big = fx.tree(entries);
    let (mut m, _) = fx.model(&empty);
    let before = fx.trees_fetched.load(std::sync::atomic::Ordering::Relaxed);
    let started = std::time::Instant::now();
    let (skipped, _) = checkout(&fx, &mut m, &empty, &big);
    assert!(skipped.is_empty());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert!(
        fx.trees_fetched.load(std::sync::atomic::Ordering::Relaxed) - before <= 2,
        "{} trees read",
        fx.trees_fetched.load(std::sync::atomic::Ordering::Relaxed) - before
    );
    assert_eq!(fx.names(&mut m, "").len(), 301);
    assert_eq!(fx.read(&mut m, "d123/f045").unwrap(), b"file 12345\n");
    // Back to the empty tree, with one directory looked into: only what was looked at is read.
    let before = fx.trees_fetched.load(std::sync::atomic::Ordering::Relaxed);
    checkout(&fx, &mut m, &big, &empty);
    assert!(
        fx.trees_fetched.load(std::sync::atomic::Ordering::Relaxed) - before <= 3,
        "{} trees read",
        fx.trees_fetched.load(std::sync::atomic::Ordering::Relaxed) - before
    );
    assert_eq!(fx.names(&mut m, ""), [".jj"]);
}

// A presentation (docs/okfs.md, "ok's roles") shows no `.jj`, and a file a service puts
// at its root beside the commit's — replacing the commit's own of that name until it is dropped —
// survives nothing but its drop: a checkout keeps it, as an untracked file.
#[test]
fn a_presentation_shows_put_files_and_no_jj() {
    let mut fx = Fixture::new();
    let one = fx.tree(vec![
        ("a.txt", Entry::File(b"a\n")),
        ("ctx.json", Entry::File(b"the commit's\n")),
    ]);
    let two = fx.tree(vec![("b.txt", Entry::File(b"b\n"))]);
    let mut m = fx.presentation(&one);
    assert_eq!(fx.names(&mut m, ""), ["a.txt", "ctx.json"]);
    assert!(fx.at(&mut m, ".jj").is_none());

    fx.run(&mut m, |m| {
        m.put_file(std::ffi::OsStr::new("ctx.json"), b"the job's\n")
    })
    .unwrap();
    fx.run(&mut m, |m| {
        m.put_file(std::ffi::OsStr::new("new.json"), b"{}\n")
    })
    .unwrap();
    assert_eq!(fx.read(&mut m, "ctx.json").unwrap(), b"the job's\n");
    assert_eq!(fx.names(&mut m, ""), ["a.txt", "ctx.json", "new.json"]);

    // Dropped, the commit's own shows again; a name the commit lacks is gone.
    fx.run(&mut m, |m| m.drop_file(std::ffi::OsStr::new("ctx.json")))
        .unwrap();
    fx.run(&mut m, |m| m.drop_file(std::ffi::OsStr::new("new.json")))
        .unwrap();
    assert_eq!(fx.read(&mut m, "ctx.json").unwrap(), b"the commit's\n");
    assert_eq!(fx.names(&mut m, ""), ["a.txt", "ctx.json"]);

    // Moved to another commit, it shows that commit's tree.
    checkout(&fx, &mut m, &one, &two);
    assert_eq!(fx.names(&mut m, ""), ["b.txt"]);
    assert_eq!(fx.read(&mut m, "b.txt").unwrap(), b"b\n");
}
