//! Test support: trees and file contents made up in memory, answering the model's needs and
//! serving a mount as its [`Fetch`]. Compiled for okfs's own tests: its unit tests, and, through
//! the `testkit` feature, its kernel test, a binary of its own harness (tests/kernel.rs).

use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use jj_lib::backend::CopyId;
use jj_lib::backend::FileId;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Tree as BackendTree;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::repo_path::RepoPathComponentBuf;

use crate::fs::Fetch;
use crate::model::Error;
use crate::model::Ino;
use crate::model::Kind;
use crate::model::Model;
use crate::model::Need;
use crate::model::Opened;
use crate::model::R;
use crate::model::resolved_terms;
use crate::persist::MemLog;

/// Trees and file contents, made up.
#[derive(Default)]
pub struct Fixture {
    pub trees: HashMap<TreeId, Arc<BackendTree>>,
    pub files: HashMap<FileId, Vec<u8>>,
    pub symlinks: HashMap<SymlinkId, Vec<u8>>,
    next: u32,
    pub dir: Option<tempfile::TempDir>,
    /// How many trees the model asked for.
    pub trees_fetched: std::sync::atomic::AtomicUsize,
    by_content: HashMap<Vec<u8>, FileId>,
}

pub enum Entry<'a> {
    File(&'a [u8]),
    Exec(&'a [u8]),
    Link(&'a str),
    Dir(Vec<(&'a str, Entry<'a>)>),
}

impl Fixture {
    pub fn new() -> Self {
        Fixture {
            dir: Some(tempfile::tempdir().unwrap()),
            ..Default::default()
        }
    }

    pub fn id(&mut self) -> Vec<u8> {
        self.next += 1;
        let mut id = vec![0u8; 32];
        id[..4].copy_from_slice(&self.next.to_be_bytes());
        id
    }

    pub fn file(&mut self, contents: &[u8]) -> FileId {
        if let Some(id) = self.by_content.get(contents) {
            return id.clone();
        }
        let id = FileId::new(self.id());
        self.files.insert(id.clone(), contents.to_vec());
        self.by_content.insert(contents.to_vec(), id.clone());
        id
    }

    pub fn tree(&mut self, entries: Vec<(&str, Entry<'_>)>) -> TreeId {
        let mut out = Vec::new();
        let mut entries = entries;
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (name, e) in entries {
            let v = match e {
                Entry::File(c) => TreeValue::File {
                    id: self.file(c),
                    executable: false,
                    copy_id: CopyId::placeholder(),
                },
                Entry::Exec(c) => TreeValue::File {
                    id: self.file(c),
                    executable: true,
                    copy_id: CopyId::placeholder(),
                },
                Entry::Link(t) => {
                    let id = SymlinkId::new(self.id());
                    self.symlinks.insert(id.clone(), t.as_bytes().to_vec());
                    TreeValue::Symlink(id)
                }
                Entry::Dir(es) => TreeValue::Tree(self.tree(es)),
            };
            out.push((RepoPathComponentBuf::new(name).unwrap(), v));
        }
        let id = TreeId::new(self.id());
        self.trees
            .insert(id.clone(), Arc::new(BackendTree::from_sorted_entries(out)));
        id
    }

    pub fn model(&mut self, base: &TreeId) -> (Model, MemLog) {
        let log = MemLog::default();
        self.id();
        let n = self.next;
        let overlay = self
            .dir
            .as_ref()
            .unwrap()
            .path()
            .join(format!("overlay-{n}"));
        let m = Model::new(
            resolved_terms(base.clone()),
            overlay,
            b"/state/jj".to_vec(),
            Box::new(log.clone()),
            crate::model::now,
        )
        .unwrap();
        (m, log)
    }

    /// A presentation's model over `base`: no `.jj`.
    pub fn presentation(&mut self, base: &TreeId) -> Model {
        self.id();
        let overlay = self
            .dir
            .as_ref()
            .unwrap()
            .path()
            .join(format!("overlay-{}", self.next));
        std::fs::create_dir_all(&overlay).unwrap();
        Model::new(
            resolved_terms(base.clone()),
            overlay,
            Vec::new(),
            Box::new(MemLog::default()),
            crate::model::now,
        )
        .unwrap()
    }

    /// Runs an operation, satisfying its needs from the fixture.
    pub fn run<T>(&self, m: &mut Model, mut f: impl FnMut(&mut Model) -> R<T>) -> Result<T, i32> {
        loop {
            match f(m) {
                Ok(t) => return Ok(t),
                Err(Error::Errno(e)) => return Err(e),
                Err(Error::Need(n)) => match n {
                    Need::Tree(id) => {
                        self.trees_fetched
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        m.cache.trees.insert(
                            id.clone(),
                            self.trees.get(&id).expect("a known tree").clone(),
                        );
                    }
                    Need::Trees(ids) => {
                        for id in ids {
                            self.trees_fetched
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            m.cache.trees.insert(
                                id.clone(),
                                self.trees.get(&id).expect("a known tree").clone(),
                            );
                        }
                    }
                    Need::Size(id) => {
                        m.cache
                            .sizes
                            .insert(id.clone(), self.files[&id].len() as u64);
                    }
                    Need::Symlink(id) => {
                        m.cache
                            .symlinks
                            .insert(id.clone(), self.symlinks[&id].clone());
                    }
                    Need::Content(id) => {
                        let p: PathBuf = self
                            .dir
                            .as_ref()
                            .unwrap()
                            .path()
                            .join(format!("content-{}", id.hex()));
                        std::fs::write(&p, &self.files[&id]).unwrap();
                        m.cache.contents.insert(id, p);
                    }
                },
            }
        }
    }

    pub fn at(&self, m: &mut Model, p: &str) -> Option<Ino> {
        self.run(m, |m| m.resolve(&path(p))).unwrap()
    }

    pub fn names(&self, m: &mut Model, p: &str) -> Vec<String> {
        let ino = self.at(m, p).unwrap();
        self.run(m, |m| m.list(ino))
            .unwrap()
            .into_iter()
            .map(|l| l.name.into_string().unwrap())
            .collect()
    }

    /// A file's contents as the file system shows it.
    pub fn read(&self, m: &mut Model, p: &str) -> Option<Vec<u8>> {
        let ino = self.at(m, p)?;
        match self.run(m, |m| m.open(ino, libc::O_RDONLY)).unwrap() {
            Opened::Overlay => {
                let data = self.run(m, |m| m.read_overlay(ino, 0, 1 << 20)).unwrap();
                m.release(ino);
                Some(data)
            }
            Opened::Value { id, .. } => {
                m.release(ino);
                Some(self.files[&id].clone())
            }
        }
    }

    pub fn write(&self, m: &mut Model, p: &str, data: &[u8]) {
        let ino = match self.at(m, p) {
            Some(i) => i,
            None => {
                let (dir, n) = path(p)
                    .split()
                    .map(|(d, n)| (d.to_owned(), n.as_internal_str().to_owned()))
                    .unwrap();
                let parent = self.at(m, dir.as_internal_file_string()).unwrap();
                self.run(m, |m| m.create(parent, name(&n), Kind::File, 0o644, b""))
                    .unwrap()
            }
        };
        self.run(m, |m| m.open(ino, libc::O_WRONLY | libc::O_TRUNC))
            .unwrap();
        self.run(m, |m| m.write(ino, 0, data)).unwrap();
        m.release(ino);
    }
}

pub fn name(s: &str) -> &OsStr {
    OsStr::new(s)
}

pub fn path(s: &str) -> RepoPathBuf {
    RepoPathBuf::from_internal_string(s).unwrap()
}

/// The fixture as a mount's source; what it was asked to prefetch, in `prefetched`. Mounting is
/// Linux's alone, so its tests are.
pub struct FixtureFetch(pub Arc<Fixture>, pub Arc<std::sync::Mutex<Vec<FileId>>>);

fn missing() -> io::Error {
    io::Error::from_raw_os_error(libc::EIO)
}

impl Fetch for FixtureFetch {
    fn tree(&self, id: &TreeId) -> io::Result<Arc<BackendTree>> {
        self.0.trees.get(id).cloned().ok_or_else(missing)
    }
    fn sizes(&self, ids: &[FileId]) -> io::Result<HashMap<FileId, u64>> {
        ids.iter()
            .map(|id| {
                Ok((
                    id.clone(),
                    self.0.files.get(id).ok_or_else(missing)?.len() as u64,
                ))
            })
            .collect()
    }
    fn symlink(&self, id: &SymlinkId) -> io::Result<Vec<u8>> {
        self.0.symlinks.get(id).cloned().ok_or_else(missing)
    }
    fn content(&self, id: &FileId, to: &std::path::Path) -> io::Result<()> {
        std::fs::write(to, self.0.files.get(id).ok_or_else(missing)?)
    }
    fn read(&self, id: &FileId, _size: u64, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let f = self.0.files.get(id).ok_or_else(missing)?;
        let start = (offset as usize).min(f.len());
        let end = (start + len as usize).min(f.len());
        Ok(f[start..end].to_vec())
    }

    fn prefetch(&self, files: Vec<(FileId, Option<u64>)>, _max: u64) {
        self.1
            .lock()
            .unwrap()
            .extend(files.into_iter().map(|(id, _)| id));
    }
}

/// A small tree of every kind of entry: files, an executable, a symlink, and directories.
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
