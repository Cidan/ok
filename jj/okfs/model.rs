//! The file system a virtual workspace presents (docs/okfs.md, "The file system"): inodes over
//! a base tree, an overlay of what was written, moved, and deleted, and a checkout that moves the
//! base without touching what the recorded tree does not hold.
//!
//! The model is synchronous and holds no network: an operation that needs a tree, a file's size, a
//! symlink's target, or a file's contents it has not cached returns [`Error::Need`], and the caller
//! fetches it into [`Cache`] and calls again. Operations that mutate check their needs before they
//! change anything, so a retried call starts from the state the first saw.
//!
//! - A **derived** inode's content is its parent directory's source at its name: the base tree at
//!   its path, for a directory that follows the base, or a moved tree's entry. It is resolved
//!   lazily and again after every checkout (the epoch).
//! - A **fixed** inode is a tree value placed somewhere new (a rename, or a chmod): its content
//!   is still ok's, unchanged.
//! - A **materialized** inode's content is the overlay's: a written file's bytes in
//!   `overlay/<ino>`, a symlink's target, or a directory's slots.
//!
//! A directory with slots is an **overlay directory**: its slots (written or moved children, and
//! whiteouts of source entries deleted) are listed over its source (`Under`): the base tree at its
//! path (`Base`), a fixed tree (`Tree`, a moved directory), or nothing (`Empty`, made by mkdir).
//! An overlay directory's parent is an overlay directory.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::ffi::OsStringExt as _;
use std::os::unix::fs::FileExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use jj_lib::backend::CopyId;
use jj_lib::backend::FileId;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Tree as BackendTree;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::merge::Merge;
use jj_lib::merge::SameChange;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::repo_path::RepoPathComponent;
use ok_proto::ok::okfs::v1 as pb;

use crate::acl;
use crate::acl::Xattrs;
use crate::persist::Log;

pub type Ino = u64;
pub const ROOT: Ino = 1;
/// The `.jj` symlink at the root, to the workspace's jj state on disk.
pub const JJ: Ino = 2;
const FIRST_FREE: Ino = 3;
pub const JJ_NAME: &str = ".jj";

/// Seconds and nanoseconds since the epoch.
pub type Time = (i64, u32);

pub fn now() -> Time {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i64, d.subsec_nanos())
}

/// A tree, as a merge's terms; a term is None where that side has no tree.
pub type Terms = Merge<Option<TreeId>>;

pub fn resolved_terms(id: TreeId) -> Terms {
    Merge::resolved(Some(id))
}

/// What an entry of a tree holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    File {
        id: FileId,
        executable: bool,
        copy_id: CopyId,
    },
    Symlink(SymlinkId),
    Tree(Terms),
    /// An unresolved conflict of non-trees: shown only where a checkout materialized it.
    Conflict,
}

impl Value {
    fn kind(&self) -> Option<Kind> {
        match self {
            Value::File { .. } => Some(Kind::File),
            Value::Symlink(_) => Some(Kind::Symlink),
            Value::Tree(_) => Some(Kind::Dir),
            Value::Conflict => None,
        }
    }

    fn from_tree_value(v: &TreeValue) -> Option<Value> {
        Some(match v {
            TreeValue::File {
                id,
                executable,
                copy_id,
            } => Value::File {
                id: id.clone(),
                executable: *executable,
                copy_id: copy_id.clone(),
            },
            TreeValue::Symlink(id) => Value::Symlink(id.clone()),
            TreeValue::Tree(id) => Value::Tree(resolved_terms(id.clone())),
            TreeValue::GitSubmodule(_) => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Dir,
    File,
    Symlink,
    Fifo,
    Socket,
}

impl Kind {
    pub fn mode_bits(self) -> u32 {
        match self {
            Kind::Dir => okfs_fuse::abi::S_IFDIR,
            Kind::File => okfs_fuse::abi::S_IFREG,
            Kind::Symlink => okfs_fuse::abi::S_IFLNK,
            Kind::Fifo => okfs_fuse::abi::S_IFIFO,
            Kind::Socket => okfs_fuse::abi::S_IFSOCK,
        }
    }
}

/// Something the model has not cached: the caller fetches it into [`Cache`] and calls again.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Need {
    Tree(TreeId),
    /// Several trees at once (a checkout's).
    Trees(Vec<TreeId>),
    Size(FileId),
    Symlink(SymlinkId),
    /// A file's whole contents, to materialize it.
    Content(FileId),
}

#[derive(Debug)]
pub enum Error {
    Errno(i32),
    Need(Need),
}

impl From<i32> for Error {
    fn from(e: i32) -> Self {
        Error::Errno(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Errno(e.raw_os_error().unwrap_or(libc::EIO))
    }
}

pub type R<T> = Result<T, Error>;

/// What the model reads from ok, cached by the caller.
#[derive(Default)]
pub struct Cache {
    pub trees: HashMap<TreeId, Arc<BackendTree>>,
    pub sizes: HashMap<FileId, u64>,
    pub symlinks: HashMap<SymlinkId, Vec<u8>>,
    /// Files whose whole contents were fetched to materialize them.
    pub contents: HashMap<FileId, PathBuf>,
    listings: HashMap<Terms, Arc<BTreeMap<String, Value>>>,
}

impl Cache {
    fn tree(&self, id: &TreeId) -> R<&Arc<BackendTree>> {
        self.trees
            .get(id)
            .ok_or_else(|| Error::Need(Need::Tree(id.clone())))
    }
}

/// Permissions, times, and extended attributes (ACLs among them, `acl`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Meta {
    pub mode: u32,
    pub mtime: Time,
    pub ctime: Time,
    pub atime: Time,
    pub xattrs: Xattrs,
}

#[derive(Clone, Debug)]
enum Content {
    Derived,
    Fixed(Value),
    Materialized,
}

/// An overlay directory's source for names its slots do not list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Under {
    Base,
    Tree(Terms),
    Empty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Slot {
    Node(Ino),
    Whiteout,
}

#[derive(Debug, Default)]
struct DirState {
    /// Some for an overlay directory.
    overlay: Option<Overlay>,
    /// Children derived from the source, by name, once looked up.
    children: HashMap<OsString, Ino>,
}

#[derive(Debug)]
struct Overlay {
    under: Under,
    slots: BTreeMap<OsString, Slot>,
}

#[derive(Debug)]
struct Inode {
    /// The primary link; parent 0 for an inode no name reaches (kept while the kernel or an
    /// open handle holds it).
    parent: Ino,
    name: OsString,
    extra: Vec<(Ino, OsString)>,
    kind: Kind,
    content: Content,
    /// A derived inode's value, as resolved at an epoch.
    derived: Option<(u64, Value)>,
    dir: Option<DirState>,
    /// A materialized symlink's target.
    target: Vec<u8>,
    meta: Meta,
    /// A materialized file's content version: bumped by every change of its bytes.
    version: u64,
    /// A materialized file's size.
    size: u64,
    /// A materialized file's content ID, known for a version.
    file_id: Option<(u64, FileId)>,
    checkout_conflict: bool,
    /// The conflict markers' length a checkout wrote its conflict with.
    marker_len: u32,
    /// A redirection: a symlink the daemon keeps to a directory on local disk.
    redirect: bool,
    nlookup: u64,
    open: u32,
}

/// How an opened file is read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Opened {
    Overlay,
    /// A file whose content is ok's, captured at open.
    Value {
        id: FileId,
        size: u64,
    },
}

/// An entry of a directory as listed.
#[derive(Clone, Debug)]
pub struct Listed {
    pub name: OsString,
    pub kind: Kind,
}

/// A kernel cache entry a change made stale: an entry (parent, name), or an inode's data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stale {
    Entry(Ino, OsString),
    Inode(Ino),
}

/// A path that changed, for the journal.
#[derive(Clone, Debug)]
pub struct Changed {
    pub paths: Vec<String>,
    pub checkout: bool,
}

/// A conflict a checkout writes as a file: its materialized contents, and the length of its
/// markers.
#[derive(Clone, Debug)]
pub struct Conflict {
    pub path: RepoPathBuf,
    pub contents: Vec<u8>,
    pub executable: bool,
    pub marker_len: u32,
}

pub struct Model {
    inodes: HashMap<Ino, Inode>,
    next: Ino,
    base: Terms,
    recorded: Terms,
    recorded_commit: Vec<u8>,
    sparse: Vec<RepoPathBuf>,
    epoch: u64,
    pub cache: Cache,
    /// A root tree's directory at a path, by (root, path).
    base_dirs: HashMap<(Terms, RepoPathBuf), Option<Terms>>,
    overlay_dir: PathBuf,
    files: HashMap<Ino, File>,
    log: Box<dyn Log>,
    dirty: BTreeSet<Ino>,
    dropped: BTreeSet<Ino>,
    uid: u32,
    gid: u32,
    jj_target: Vec<u8>,
    jj_time: Time,
    changed: Vec<Changed>,
    /// Bumped by every change to the file system (not by lookups).
    seq: u64,
    clock: fn() -> Time,
}

fn errno<T>(e: i32) -> R<T> {
    Err(Error::Errno(e))
}

fn name_str(name: &OsStr) -> Option<&str> {
    std::str::from_utf8(name.as_bytes()).ok()
}

fn component(name: &OsStr) -> Option<&RepoPathComponent> {
    name_str(name).and_then(|s| RepoPathComponent::new(s).ok())
}

impl Model {
    /// A model with nothing written: the base tree and the `.jj` link, its overlay files in
    /// `overlay_dir`. Its first record is the base.
    pub fn new(
        base: Terms,
        overlay_dir: PathBuf,
        jj_target: Vec<u8>,
        log: Box<dyn Log>,
        clock: fn() -> Time,
    ) -> io::Result<Self> {
        let mut m = Self::empty(base, overlay_dir, jj_target, log, clock)?;
        m.log_base().map_err(|e| match e {
            Error::Errno(n) => io::Error::from_raw_os_error(n),
            Error::Need(_) => io::Error::other("a need while logging"),
        })?;
        Ok(m)
    }

    fn empty(
        base: Terms,
        overlay_dir: PathBuf,
        jj_target: Vec<u8>,
        log: Box<dyn Log>,
        clock: fn() -> Time,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(&overlay_dir)?;
        let t = clock();
        let mut m = Model {
            inodes: HashMap::new(),
            next: FIRST_FREE,
            recorded: base.clone(),
            base,
            recorded_commit: Vec::new(),
            sparse: vec![RepoPathBuf::root()],
            epoch: 1,
            cache: Cache::default(),
            base_dirs: HashMap::new(),
            overlay_dir,
            files: HashMap::new(),
            log,
            dirty: BTreeSet::new(),
            dropped: BTreeSet::new(),
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            jj_target,
            jj_time: t,
            changed: Vec::new(),
            seq: 0,
            clock,
        };
        m.inodes.insert(ROOT, Self::root_inode(t));
        let mut jj = Self::root_inode(t);
        jj.parent = ROOT;
        jj.name = OsString::from(JJ_NAME);
        jj.kind = Kind::Symlink;
        jj.dir = None;
        jj.meta.mode = 0o777;
        jj.target = m.jj_target.clone();
        m.inodes.insert(JJ, jj);
        Ok(m)
    }

    fn root_inode(t: Time) -> Inode {
        Inode {
            parent: 0,
            name: OsString::new(),
            extra: Vec::new(),
            kind: Kind::Dir,
            content: Content::Materialized,
            derived: None,
            dir: Some(DirState {
                overlay: Some(Overlay {
                    under: Under::Base,
                    slots: BTreeMap::new(),
                }),
                children: HashMap::new(),
            }),
            target: Vec::new(),
            meta: Meta {
                mode: 0o755,
                mtime: t,
                ctime: t,
                atime: t,
                xattrs: BTreeMap::new(),
            },
            version: 0,
            size: 0,
            file_id: None,
            checkout_conflict: false,
            marker_len: 0,
            redirect: false,
            nlookup: 1,
            open: 0,
        }
    }

    /// Whether the root shows `.jj`: a workspace's does, a presentation's (an empty target) does
    /// not.
    fn has_jj(&self) -> bool {
        !self.jj_target.is_empty()
    }

    pub fn base(&self) -> &Terms {
        &self.base
    }

    pub fn recorded(&self) -> (&Terms, &[u8]) {
        (&self.recorded, &self.recorded_commit)
    }

    pub fn sparse(&self) -> &[RepoPathBuf] {
        &self.sparse
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn overlay_path(&self, ino: Ino) -> PathBuf {
        self.overlay_dir.join(ino.to_string())
    }

    /// The changes made since the last call, for the journal.
    pub fn take_changed(&mut self) -> Vec<Changed> {
        std::mem::take(&mut self.changed)
    }

    fn inode(&self, ino: Ino) -> R<&Inode> {
        self.inodes.get(&ino).ok_or(Error::Errno(libc::ESTALE))
    }

    fn inode_mut(&mut self, ino: Ino) -> R<&mut Inode> {
        self.inodes.get_mut(&ino).ok_or(Error::Errno(libc::ESTALE))
    }

    fn touch(&mut self, ino: Ino) {
        self.dirty.insert(ino);
    }

    fn note(&mut self, ino: Ino) {
        self.seq += 1;
        if let Some(p) = self.path_of(ino) {
            self.changed.push(Changed {
                paths: vec![p.as_internal_file_string().to_owned()],
                checkout: false,
            });
        }
    }

    // ─── Paths and sources ───

    /// An inode's path, through its primary links; None for an unlinked inode or a name that is
    /// not UTF-8.
    pub fn path_of(&self, ino: Ino) -> Option<RepoPathBuf> {
        let mut names = Vec::new();
        let mut cur = ino;
        while cur != ROOT {
            let i = self.inodes.get(&cur)?;
            if i.parent == 0 {
                return None;
            }
            names.push(name_str(&i.name)?.to_owned());
            cur = i.parent;
        }
        names.reverse();
        RepoPathBuf::from_internal_string(names.join("/")).ok()
    }

    /// Whether an inode's content follows the base tree at its path: a derived inode or an
    /// overlay directory over the base, all the way to the root.
    fn follows_base(&self, ino: Ino) -> bool {
        let mut cur = ino;
        loop {
            if cur == ROOT {
                return true;
            }
            let Some(i) = self.inodes.get(&cur) else {
                return false;
            };
            if i.parent == 0 {
                return false;
            }
            match &i.content {
                Content::Derived => {}
                Content::Fixed(_) => return false,
                Content::Materialized => match i.dir.as_ref().and_then(|d| d.overlay.as_ref()) {
                    Some(ov) if ov.under == Under::Base => {}
                    _ => return false,
                },
            }
            cur = i.parent;
        }
    }

    /// A tree's entries, merged across its terms: a term with no tree has none; an entry the terms
    /// disagree on is a merged directory when every side is a directory, and a conflict
    /// otherwise.
    fn entries(&mut self, terms: &Terms) -> R<Arc<BTreeMap<String, Value>>> {
        if let Some(l) = self.cache.listings.get(terms) {
            return Ok(l.clone());
        }
        let mut trees = Vec::new();
        for t in terms.iter().flatten() {
            trees.push(self.cache.tree(t)?.clone());
        }
        let mut out = BTreeMap::new();
        if let Some(t) = terms.as_resolved() {
            if let Some(id) = t {
                for e in self.cache.tree(id)?.entries() {
                    if let Some(v) = Value::from_tree_value(e.value()) {
                        out.insert(e.name().as_internal_str().to_owned(), v);
                    }
                }
            }
        } else {
            let mut names = BTreeSet::new();
            for t in &trees {
                for n in t.names() {
                    names.insert(n.as_internal_str().to_owned());
                }
            }
            for name in names {
                let c = RepoPathComponent::new(&name).expect("a tree's entry names are valid");
                let values: Merge<Option<TreeValue>> = terms.map(|t| {
                    t.as_ref()
                        .and_then(|id| self.cache.trees[id].value(c).cloned())
                });
                if let Some(v) = values.resolve_trivial(SameChange::Accept) {
                    if let Some(v) = v.as_ref().and_then(Value::from_tree_value) {
                        out.insert(name, v);
                    }
                } else if values
                    .iter()
                    .all(|v| v.as_ref().is_none_or(|v| matches!(v, TreeValue::Tree(_))))
                {
                    out.insert(
                        name,
                        Value::Tree(values.map(|v| match v {
                            Some(TreeValue::Tree(id)) => Some(id.clone()),
                            _ => None,
                        })),
                    );
                } else {
                    out.insert(name, Value::Conflict);
                }
            }
        }
        let out = Arc::new(out);
        self.cache.listings.insert(terms.clone(), out.clone());
        Ok(out)
    }

    /// The base tree at a directory path: None where it has no directory.
    fn base_at(&mut self, root: &Terms, path: &RepoPath) -> R<Option<Terms>> {
        let key = (root.clone(), path.to_owned());
        if let Some(t) = self.base_dirs.get(&key) {
            return Ok(t.clone());
        }
        let t = match path.split() {
            None => Some(root.clone()),
            Some((parent, name)) => match self.base_at(root, parent)? {
                None => None,
                Some(p) => match self.entries(&p)?.get(name.as_internal_str()) {
                    Some(Value::Tree(t)) => Some(t.clone()),
                    _ => None,
                },
            },
        };
        self.base_dirs.insert(key, t.clone());
        Ok(t)
    }

    /// A directory's source: the tree its unlisted names come from.
    fn source(&mut self, dir: Ino) -> R<Option<Terms>> {
        let under = {
            let i = self.inode(dir)?;
            i.dir
                .as_ref()
                .and_then(|d| d.overlay.as_ref())
                .map(|ov| ov.under.clone())
        };
        match under {
            Some(Under::Base) => match self.path_of(dir) {
                Some(p) => {
                    let base = self.base.clone();
                    self.base_at(&base, &p)
                }
                None => Ok(None),
            },
            Some(Under::Tree(t)) => Ok(Some(t)),
            Some(Under::Empty) => Ok(None),
            None => match self.value_of(dir)? {
                Value::Tree(t) => Ok(Some(t)),
                _ => Ok(None),
            },
        }
    }

    /// A derived or fixed inode's value.
    fn value_of(&mut self, ino: Ino) -> R<Value> {
        let (content, derived, parent, name) = {
            let i = self.inode(ino)?;
            (
                i.content.clone(),
                i.derived.clone(),
                i.parent,
                i.name.clone(),
            )
        };
        match content {
            Content::Fixed(v) => Ok(v),
            Content::Materialized => errno(libc::EINVAL),
            Content::Derived => {
                if let Some((epoch, v)) = &derived
                    && (*epoch == self.epoch || parent == 0)
                {
                    return Ok(v.clone());
                }
                let src = self.source(parent)?;
                let v = match (src, name_str(&name)) {
                    (Some(src), Some(n)) => self.entries(&src)?.get(n).cloned(),
                    _ => None,
                };
                match v {
                    Some(v) => {
                        self.inode_mut(ino)?.derived = Some((self.epoch, v.clone()));
                        Ok(v)
                    }
                    None => match derived {
                        Some((_, v)) => Ok(v),
                        None => errno(libc::ESTALE),
                    },
                }
            }
        }
    }

    fn sparse_allows(&self, path: &RepoPath, is_dir: bool) -> bool {
        self.sparse
            .iter()
            .any(|p| path.starts_with(p) || (is_dir && p.starts_with(path)))
    }

    // ─── Reading ───

    /// The child `name` of directory `parent`, looked up (counting a kernel reference when
    /// `count`).
    pub fn lookup(&mut self, parent: Ino, name: &OsStr, count: bool) -> R<Option<Ino>> {
        let found = self.find(parent, name)?;
        if let Some(ino) = found
            && count
        {
            self.inode_mut(ino)?.nlookup += 1;
        }
        self.flush()?;
        Ok(found)
    }

    /// The child `name` of `parent`, without counting a reference.
    fn find(&mut self, parent: Ino, name: &OsStr) -> R<Option<Ino>> {
        let p = self.inode(parent)?;
        if p.kind != Kind::Dir {
            return errno(libc::ENOTDIR);
        }
        if parent == ROOT && name == JJ_NAME && self.has_jj() {
            return Ok(Some(JJ));
        }
        let d = p.dir.as_ref().expect("a directory has its state");
        if let Some(ov) = &d.overlay {
            match ov.slots.get(name) {
                Some(Slot::Whiteout) => return Ok(None),
                Some(Slot::Node(ino)) => return Ok(Some(*ino)),
                None => {}
            }
        }
        let cached = d.children.get(name).copied();
        let Some(n) = name_str(name) else {
            return Ok(None);
        };
        let Some(src) = self.source(parent)? else {
            return Ok(None);
        };
        let Some(value) = self.entries(&src)?.get(n).cloned() else {
            return Ok(None);
        };
        let Some(kind) = value.kind() else {
            return Ok(None);
        };
        if self.follows_base(parent)
            && let Some(path) = self
                .path_of(parent)
                .map(|p| p.join(component(name).unwrap()))
            && !self.sparse_allows(&path, kind == Kind::Dir)
        {
            return Ok(None);
        }
        if let Some(ino) = cached {
            if self.inode(ino)?.kind == kind {
                self.inode_mut(ino)?.derived = Some((self.epoch, value));
                return Ok(Some(ino));
            }
            // The base changed the entry's kind: the old inode is detached, keeping its last value.
            self.detach_derived(ino);
        }
        let ino = self.alloc(parent, name.to_owned(), kind, Content::Derived, Some(value));
        self.inode_mut(parent)?
            .dir
            .as_mut()
            .unwrap()
            .children
            .insert(name.to_owned(), ino);
        Ok(Some(ino))
    }

    fn alloc(
        &mut self,
        parent: Ino,
        name: OsString,
        kind: Kind,
        content: Content,
        derived: Option<Value>,
    ) -> Ino {
        let ino = self.next;
        self.next += 1;
        let t = (self.clock)();
        let mode = match (&kind, &derived) {
            (Kind::Dir, _) => 0o755,
            (
                Kind::File,
                Some(Value::File {
                    executable: true, ..
                }),
            ) => 0o755,
            (Kind::Symlink, _) => 0o777,
            _ => 0o644,
        };
        self.inodes.insert(
            ino,
            Inode {
                parent,
                name,
                extra: Vec::new(),
                kind,
                content,
                derived: derived.map(|v| (self.epoch, v)),
                dir: (kind == Kind::Dir).then(DirState::default),
                target: Vec::new(),
                meta: Meta {
                    mode,
                    mtime: t,
                    ctime: t,
                    atime: t,
                    xattrs: BTreeMap::new(),
                },
                version: 0,
                size: 0,
                file_id: None,
                checkout_conflict: false,
                marker_len: 0,
                redirect: false,
                nlookup: 0,
                open: 0,
            },
        );
        self.touch(ino);
        ino
    }

    /// Takes a derived inode out of its parent's children, keeping its last value: what an inode
    /// the kernel or a handle still holds becomes when its path no longer resolves to it.
    fn detach_derived(&mut self, ino: Ino) {
        let Some(i) = self.inodes.get_mut(&ino) else {
            return;
        };
        let (parent, name) = (i.parent, i.name.clone());
        if let Content::Derived = i.content
            && let Some((_, v)) = i.derived.take()
        {
            i.content = Content::Fixed(v);
        }
        i.parent = 0;
        if let Some(d) = self.inodes.get_mut(&parent).and_then(|p| p.dir.as_mut())
            && d.children.get(&name) == Some(&ino)
        {
            d.children.remove(&name);
        }
        self.touch(ino);
        self.maybe_drop(ino);
    }

    /// An inode's attributes.
    pub fn attr(&mut self, ino: Ino) -> R<okfs_fuse::Attr> {
        let (kind, content, meta, size, target_len, extra, parent) = {
            let i = self.inode(ino)?;
            (
                i.kind,
                i.content.clone(),
                i.meta.clone(),
                i.size,
                i.target.len(),
                i.extra.len(),
                i.parent,
            )
        };
        let (perm, size) = if ino == JJ {
            (0o777, self.jj_target.len() as u64)
        } else {
            match (kind, &content) {
                (Kind::File, Content::Materialized) => (meta.mode, size),
                (Kind::File, _) => match self.value_of(ino)? {
                    Value::File { id, executable, .. } => {
                        let size = *self
                            .cache
                            .sizes
                            .get(&id)
                            .ok_or_else(|| Error::Need(Need::Size(id.clone())))?;
                        let perm = match content {
                            Content::Derived => {
                                if executable {
                                    0o755
                                } else {
                                    0o644
                                }
                            }
                            _ => meta.mode,
                        };
                        (perm, size)
                    }
                    _ => (meta.mode, 0),
                },
                (Kind::Symlink, Content::Materialized) => (0o777, target_len as u64),
                (Kind::Symlink, _) => match self.value_of(ino)? {
                    Value::Symlink(id) => (0o777, self.symlink_target(&id)?.len() as u64),
                    _ => (0o777, 0),
                },
                (Kind::Dir, _) => (meta.mode, 4096),
                _ => (meta.mode, 0),
            }
        };
        let nlink = match kind {
            Kind::Dir => 2,
            _ if parent == 0 && ino != ROOT => extra as u32,
            _ => 1 + extra as u32,
        };
        let (mtime, ctime, atime) = if ino == JJ {
            (self.jj_time, self.jj_time, self.jj_time)
        } else {
            (meta.mtime, meta.ctime, meta.atime)
        };
        Ok(okfs_fuse::Attr {
            ino,
            size,
            atime,
            mtime,
            ctime,
            mode: kind.mode_bits() | (perm & 0o7777),
            nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
        })
    }

    fn symlink_target(&self, id: &SymlinkId) -> R<Vec<u8>> {
        self.cache
            .symlinks
            .get(id)
            .cloned()
            .ok_or_else(|| Error::Need(Need::Symlink(id.clone())))
    }

    pub fn readlink(&mut self, ino: Ino) -> R<Vec<u8>> {
        if ino == JJ {
            return Ok(self.jj_target.clone());
        }
        let (kind, content, target) = {
            let i = self.inode(ino)?;
            (i.kind, i.content.clone(), i.target.clone())
        };
        if kind != Kind::Symlink {
            return errno(libc::EINVAL);
        }
        match content {
            Content::Materialized => Ok(target),
            _ => match self.value_of(ino)? {
                Value::Symlink(id) => self.symlink_target(&id),
                _ => errno(libc::EINVAL),
            },
        }
    }

    /// A directory's entries as they stand, by name (`.` and `..` not included).
    pub fn list(&mut self, dir: Ino) -> R<Vec<Listed>> {
        let i = self.inode(dir)?;
        if i.kind != Kind::Dir {
            return errno(libc::ENOTDIR);
        }
        let slots: Vec<(OsString, Slot)> = i
            .dir
            .as_ref()
            .unwrap()
            .overlay
            .as_ref()
            .map(|ov| ov.slots.iter().map(|(n, s)| (n.clone(), *s)).collect())
            .unwrap_or_default();
        let mut out: BTreeMap<OsString, Kind> = BTreeMap::new();
        if let Some(src) = self.source(dir)? {
            let base_path = if self.follows_base(dir) {
                self.path_of(dir)
            } else {
                None
            };
            for (name, v) in self.entries(&src)?.iter() {
                let Some(kind) = v.kind() else {
                    continue;
                };
                if let Some(p) = &base_path {
                    let c = RepoPathComponent::new(name).unwrap();
                    if !self.sparse_allows(&p.join(c), kind == Kind::Dir) {
                        continue;
                    }
                }
                out.insert(OsString::from(name), kind);
            }
        }
        for (name, slot) in slots {
            match slot {
                Slot::Whiteout => {
                    out.remove(&name);
                }
                Slot::Node(ino) => {
                    out.insert(name, self.inode(ino)?.kind);
                }
            }
        }
        if dir == ROOT && self.has_jj() {
            out.insert(OsString::from(JJ_NAME), Kind::Symlink);
        }
        Ok(out
            .into_iter()
            .map(|(name, kind)| Listed { name, kind })
            .collect())
    }

    /// The content IDs of a directory's files that are ok's (not written here), but `except`'s,
    /// in name order, with their sizes when known, leaving out those known to be larger than
    /// `max`: what a directory prefetch reads.
    pub fn prefetchable(
        &mut self,
        dir: Ino,
        max: u64,
        except: Ino,
    ) -> R<Vec<(FileId, Option<u64>)>> {
        let except = match self.inodes.get(&except).map(|i| i.content.clone()) {
            Some(Content::Derived) | Some(Content::Fixed(_)) => match self.value_of(except)? {
                Value::File { id, .. } => Some(id),
                _ => None,
            },
            _ => None,
        };
        let shadowed: BTreeSet<OsString> = self
            .inode(dir)?
            .dir
            .as_ref()
            .and_then(|d| d.overlay.as_ref())
            .map(|ov| ov.slots.keys().cloned().collect())
            .unwrap_or_default();
        let mut out = Vec::new();
        if let Some(src) = self.source(dir)? {
            for (name, v) in self.entries(&src)?.iter() {
                if shadowed.contains(OsStr::new(name)) {
                    continue;
                }
                if let Value::File { id, .. } = v
                    && Some(id) != except.as_ref()
                    && self.cache.sizes.get(id).is_none_or(|s| *s <= max)
                {
                    out.push((id.clone(), self.cache.sizes.get(id).copied()));
                }
            }
        }
        Ok(out)
    }

    /// The file IDs of a directory's derived and fixed files whose sizes are not cached: what a
    /// listing with attributes will need, to fetch at once.
    pub fn missing_sizes(&mut self, dir: Ino) -> R<Vec<FileId>> {
        let mut out = Vec::new();
        if let Some(src) = self.source(dir)? {
            for v in self.entries(&src)?.values() {
                if let Value::File { id, .. } = v
                    && !self.cache.sizes.contains_key(id)
                {
                    out.push(id.clone());
                }
            }
        }
        Ok(out)
    }

    // ─── Kernel references and handles ───

    /// Counts a reference the kernel took to an inode (a lookup's reply).
    pub fn lookup_count(&mut self, ino: Ino) -> R<()> {
        self.inode_mut(ino)?.nlookup += 1;
        Ok(())
    }

    pub fn parent_of(&self, ino: Ino) -> Option<Ino> {
        self.inodes.get(&ino).map(|i| i.parent).filter(|p| *p != 0)
    }

    pub fn forget(&mut self, ino: Ino, n: u64) {
        if let Some(i) = self.inodes.get_mut(&ino) {
            i.nlookup = i.nlookup.saturating_sub(n);
        }
        self.maybe_drop(ino);
        let _ = self.flush();
    }

    /// Inodes no name reaches, kept for a reference the kernel holds: what NFS, which never says
    /// it is done with a handle, keeps until it lets them go (nfs.rs).
    pub fn detached(&self) -> Vec<Ino> {
        self.inodes
            .iter()
            .filter(|(ino, i)| **ino != ROOT && **ino != JJ && i.parent == 0 && i.extra.is_empty())
            .map(|(ino, _)| *ino)
            .collect()
    }

    /// Closes a file's overlay descriptor, unless a handle has the file open: one a write or
    /// read over NFS, which has no close, opened.
    pub fn close_idle(&mut self, ino: Ino) {
        if self.inodes.get(&ino).is_some_and(|i| i.open == 0) {
            self.files.remove(&ino);
        }
    }

    /// Overlay files open.
    #[cfg(test)]
    pub fn overlay_fds(&self) -> usize {
        self.files.len()
    }

    /// Every inode the model holds.
    pub fn inos(&self) -> Vec<Ino> {
        self.inodes.keys().copied().collect()
    }

    pub fn is_dir(&self, ino: Ino) -> bool {
        self.inodes.get(&ino).is_some_and(|i| i.kind == Kind::Dir)
    }

    /// Drops an inode no name, kernel reference, or handle holds any more.
    fn maybe_drop(&mut self, ino: Ino) {
        let Some(i) = self.inodes.get(&ino) else {
            return;
        };
        if ino == ROOT
            || ino == JJ
            || i.parent != 0
            || !i.extra.is_empty()
            || i.nlookup > 0
            || i.open > 0
        {
            return;
        }
        let i = self.inodes.remove(&ino).unwrap();
        self.files.remove(&ino);
        if matches!(i.content, Content::Materialized) && i.kind == Kind::File {
            let _ = std::fs::remove_file(self.overlay_path(ino));
        }
        // A detached directory's own children go with it.
        if let Some(d) = i.dir {
            let mut kids: Vec<Ino> = d.children.values().copied().collect();
            if let Some(ov) = d.overlay {
                kids.extend(ov.slots.values().filter_map(|s| match s {
                    Slot::Node(c) => Some(*c),
                    Slot::Whiteout => None,
                }));
            }
            for k in kids {
                if let Some(c) = self.inodes.get_mut(&k)
                    && c.parent == ino
                {
                    c.parent = 0;
                    self.dirty.insert(k);
                }
                self.maybe_drop(k);
            }
        }
        self.dirty.remove(&ino);
        self.dropped.insert(ino);
    }

    /// Opens a file: a write or truncation materializes it first.
    pub fn open(&mut self, ino: Ino, flags: i32) -> R<Opened> {
        let i = self.inode(ino)?;
        match i.kind {
            Kind::Dir => return errno(libc::EISDIR),
            Kind::File => {}
            _ => return errno(libc::EINVAL),
        }
        let writing = flags & libc::O_ACCMODE != libc::O_RDONLY || flags & libc::O_TRUNC != 0;
        if writing {
            self.materialize_file(ino)?;
            if flags & libc::O_TRUNC != 0 {
                self.truncate(ino, 0)?;
            }
        }
        let opened = match self.inode(ino)?.content {
            Content::Materialized => Opened::Overlay,
            _ => match self.value_of(ino)? {
                Value::File { id, .. } => {
                    let size = *self
                        .cache
                        .sizes
                        .get(&id)
                        .ok_or_else(|| Error::Need(Need::Size(id.clone())))?;
                    Opened::Value { id, size }
                }
                _ => return errno(libc::EIO),
            },
        };
        self.inode_mut(ino)?.open += 1;
        self.flush()?;
        Ok(opened)
    }

    /// How a file reads now, for a handle this daemon did not open (a predecessor did, before a
    /// restart): its overlay contents, or ok's value.
    pub fn current(&mut self, ino: Ino) -> R<Opened> {
        if let Content::Materialized = self.inode(ino)?.content {
            return Ok(Opened::Overlay);
        }
        match self.value_of(ino)? {
            Value::File { id, .. } => {
                let size = *self
                    .cache
                    .sizes
                    .get(&id)
                    .ok_or_else(|| Error::Need(Need::Size(id.clone())))?;
                Ok(Opened::Value { id, size })
            }
            _ => errno(libc::EINVAL),
        }
    }

    pub fn release(&mut self, ino: Ino) {
        if let Some(i) = self.inodes.get_mut(&ino) {
            i.open = i.open.saturating_sub(1);
            if i.open == 0 {
                self.files.remove(&ino);
            }
        }
        self.maybe_drop(ino);
        let _ = self.flush();
    }

    fn overlay_file(&mut self, ino: Ino) -> R<&File> {
        if !self.files.contains_key(&ino) {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.overlay_path(ino))?;
            self.files.insert(ino, f);
        }
        Ok(&self.files[&ino])
    }

    pub fn read_overlay(&mut self, ino: Ino, offset: u64, size: u32) -> R<Vec<u8>> {
        let len = self.inode(ino)?.size;
        if offset >= len {
            return Ok(Vec::new());
        }
        let n = (size as u64).min(len - offset) as usize;
        let mut buf = vec![0u8; n];
        let f = self.overlay_file(ino)?;
        let mut done = 0;
        while done < n {
            let r = f.read_at(&mut buf[done..], offset + done as u64)?;
            if r == 0 {
                break;
            }
            done += r;
        }
        buf.truncate(done);
        Ok(buf)
    }

    pub fn write(&mut self, ino: Ino, offset: u64, data: &[u8]) -> R<u32> {
        if !matches!(self.inode(ino)?.content, Content::Materialized) {
            self.materialize_file(ino)?;
        }
        self.overlay_file(ino)?.write_all_at(data, offset)?;
        let t = (self.clock)();
        let i = self.inode_mut(ino)?;
        i.size = i.size.max(offset + data.len() as u64);
        i.version += 1;
        i.checkout_conflict = false;
        i.meta.mtime = t;
        i.meta.ctime = t;
        self.touch(ino);
        self.note(ino);
        self.flush()?;
        Ok(data.len() as u32)
    }

    /// What an inode is, if it exists.
    pub fn kind_of(&self, ino: Ino) -> Option<Kind> {
        self.inodes.get(&ino).map(|i| i.kind)
    }

    /// A materialized file's content version: bumped by every change of its bytes.
    pub fn version_of(&self, ino: Ino) -> Option<u64> {
        self.inodes.get(&ino).map(|i| i.version)
    }

    /// Whether an inode is a secret's file (docs/secrets.md): a regular file beneath the root's
    /// `secret` folder, named neither `acl.yaml` nor as an OWNERS file. It walks the inode's links
    /// without building a path.
    pub fn is_secret(&self, ino: Ino) -> bool {
        let Some(i) = self.inodes.get(&ino) else {
            return false;
        };
        if i.kind != Kind::File || !i.name.to_str().is_some_and(ok_secret::secret_name) {
            return false;
        }
        let mut cur = ino;
        let mut depth = 0;
        loop {
            let Some(n) = self.inodes.get(&cur) else {
                return false;
            };
            match n.parent {
                0 => return false,
                ROOT => return depth > 0 && n.name == ok_secret::DIR,
                p => {
                    cur = p;
                    depth += 1;
                }
            }
        }
    }

    /// Records a change to a file whose bytes are not the overlay's: a secret's value, which the
    /// mount keeps in memory and never writes here (docs/secrets.md, "In a virtual workspace").
    /// The file is materialized — its overlay keeps whatever pointer it had — and its version moves,
    /// so a snapshot or checkpoint looks at it.
    pub fn secret_written(&mut self, ino: Ino) -> R<()> {
        if !matches!(self.inode(ino)?.content, Content::Materialized) {
            self.materialize_file(ino)?;
        }
        let t = (self.clock)();
        let i = self.inode_mut(ino)?;
        i.version += 1;
        i.checkout_conflict = false;
        i.meta.mtime = t;
        i.meta.ctime = t;
        self.touch(ino);
        self.note(ino);
        self.flush()?;
        Ok(())
    }

    /// Replaces a file's contents in the overlay: a secret's pointer, once its value is sealed.
    pub fn put_contents(&mut self, ino: Ino, contents: &[u8]) -> R<()> {
        if !matches!(self.inode(ino)?.content, Content::Materialized) {
            self.materialize_file(ino)?;
        }
        self.truncate(ino, 0)?;
        self.write(ino, 0, contents)?;
        Ok(())
    }

    pub fn fsync(&mut self, ino: Ino) -> R<()> {
        if let Some(f) = self.files.get(&ino) {
            f.sync_data()?;
        }
        self.log.sync()?;
        Ok(())
    }

    fn truncate(&mut self, ino: Ino, size: u64) -> R<()> {
        self.overlay_file(ino)?.set_len(size)?;
        let t = (self.clock)();
        let i = self.inode_mut(ino)?;
        i.size = size;
        i.version += 1;
        i.checkout_conflict = false;
        i.meta.mtime = t;
        i.meta.ctime = t;
        self.touch(ino);
        self.note(ino);
        Ok(())
    }

    // ─── Materializing ───

    /// Makes a directory an overlay directory, and its parents: a derived directory's slots
    /// start empty over its source (the base at its path when it follows the base).
    fn overlay_dir(&mut self, dir: Ino) -> R<()> {
        let i = self.inode(dir)?;
        if i.kind != Kind::Dir {
            return errno(libc::ENOTDIR);
        }
        if i.dir.as_ref().unwrap().overlay.is_some() {
            return Ok(());
        }
        let parent = i.parent;
        if parent == 0 {
            return errno(libc::ENOENT);
        }
        let under = if self.follows_base(dir) {
            Under::Base
        } else {
            match self.value_of(dir)? {
                Value::Tree(t) => Under::Tree(t),
                _ => Under::Empty,
            }
        };
        self.overlay_dir(parent)?;
        self.place(dir)?;
        let i = self.inode_mut(dir)?;
        i.content = Content::Materialized;
        i.derived = None;
        i.dir.as_mut().unwrap().overlay = Some(Overlay {
            under,
            slots: BTreeMap::new(),
        });
        self.touch(dir);
        Ok(())
    }

    /// Lists an inode in its parent's slots (its parent an overlay directory), out of the
    /// parent's derived children.
    fn place(&mut self, ino: Ino) -> R<()> {
        let (parent, name) = {
            let i = self.inode(ino)?;
            (i.parent, i.name.clone())
        };
        let p = self.inode_mut(parent)?.dir.as_mut().unwrap();
        if p.children.get(&name) == Some(&ino) {
            p.children.remove(&name);
        }
        p.overlay
            .as_mut()
            .expect("placed in an overlay directory")
            .slots
            .insert(name, Slot::Node(ino));
        Ok(())
    }

    /// Makes a derived file or symlink fixed: its value pinned where it is, listed in its parent's
    /// overlay.
    fn pin(&mut self, ino: Ino) -> R<()> {
        if !matches!(self.inode(ino)?.content, Content::Derived) {
            return Ok(());
        }
        let parent = self.inode(ino)?.parent;
        let v = self.value_of(ino)?;
        self.overlay_dir(parent)?;
        self.place(ino)?;
        let i = self.inode_mut(ino)?;
        i.content = Content::Fixed(v);
        i.derived = None;
        self.touch(ino);
        Ok(())
    }

    /// Makes a file's content the overlay's: its bytes copied there (fetched first), or a
    /// symlink's target.
    fn materialize_file(&mut self, ino: Ino) -> R<()> {
        let (kind, content, parent) = {
            let i = self.inode(ino)?;
            (i.kind, i.content.clone(), i.parent)
        };
        if matches!(content, Content::Materialized) {
            return Ok(());
        }
        let value = self.value_of(ino)?;
        match (kind, &value) {
            (Kind::File, Value::File { id, executable, .. }) => {
                let src = self
                    .cache
                    .contents
                    .get(id)
                    .cloned()
                    .ok_or_else(|| Error::Need(Need::Content(id.clone())))?;
                if parent != 0 {
                    self.overlay_dir(parent)?;
                }
                copy_file(&src, &self.overlay_path(ino))?;
                let size = std::fs::metadata(self.overlay_path(ino))?.len();
                let fixed_mode = match &content {
                    Content::Fixed(_) => Some(self.inode(ino)?.meta.mode),
                    _ => None,
                };
                let i = self.inode_mut(ino)?;
                i.content = Content::Materialized;
                i.derived = None;
                i.size = size;
                i.version += 1;
                i.file_id = Some((i.version, id.clone()));
                i.meta.mode = fixed_mode.unwrap_or(if *executable { 0o755 } else { 0o644 });
            }
            (Kind::Symlink, Value::Symlink(id)) => {
                let target = self.symlink_target(id)?;
                if parent != 0 {
                    self.overlay_dir(parent)?;
                }
                let i = self.inode_mut(ino)?;
                i.content = Content::Materialized;
                i.derived = None;
                i.target = target;
            }
            _ => return errno(libc::EIO),
        }
        if parent != 0 {
            self.place(ino)?;
        }
        self.touch(ino);
        Ok(())
    }

    // ─── Changing the tree ───

    /// The child `name` of an overlay directory, as a slot or a source entry: for deciding
    /// whether a name is taken, and whether deleting it needs a whiteout.
    fn source_has(&mut self, dir: Ino, name: &OsStr) -> R<bool> {
        let Some(n) = name_str(name) else {
            return Ok(false);
        };
        Ok(match self.source(dir)? {
            Some(src) => self.entries(&src)?.contains_key(n),
            None => false,
        })
    }

    fn check_name(parent: Ino, name: &OsStr) -> R<()> {
        if name.is_empty() || name == "." || name == ".." || name.as_bytes().contains(&b'/') {
            return errno(libc::EINVAL);
        }
        if name.len() > 255 {
            return errno(libc::ENAMETOOLONG);
        }
        if parent == ROOT && name == JJ_NAME {
            return errno(libc::EPERM);
        }
        Ok(())
    }

    /// Makes a new inode in an overlay directory: a file, directory, symlink, FIFO, or socket.
    pub fn create(
        &mut self,
        parent: Ino,
        name: &OsStr,
        kind: Kind,
        perm: u32,
        target: &[u8],
    ) -> R<Ino> {
        self.create_with(parent, name, kind, perm, Xattrs::new(), target)
    }

    /// Makes a new file, directory, FIFO, or socket as a process asks to, with the mode it asked
    /// for and its umask: under a directory with a default ACL, the inode inherits the ACL (and
    /// the umask is not applied); elsewhere it takes the mode less the umask (`acl::inherit`).
    pub fn create_as(
        &mut self,
        parent: Ino,
        name: &OsStr,
        kind: Kind,
        mode: u32,
        umask: u32,
    ) -> R<Ino> {
        let (perm, xattrs) = acl::inherit(
            &self.inode(parent)?.meta.xattrs,
            kind == Kind::Dir,
            mode & 0o7777,
            umask,
        );
        self.create_with(parent, name, kind, perm, xattrs, b"")
    }

    fn create_with(
        &mut self,
        parent: Ino,
        name: &OsStr,
        kind: Kind,
        perm: u32,
        xattrs: Xattrs,
        target: &[u8],
    ) -> R<Ino> {
        Self::check_name(parent, name)?;
        if self.find(parent, name)?.is_some() {
            return errno(libc::EEXIST);
        }
        self.overlay_dir(parent)?;
        let ino = self.alloc(parent, name.to_owned(), kind, Content::Materialized, None);
        {
            let i = self.inode_mut(ino)?;
            i.meta.mode = perm & 0o7777;
            i.meta.xattrs = xattrs;
            i.target = target.to_vec();
            if kind == Kind::Dir {
                i.dir.as_mut().unwrap().overlay = Some(Overlay {
                    under: Under::Empty,
                    slots: BTreeMap::new(),
                });
            }
            i.nlookup = 1;
            i.version = 1;
        }
        if kind == Kind::File {
            File::create(self.overlay_path(ino))?;
        }
        self.place(ino)?;
        self.touch_dir(parent);
        self.note(ino);
        self.flush()?;
        Ok(ino)
    }

    fn touch_dir(&mut self, dir: Ino) {
        let t = (self.clock)();
        if let Some(i) = self.inodes.get_mut(&dir) {
            i.meta.mtime = t;
            i.meta.ctime = t;
        }
        self.touch(dir);
    }

    /// Takes the name `name` out of `parent`: a whiteout where the source holds it.
    fn unlist(&mut self, parent: Ino, name: &OsStr, ino: Ino) -> R<()> {
        self.overlay_dir(parent)?;
        let whiteout = self.source_has(parent, name)?;
        {
            let d = self.inode_mut(parent)?.dir.as_mut().unwrap();
            d.children.remove(name);
            let ov = d.overlay.as_mut().unwrap();
            if whiteout {
                ov.slots.insert(name.to_owned(), Slot::Whiteout);
            } else {
                ov.slots.remove(name);
            }
        }
        if whiteout {
            self.log.record(&pb::Record {
                record: Some(pb::record::Record::Whiteout(pb::Whiteout {
                    dir: parent,
                    name: os_bytes(name),
                })),
            })?;
        } else {
            self.log.record(&pb::Record {
                record: Some(pb::record::Record::Unwhiteout(pb::Unwhiteout {
                    dir: parent,
                    name: os_bytes(name),
                })),
            })?;
        }
        // The link goes; an inode with another link keeps it as its primary.
        let i = self.inode_mut(ino)?;
        if i.parent == parent && i.name == name {
            if let Some((p, n)) = i.extra.pop() {
                i.parent = p;
                i.name = n;
            } else {
                i.parent = 0;
            }
        } else {
            i.extra.retain(|(p, n)| !(*p == parent && n == name));
        }
        self.touch(ino);
        self.touch_dir(parent);
        Ok(())
    }

    pub fn unlink(&mut self, parent: Ino, name: &OsStr) -> R<Ino> {
        Self::check_name(parent, name)?;
        let ino = self.find(parent, name)?.ok_or(Error::Errno(libc::ENOENT))?;
        if self.inode(ino)?.kind == Kind::Dir {
            return errno(libc::EISDIR);
        }
        self.note(ino);
        self.unlist(parent, name, ino)?;
        self.maybe_drop(ino);
        self.flush()?;
        Ok(ino)
    }

    pub fn rmdir(&mut self, parent: Ino, name: &OsStr) -> R<Ino> {
        Self::check_name(parent, name)?;
        let ino = self.find(parent, name)?.ok_or(Error::Errno(libc::ENOENT))?;
        if self.inode(ino)?.kind != Kind::Dir {
            return errno(libc::ENOTDIR);
        }
        if !self.list(ino)?.is_empty() {
            return errno(libc::ENOTEMPTY);
        }
        self.note(ino);
        self.unlist(parent, name, ino)?;
        self.maybe_drop(ino);
        self.flush()?;
        Ok(ino)
    }

    /// Whether `a` is `b` or one of its ancestors.
    fn is_ancestor(&self, a: Ino, mut b: Ino) -> bool {
        loop {
            if a == b {
                return true;
            }
            match self.inodes.get(&b) {
                Some(i) if i.parent != 0 => b = i.parent,
                _ => return false,
            }
        }
    }

    /// Detaches a directory from the base before it moves: an overlay directory over the base
    /// gets the base tree at its old path as its source, as do the overlay directories beneath
    /// it; a derived one becomes an overlay directory over its value.
    fn unbase(&mut self, dir: Ino) -> R<()> {
        // Resolve everything first: nothing changes if a tree is missing.
        let mut plan: Vec<(Ino, Option<Terms>)> = Vec::new();
        let mut stack = vec![dir];
        while let Some(d) = stack.pop() {
            let i = self.inode(d)?;
            let ov = i.dir.as_ref().and_then(|s| s.overlay.as_ref());
            match ov {
                Some(ov) if ov.under == Under::Base => {
                    let kids: Vec<Ino> = ov
                        .slots
                        .values()
                        .filter_map(|s| match s {
                            Slot::Node(c) => Some(*c),
                            Slot::Whiteout => None,
                        })
                        .collect();
                    let t = self.source(d)?;
                    plan.push((d, t));
                    for k in kids {
                        if self.inode(k)?.kind == Kind::Dir {
                            stack.push(k);
                        }
                    }
                }
                Some(_) => {}
                None => {
                    // A derived directory: its value, pinned when it becomes an overlay directory.
                    let t = match self.value_of(d)? {
                        Value::Tree(t) => Some(t),
                        _ => None,
                    };
                    plan.push((d, t));
                }
            }
        }
        for (d, t) in plan {
            let under = t.map_or(Under::Empty, Under::Tree);
            let is_overlay = self.inode(d)?.dir.as_ref().unwrap().overlay.is_some();
            if !is_overlay {
                let parent = self.inode(d)?.parent;
                self.overlay_dir(parent)?;
                self.place(d)?;
                let i = self.inode_mut(d)?;
                i.content = Content::Materialized;
                i.derived = None;
                i.dir.as_mut().unwrap().overlay = Some(Overlay {
                    under,
                    slots: BTreeMap::new(),
                });
            } else {
                self.inode_mut(d)?
                    .dir
                    .as_mut()
                    .unwrap()
                    .overlay
                    .as_mut()
                    .unwrap()
                    .under = under;
            }
            self.touch(d);
        }
        Ok(())
    }

    /// Renames, with renameat2's flags (`RENAME_NOREPLACE`, `RENAME_EXCHANGE`). Returns the inode
    /// a replaced name held, if any.
    pub fn rename(
        &mut self,
        parent: Ino,
        name: &OsStr,
        newparent: Ino,
        newname: &OsStr,
        flags: u32,
    ) -> R<Option<Ino>> {
        const NOREPLACE: u32 = 1;
        const EXCHANGE: u32 = 2;
        if flags & !(NOREPLACE | EXCHANGE) != 0 {
            return errno(libc::EINVAL);
        }
        Self::check_name(parent, name)?;
        Self::check_name(newparent, newname)?;
        let src = self.find(parent, name)?.ok_or(Error::Errno(libc::ENOENT))?;
        let dst = self.find(newparent, newname)?;
        if self.inode(newparent)?.kind != Kind::Dir {
            return errno(libc::ENOTDIR);
        }
        let src_dir = self.inode(src)?.kind == Kind::Dir;
        if src_dir && self.is_ancestor(src, newparent) {
            return errno(libc::EINVAL);
        }
        if dst == Some(src) {
            return Ok(None);
        }
        if flags & EXCHANGE != 0 {
            let dst = dst.ok_or(Error::Errno(libc::ENOENT))?;
            if self.inode(dst)?.kind == Kind::Dir && self.is_ancestor(dst, parent) {
                return errno(libc::EINVAL);
            }
            self.detach_for_move(src)?;
            self.detach_for_move(dst)?;
            self.overlay_dir(parent)?;
            self.overlay_dir(newparent)?;
            self.relink(src, parent, name, newparent, newname);
            self.relink(dst, newparent, newname, parent, name);
            self.place_at(src, newparent, newname);
            self.place_at(dst, parent, name);
            self.touch_dir(parent);
            self.touch_dir(newparent);
            self.note(src);
            self.note(dst);
            self.flush()?;
            return Ok(None);
        }
        if let Some(d) = dst {
            if flags & NOREPLACE != 0 {
                return errno(libc::EEXIST);
            }
            let dst_dir = self.inode(d)?.kind == Kind::Dir;
            match (src_dir, dst_dir) {
                (true, false) => return errno(libc::ENOTDIR),
                (false, true) => return errno(libc::EISDIR),
                (true, true) if !self.list(d)?.is_empty() => return errno(libc::ENOTEMPTY),
                _ => {}
            }
        }
        self.detach_for_move(src)?;
        self.overlay_dir(newparent)?;
        self.overlay_dir(parent)?;
        self.note(src);
        if let Some(d) = dst {
            // The replaced inode loses this name.
            let i = self.inode_mut(d)?;
            if i.parent == newparent && i.name == newname {
                if let Some((p, n)) = i.extra.pop() {
                    i.parent = p;
                    i.name = n;
                } else {
                    i.parent = 0;
                }
            } else {
                i.extra.retain(|(p, n)| !(*p == newparent && n == newname));
            }
            self.touch(d);
            let dd = self.inode_mut(newparent)?.dir.as_mut().unwrap();
            dd.children.remove(newname);
        }
        // Out of the old place: a whiteout where the source holds the name.
        let whiteout = self.source_has(parent, name)?;
        {
            let pd = self.inode_mut(parent)?.dir.as_mut().unwrap();
            pd.children.remove(name);
            let ov = pd.overlay.as_mut().unwrap();
            if whiteout {
                ov.slots.insert(name.to_owned(), Slot::Whiteout);
            } else {
                ov.slots.remove(name);
            }
        }
        let rec = if whiteout {
            pb::record::Record::Whiteout(pb::Whiteout {
                dir: parent,
                name: os_bytes(name),
            })
        } else {
            pb::record::Record::Unwhiteout(pb::Unwhiteout {
                dir: parent,
                name: os_bytes(name),
            })
        };
        self.log.record(&pb::Record { record: Some(rec) })?;
        self.relink(src, parent, name, newparent, newname);
        self.place_at(src, newparent, newname);
        self.log.record(&pb::Record {
            record: Some(pb::record::Record::Unwhiteout(pb::Unwhiteout {
                dir: newparent,
                name: os_bytes(newname),
            })),
        })?;
        self.touch_dir(parent);
        self.touch_dir(newparent);
        self.note(src);
        if let Some(d) = dst {
            self.maybe_drop(d);
        }
        self.flush()?;
        Ok(dst)
    }

    /// Before an inode moves: a derived file or symlink is pinned, and a directory detached from
    /// the base.
    fn detach_for_move(&mut self, ino: Ino) -> R<()> {
        if self.inode(ino)?.kind == Kind::Dir {
            self.unbase(ino)
        } else {
            self.pin(ino)
        }
    }

    fn relink(&mut self, ino: Ino, parent: Ino, name: &OsStr, newparent: Ino, newname: &OsStr) {
        let i = self.inodes.get_mut(&ino).unwrap();
        if i.parent == parent && i.name == name {
            i.parent = newparent;
            i.name = newname.to_owned();
        } else if let Some(l) = i.extra.iter_mut().find(|(p, n)| *p == parent && n == name) {
            *l = (newparent, newname.to_owned());
        }
        self.dirty.insert(ino);
    }

    fn place_at(&mut self, ino: Ino, parent: Ino, name: &OsStr) {
        let d = self.inodes.get_mut(&parent).unwrap().dir.as_mut().unwrap();
        d.children.remove(name);
        d.overlay
            .as_mut()
            .unwrap()
            .slots
            .insert(name.to_owned(), Slot::Node(ino));
    }

    /// A hard link to a file (materialized first) at `newparent`/`newname`.
    pub fn link(&mut self, ino: Ino, newparent: Ino, newname: &OsStr) -> R<()> {
        Self::check_name(newparent, newname)?;
        let kind = self.inode(ino)?.kind;
        if kind == Kind::Dir {
            return errno(libc::EPERM);
        }
        if self.find(newparent, newname)?.is_some() {
            return errno(libc::EEXIST);
        }
        if matches!(kind, Kind::File | Kind::Symlink) {
            self.materialize_file(ino)?;
        }
        self.overlay_dir(newparent)?;
        {
            let i = self.inode_mut(ino)?;
            if i.parent == 0 {
                i.parent = newparent;
                i.name = newname.to_owned();
            } else {
                i.extra.push((newparent, newname.to_owned()));
            }
            i.nlookup += 1;
        }
        self.place_at(ino, newparent, newname);
        self.log.record(&pb::Record {
            record: Some(pb::record::Record::Unwhiteout(pb::Unwhiteout {
                dir: newparent,
                name: os_bytes(newname),
            })),
        })?;
        self.touch(ino);
        self.touch_dir(newparent);
        self.note(ino);
        self.flush()?;
        Ok(())
    }

    /// Changes an inode's attributes. A size change materializes a file; a mode or time change
    /// pins a derived file (or makes a directory an overlay directory) to keep it.
    pub fn setattr(&mut self, ino: Ino, set: &okfs_fuse::SetAttr) -> R<()> {
        if ino == JJ {
            return errno(libc::EPERM);
        }
        if set.uid.is_some_and(|u| u != self.uid) || set.gid.is_some_and(|g| g != self.gid) {
            return errno(libc::EPERM);
        }
        let kind = self.inode(ino)?.kind;
        if let Some(size) = set.size {
            if kind != Kind::File {
                return errno(libc::EINVAL);
            }
            self.materialize_file(ino)?;
            self.truncate(ino, size)?;
        }
        let meta_change =
            set.mode.is_some() || set.atime.is_some() || set.mtime.is_some() || set.ctime.is_some();
        if meta_change {
            match kind {
                Kind::Dir if ino != ROOT => self.overlay_dir(ino)?,
                Kind::File | Kind::Symlink => self.pin(ino)?,
                _ => {}
            }
            if let Some(mode) = set.mode {
                let perm = mode & 0o7777;
                acl::chmod(&mut self.inode_mut(ino)?.meta.xattrs, perm);
                self.set_perm(ino, perm)?;
            }
            let t = (self.clock)();
            let i = self.inode_mut(ino)?;
            let resolve = |v: okfs_fuse::TimeOrNow| match v {
                okfs_fuse::TimeOrNow::Time(t) => t,
                okfs_fuse::TimeOrNow::Now => t,
            };
            if let Some(a) = set.atime {
                i.meta.atime = resolve(a);
            }
            if let Some(m) = set.mtime {
                i.meta.mtime = resolve(m);
            }
            i.meta.ctime = set.ctime.unwrap_or(t);
            self.touch(ino);
        }
        self.flush()?;
        Ok(())
    }

    /// Sets an inode's permissions, the inode already kept (pinned, or an overlay directory). A
    /// file whose executable bit changes is a change for the snapshot.
    fn set_perm(&mut self, ino: Ino, perm: u32) -> R<()> {
        let i = self.inode_mut(ino)?;
        let exec = perm & 0o100 != 0;
        let exec_changed = i.kind == Kind::File && exec != (i.meta.mode & 0o100 != 0);
        i.meta.mode = perm;
        if exec_changed {
            if let Content::Fixed(Value::File { executable, .. }) = &mut i.content {
                *executable = exec;
            }
            self.note(ino);
        }
        Ok(())
    }

    pub fn getxattr(&mut self, ino: Ino, name: &OsStr) -> R<Vec<u8>> {
        if name == "user.ok.file_id" {
            let i = self.inode(ino)?;
            if i.kind != Kind::File {
                return errno(libc::ENODATA);
            }
            if let Content::Materialized = i.content {
                return match &i.file_id {
                    Some((v, id)) if *v == i.version => Ok(id.hex().into_bytes()),
                    _ => errno(libc::ENODATA),
                };
            }
            return match self.value_of(ino)? {
                Value::File { id, .. } => Ok(id.hex().into_bytes()),
                _ => errno(libc::ENODATA),
            };
        }
        self.inode(ino)?
            .meta
            .xattrs
            .get(name)
            .cloned()
            .ok_or(Error::Errno(libc::ENODATA))
    }

    pub fn listxattr(&mut self, ino: Ino) -> R<Vec<u8>> {
        let i = self.inode(ino)?;
        let mut out = Vec::new();
        for n in i.meta.xattrs.keys() {
            out.extend_from_slice(n.as_bytes());
            out.push(0);
        }
        Ok(out)
    }

    /// Sets an extended attribute. ACLs are kept with their semantics (`acl`): an access ACL
    /// sets the inode's permissions, and clears its setgid bit for `kill_sgid`; only a directory
    /// has a default ACL.
    pub fn setxattr(
        &mut self,
        ino: Ino,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        kill_sgid: bool,
    ) -> R<()> {
        if ino == JJ || name.as_bytes().starts_with(b"user.ok.") {
            return errno(libc::EPERM);
        }
        let kind = self.inode(ino)?.kind;
        if name == acl::ACCESS || name == acl::DEFAULT {
            let parsed = acl::Acl::parse(value).map_err(Error::Errno)?;
            if name == acl::DEFAULT && kind != Kind::Dir && parsed.is_some() {
                return errno(libc::EACCES);
            }
        }
        let exists = self.inode(ino)?.meta.xattrs.contains_key(name);
        if flags & libc::XATTR_CREATE != 0 && exists {
            return errno(libc::EEXIST);
        }
        if flags & libc::XATTR_REPLACE != 0 && !exists {
            return errno(libc::ENODATA);
        }
        match kind {
            Kind::Dir if ino != ROOT => self.overlay_dir(ino)?,
            Kind::File | Kind::Symlink => self.pin(ino)?,
            _ => {}
        }
        let t = (self.clock)();
        let i = self.inode_mut(ino)?;
        if name == acl::ACCESS {
            let mode = acl::set_access(&mut i.meta.xattrs, i.meta.mode, value, kill_sgid)
                .map_err(Error::Errno)?;
            i.meta.ctime = t;
            self.set_perm(ino, mode)?;
        } else if name == acl::DEFAULT {
            acl::set_default(&mut i.meta.xattrs, kind == Kind::Dir, value).map_err(Error::Errno)?;
        } else {
            i.meta.xattrs.insert(name.to_owned(), value.to_vec());
        }
        self.touch(ino);
        self.flush()?;
        Ok(())
    }

    pub fn removexattr(&mut self, ino: Ino, name: &OsStr) -> R<()> {
        if !self.inode(ino)?.meta.xattrs.contains_key(name) {
            return errno(libc::ENODATA);
        }
        self.inode_mut(ino)?.meta.xattrs.remove(name);
        self.touch(ino);
        self.flush()?;
        Ok(())
    }

    // ─── For jj: listing for a snapshot, checkout ───

    /// The inode at a path, walking from the root.
    pub fn resolve(&mut self, path: &RepoPath) -> R<Option<Ino>> {
        let mut cur = ROOT;
        for c in path.components() {
            match self.find(cur, OsStr::new(c.as_internal_str()))? {
                Some(i) => cur = i,
                None => return Ok(None),
            }
        }
        self.flush()?;
        Ok(Some(cur))
    }

    /// A directory as a snapshot reads it (ListDir): its source, and its overlay's entries.
    pub fn list_dir(&mut self, path: &RepoPath) -> R<pb::ListDirReply> {
        let Some(dir) = self.resolve(path)? else {
            return errno(libc::ENOENT);
        };
        if self.inode(dir)?.kind != Kind::Dir {
            return errno(libc::ENOTDIR);
        }
        let source = self.source(dir)?;
        let slots: Vec<(OsString, Slot)> = self
            .inode(dir)?
            .dir
            .as_ref()
            .unwrap()
            .overlay
            .as_ref()
            .map(|ov| ov.slots.iter().map(|(n, s)| (n.clone(), *s)).collect())
            .unwrap_or_default();
        let materialized = self.inode(dir)?.dir.as_ref().unwrap().overlay.is_some();
        let mut entries = Vec::new();
        for (name, slot) in slots {
            let mut e = pb::DirEntry {
                name: os_bytes(&name),
                ..Default::default()
            };
            match slot {
                Slot::Whiteout => e.set_kind(pb::dir_entry::Kind::Deleted),
                Slot::Node(ino) => {
                    let (kind, content) = {
                        let i = self.inode(ino)?;
                        (i.kind, i.content.clone())
                    };
                    e.ino = ino;
                    match (kind, content) {
                        (Kind::Dir, _) => e.set_kind(pb::dir_entry::Kind::Dir),
                        (Kind::File, Content::Materialized) => {
                            let path = self.overlay_path(ino);
                            let i = self.inode(ino)?;
                            e.set_kind(pb::dir_entry::Kind::File);
                            e.overlay_path = path.to_string_lossy().into_owned();
                            e.version = i.version;
                            e.executable = i.meta.mode & 0o100 != 0;
                            e.size = i.size;
                            e.checkout_conflict = i.checkout_conflict;
                            e.conflict_marker_len = i.marker_len;
                            if let Some((v, id)) = &i.file_id
                                && *v == i.version
                            {
                                e.file_id = id.to_bytes();
                            }
                        }
                        (Kind::Symlink, Content::Materialized) => {
                            e.set_kind(pb::dir_entry::Kind::Symlink);
                            let i = self.inode(ino)?;
                            e.symlink_target = i.target.clone();
                            e.redirect = i.redirect;
                        }
                        (Kind::File | Kind::Symlink, _) => {
                            let v = self.value_of(ino)?;
                            e.set_kind(if kind == Kind::File {
                                pb::dir_entry::Kind::File
                            } else {
                                pb::dir_entry::Kind::Symlink
                            });
                            e.value = Some(value_out(&v));
                        }
                        _ => e.set_kind(pb::dir_entry::Kind::Other),
                    }
                }
            }
            entries.push(e);
        }
        Ok(pb::ListDirReply {
            source: source.as_ref().map(terms_out),
            materialized,
            entries,
        })
    }

    /// Remembers the content ID jj computed for a written file's version.
    pub fn remember(&mut self, ino: Ino, version: u64, id: FileId) {
        if let Some(i) = self.inodes.get_mut(&ino)
            && i.version == version
        {
            i.file_id = Some((version, id));
        }
    }

    /// The tree jj recorded for the working copy.
    pub fn set_recorded(&mut self, tree: Terms, commit: Vec<u8>) -> R<()> {
        self.recorded = tree;
        self.recorded_commit = commit;
        self.log_base()?;
        self.log.sync()?;
        Ok(())
    }

    fn log_base(&mut self) -> R<()> {
        self.log.record(&pb::Record {
            record: Some(pb::record::Record::Base(pb::Base {
                base: Some(terms_out(&self.base)),
                recorded: Some(terms_out(&self.recorded)),
                sparse: self
                    .sparse
                    .iter()
                    .map(|p| p.as_internal_file_string().to_owned())
                    .collect(),
                recorded_commit: self.recorded_commit.clone(),
            })),
        })?;
        Ok(())
    }

    /// Hides what the sparse patterns exclude; the caller drops the kernel's cached entries.
    pub fn set_sparse(&mut self, patterns: Vec<RepoPathBuf>) -> R<()> {
        self.sparse = patterns;
        self.epoch += 1;
        self.log_base()?;
        self.changed.push(Changed {
            paths: Vec::new(),
            checkout: true,
        });
        self.seq += 1;
        Ok(())
    }

    /// Moves the workspace from `recorded` (the tree jj recorded for its working copy) to
    /// `new_tree` (docs/okfs.md, "jj's working copy"). It costs what the workspace holds, not
    /// the size of the change: the overlay — what was written, deleted, or moved — is reconciled
    /// with the two trees (a tracked written file the change touches comes from the new tree; an
    /// untracked one stays, and is reported where the new tree puts something of its own; a
    /// directory left empty goes), and the kernel is told what changed only in the directories it
    /// has looked up. Conflicts arrive materialized. Every tree it reads is gathered first, all
    /// that are missing asked for together: nothing changes while one is missing. Returns the
    /// skipped paths and what the kernel must drop.
    pub fn check_out(
        &mut self,
        recorded: Terms,
        new_tree: Terms,
        conflicts: &[Conflict],
    ) -> R<(Vec<RepoPathBuf>, Vec<Stale>)> {
        let old_base = self.base.clone();
        self.checkout_needs(&old_base, &recorded, &new_tree, conflicts)?;

        let mut stale = Vec::new();
        self.epoch += 1;
        let t = (self.clock)();
        self.refresh_live(
            ROOT,
            RepoPathBuf::root(),
            &old_base,
            &new_tree,
            t,
            &mut stale,
        )?;
        self.base = new_tree.clone();
        self.recorded = new_tree.clone();
        let mut skipped = Vec::new();
        self.reconcile(
            ROOT,
            RepoPathBuf::root(),
            &old_base,
            &recorded,
            &new_tree,
            t,
            &mut skipped,
            &mut stale,
        )?;
        for c in conflicts {
            if skipped.contains(&c.path) {
                continue;
            }
            self.write_conflict(c, &mut stale)?;
        }
        self.base_dirs.retain(|(root, _), _| root == &new_tree);
        self.log_base()?;
        self.flush()?;
        self.log.sync()?;
        self.seq += 1;
        self.changed.push(Changed {
            paths: Vec::new(),
            checkout: true,
        });
        Ok((skipped, stale))
    }

    /// Records the trees a term list names that are not cached.
    fn gather_entries(&mut self, terms: &Terms, missing: &mut Vec<TreeId>) -> bool {
        let mut all = true;
        for id in terms.iter().flatten() {
            if !self.cache.trees.contains_key(id) {
                if !missing.contains(id) {
                    missing.push(id.clone());
                }
                all = false;
            }
        }
        all
    }

    /// `root`'s directory at `path`, or None when a tree on the way is missing (recorded).
    fn try_at(
        &mut self,
        root: &Terms,
        path: &RepoPath,
        missing: &mut Vec<TreeId>,
    ) -> Option<Option<Terms>> {
        match self.base_at(root, path) {
            Ok(t) => Some(t),
            Err(Error::Need(Need::Tree(id))) => {
                if !missing.contains(&id) {
                    missing.push(id);
                }
                None
            }
            Err(_) => None,
        }
    }

    /// `root`'s directory at `path` and its entries, recording what is missing.
    fn gather_dir(&mut self, root: &Terms, path: &RepoPath, missing: &mut Vec<TreeId>) {
        if let Some(Some(t)) = self.try_at(root, path, missing) {
            self.gather_entries(&t, missing);
        }
    }

    /// The overlay directories beneath `dir` (its slots that are directories).
    fn slot_dirs(&self, dir: Ino) -> Vec<(OsString, Ino)> {
        let Some(ov) = self
            .inodes
            .get(&dir)
            .and_then(|i| i.dir.as_ref())
            .and_then(|d| d.overlay.as_ref())
        else {
            return Vec::new();
        };
        ov.slots
            .iter()
            .filter_map(|(n, s)| match s {
                Slot::Node(c) if self.inodes.get(c).is_some_and(|i| i.kind == Kind::Dir) => {
                    Some((n.clone(), *c))
                }
                _ => None,
            })
            .collect()
    }

    /// The directories the kernel looked up beneath `dir` that follow its source.
    fn derived_dirs(&self, dir: Ino) -> Vec<(OsString, Ino)> {
        let Some(d) = self.inodes.get(&dir).and_then(|i| i.dir.as_ref()) else {
            return Vec::new();
        };
        d.children
            .iter()
            .filter(|(_, c)| self.inodes.get(c).is_some_and(|i| i.kind == Kind::Dir))
            .map(|(n, c)| (n.clone(), *c))
            .collect()
    }

    /// Every tree a checkout reads, gathered without changing anything: the recorded and new
    /// trees along the overlay, the old and new bases where the kernel has looked and they
    /// differ, and the new tree along each conflict. All that are missing are one `Need::Trees`,
    /// so a checkout fetches in as many rounds as the trees are deep.
    fn checkout_needs(
        &mut self,
        old_base: &Terms,
        recorded: &Terms,
        new: &Terms,
        conflicts: &[Conflict],
    ) -> R<()> {
        let mut missing = Vec::new();
        let mut stack = vec![(ROOT, RepoPathBuf::root())];
        while let Some((dir, path)) = stack.pop() {
            for root in [old_base, recorded, new] {
                self.gather_dir(root, &path, &mut missing);
            }
            let under = self
                .inode(dir)?
                .dir
                .as_ref()
                .and_then(|d| d.overlay.as_ref())
                .map(|ov| ov.under.clone());
            if let Some(Under::Tree(t)) = under {
                self.gather_entries(&t, &mut missing);
            }
            for (name, c) in self.slot_dirs(dir) {
                if let Some(comp) = component(&name) {
                    stack.push((c, path.join(comp)));
                }
            }
        }
        let mut stack = vec![(ROOT, RepoPathBuf::root())];
        while let Some((dir, path)) = stack.pop() {
            let (Some(b), Some(n)) = (
                self.try_at(old_base, &path, &mut missing),
                self.try_at(new, &path, &mut missing),
            ) else {
                continue;
            };
            if b == n {
                continue;
            }
            let have_b = b
                .as_ref()
                .is_none_or(|b| self.gather_entries(b, &mut missing));
            let have_n = n
                .as_ref()
                .is_none_or(|n| self.gather_entries(n, &mut missing));
            if !(have_b && have_n) {
                continue;
            }
            for (name, c) in self
                .derived_dirs(dir)
                .into_iter()
                .chain(self.slot_dirs(dir))
            {
                if let Some(comp) = component(&name) {
                    stack.push((c, path.join(comp)));
                }
            }
        }
        for c in conflicts {
            if let Some(parent) = c.path.parent() {
                self.gather_dir(new, parent, &mut missing);
            }
        }
        if missing.is_empty() {
            Ok(())
        } else {
            Err(Error::Need(Need::Trees(missing)))
        }
    }

    fn listing(&mut self, t: &Option<Terms>) -> R<Arc<BTreeMap<String, Value>>> {
        match t {
            Some(t) => self.entries(t),
            None => Ok(Arc::new(BTreeMap::new())),
        }
    }

    /// Re-derives a looked-up child whose source changed: the same kind keeps its inode, with the
    /// new value and the checkout's time; another kind, or none, detaches it with its old value.
    fn rederive(
        &mut self,
        child: Ino,
        old: Option<&Value>,
        new: Option<&Value>,
        t: Time,
        stale: &mut Vec<Stale>,
    ) -> R<bool> {
        stale.push(Stale::Inode(child));
        let kind = self.inode(child)?.kind;
        match new {
            Some(v) if v.kind() == Some(kind) => {
                let epoch = self.epoch;
                let i = self.inode_mut(child)?;
                i.derived = Some((epoch, v.clone()));
                i.meta.mtime = t;
                i.meta.ctime = t;
                self.touch(child);
                Ok(true)
            }
            _ => {
                let epoch = self.epoch;
                if let Some(old) = old {
                    self.inode_mut(child)?.derived = Some((epoch, old.clone()));
                }
                self.detach_derived(child);
                Ok(false)
            }
        }
    }

    /// Tells the kernel what a checkout changed among what it has looked up, from `dir` (at
    /// `path`) down, reading only directories whose old and new bases differ.
    fn refresh_live(
        &mut self,
        dir: Ino,
        path: RepoPathBuf,
        old: &Terms,
        new: &Terms,
        t: Time,
        stale: &mut Vec<Stale>,
    ) -> R<()> {
        let b = self.base_at(old, &path)?;
        let n = self.base_at(new, &path)?;
        if b == n {
            return Ok(());
        }
        stale.push(Stale::Inode(dir));
        let be = self.listing(&b)?;
        let ne = self.listing(&n)?;
        let names: BTreeSet<String> = be.keys().chain(ne.keys()).cloned().collect();
        for name in names {
            let (bv, nv) = (be.get(&name), ne.get(&name));
            if bv == nv {
                continue;
            }
            let os = OsString::from(&name);
            stale.push(Stale::Entry(dir, os.clone()));
            let child = self
                .inodes
                .get(&dir)
                .and_then(|i| i.dir.as_ref())
                .and_then(|d| d.children.get(&os).copied());
            if let Some(c) = child
                && self.rederive(c, bv, nv, t, stale)?
                && self.inode(c)?.kind == Kind::Dir
            {
                let comp = RepoPathComponent::new(&name).expect("a tree's entry names are valid");
                self.refresh_live(c, path.join(comp), old, new, t, stale)?;
            }
        }
        // Overlay directories that follow the base list its changes too.
        for (name, c) in self.slot_dirs(dir) {
            let follows = self
                .inode(c)?
                .dir
                .as_ref()
                .and_then(|d| d.overlay.as_ref())
                .is_some_and(|ov| ov.under == Under::Base);
            if let (true, Some(comp)) = (follows, component(&name)) {
                self.refresh_live(c, path.join(comp), old, new, t, stale)?;
            }
        }
        Ok(())
    }

    /// Reconciles the overlay with a checkout from `recorded` to `new` (the new base in place),
    /// from overlay directory `dir` (at `path`) down.
    #[allow(clippy::too_many_arguments)]
    fn reconcile(
        &mut self,
        dir: Ino,
        path: RepoPathBuf,
        old_base: &Terms,
        recorded: &Terms,
        new: &Terms,
        t: Time,
        skipped: &mut Vec<RepoPathBuf>,
        stale: &mut Vec<Stale>,
    ) -> R<()> {
        let r = self.base_at(recorded, &path)?;
        let n = self.base_at(new, &path)?;
        let re = self.listing(&r)?;
        let ne = self.listing(&n)?;
        // Unwritten names here come from the new tree's directory, if it has one.
        if dir != ROOT {
            let old_under = self
                .inode(dir)?
                .dir
                .as_ref()
                .and_then(|d| d.overlay.as_ref())
                .map(|ov| ov.under.clone());
            let under = if n.is_some() {
                Under::Base
            } else {
                Under::Empty
            };
            if let Some(old_under) = old_under.filter(|o| *o != under) {
                let before = match &old_under {
                    Under::Base => {
                        let b = self.base_at(old_base, &path)?;
                        self.listing(&b)?
                    }
                    Under::Tree(t) => self.entries(t)?,
                    Under::Empty => Arc::new(BTreeMap::new()),
                };
                let after = if under == Under::Base {
                    ne.clone()
                } else {
                    Arc::new(BTreeMap::new())
                };
                stale.push(Stale::Inode(dir));
                let names: BTreeSet<String> = before.keys().chain(after.keys()).cloned().collect();
                for name in names {
                    let (bv, av) = (before.get(&name), after.get(&name));
                    if bv == av {
                        continue;
                    }
                    let os = OsString::from(&name);
                    stale.push(Stale::Entry(dir, os.clone()));
                    let child = self
                        .inodes
                        .get(&dir)
                        .and_then(|i| i.dir.as_ref())
                        .and_then(|d| d.children.get(&os).copied());
                    if let Some(c) = child {
                        self.rederive(c, bv, av, t, stale)?;
                    }
                }
                self.inode_mut(dir)?
                    .dir
                    .as_mut()
                    .unwrap()
                    .overlay
                    .as_mut()
                    .unwrap()
                    .under = under;
                self.touch(dir);
            }
        }
        let slots: Vec<(OsString, Slot)> = self
            .inode(dir)?
            .dir
            .as_ref()
            .and_then(|d| d.overlay.as_ref())
            .map(|ov| ov.slots.iter().map(|(n, s)| (n.clone(), *s)).collect())
            .unwrap_or_default();
        for (name, slot) in slots {
            // A name that is not UTF-8 is never in a tree: untracked.
            let Some(comp) = component(&name) else {
                continue;
            };
            let child_path = path.join(comp);
            let (rv, nv) = (
                re.get(comp.as_internal_str()),
                ne.get(comp.as_internal_str()),
            );
            match slot {
                Slot::Whiteout => {
                    if nv.is_some() && rv != nv {
                        self.drop_slot(dir, &name, None, stale);
                    }
                }
                Slot::Node(c) if self.inode(c)?.kind == Kind::Dir => {
                    self.reconcile(
                        c,
                        child_path.clone(),
                        old_base,
                        recorded,
                        new,
                        t,
                        skipped,
                        stale,
                    )?;
                    let new_dir = matches!(nv, Some(Value::Tree(_)));
                    if new_dir {
                        continue;
                    }
                    if self.list(c)?.is_empty() {
                        // Left empty where the new tree has no directory: it goes.
                        self.drop_slot(dir, &name, Some(c), stale);
                    } else if nv.is_some() {
                        // Untracked files stand where the new tree puts a file.
                        skipped.push(child_path);
                    }
                }
                Slot::Node(c) => {
                    let tracked = rv.is_some_and(|v| !matches!(v, Value::Tree(_)));
                    if tracked {
                        if rv != nv {
                            self.drop_slot(dir, &name, Some(c), stale);
                        }
                    } else if nv.is_some() {
                        skipped.push(child_path);
                    }
                }
            }
        }
        Ok(())
    }

    fn drop_slot(&mut self, dir: Ino, name: &OsStr, ino: Option<Ino>, stale: &mut Vec<Stale>) {
        let d = self.inodes.get_mut(&dir).unwrap().dir.as_mut().unwrap();
        d.overlay.as_mut().unwrap().slots.remove(name);
        let _ = self.log.record(&pb::Record {
            record: Some(pb::record::Record::Unwhiteout(pb::Unwhiteout {
                dir,
                name: os_bytes(name),
            })),
        });
        stale.push(Stale::Entry(dir, name.to_owned()));
        if let Some(ino) = ino {
            stale.push(Stale::Inode(ino));
            if let Some(i) = self.inodes.get_mut(&ino) {
                if i.parent == dir && i.name == name {
                    if let Some((p, n)) = i.extra.pop() {
                        i.parent = p;
                        i.name = n;
                    } else {
                        i.parent = 0;
                    }
                } else {
                    i.extra.retain(|(p, n)| !(*p == dir && n == name));
                }
            }
            self.dirty.insert(ino);
            self.maybe_drop(ino);
        }
    }

    fn write_conflict(&mut self, c: &Conflict, stale: &mut Vec<Stale>) -> R<()> {
        let Some((parent_path, name)) = c.path.split() else {
            return Ok(());
        };
        let Some(parent) = self.resolve(parent_path)? else {
            // The new tree's parent directories: made where missing.
            let mut cur = ROOT;
            for c in parent_path.components() {
                let n = OsStr::new(c.as_internal_str());
                cur = match self.find(cur, n)? {
                    Some(i) => i,
                    None => {
                        let i = self.create(cur, n, Kind::Dir, 0o755, b"")?;
                        self.inode_mut(i)?.nlookup = 0;
                        i
                    }
                };
            }
            return self.write_conflict_in(cur, OsStr::new(name.as_internal_str()), c, stale);
        };
        self.write_conflict_in(parent, OsStr::new(name.as_internal_str()), c, stale)
    }

    fn write_conflict_in(
        &mut self,
        parent: Ino,
        name: &OsStr,
        c: &Conflict,
        stale: &mut Vec<Stale>,
    ) -> R<()> {
        stale.push(Stale::Entry(parent, name.to_owned()));
        if let Some(old) = self.find(parent, name)? {
            if self.inode(old)?.kind == Kind::Dir {
                return Ok(());
            }
            stale.push(Stale::Inode(old));
            self.unlist(parent, name, old)?;
            self.maybe_drop(old);
        }
        let ino = self.create(
            parent,
            name,
            Kind::File,
            if c.executable { 0o755 } else { 0o644 },
            b"",
        )?;
        self.overlay_file(ino)?.write_all_at(&c.contents, 0)?;
        let i = self.inode_mut(ino)?;
        i.size = c.contents.len() as u64;
        i.version += 1;
        i.checkout_conflict = true;
        i.marker_len = c.marker_len;
        i.nlookup = 0;
        self.touch(ino);
        Ok(())
    }

    // ─── Persistence ───

    /// Writes the records of what changed since the last flush.
    fn flush(&mut self) -> R<()> {
        for ino in std::mem::take(&mut self.dropped) {
            self.log.record(&pb::Record {
                record: Some(pb::record::Record::Drop(ino)),
            })?;
        }
        for ino in std::mem::take(&mut self.dirty) {
            if let Some(r) = self.record_of(ino) {
                self.log.record(&pb::Record {
                    record: Some(pb::record::Record::Inode(r)),
                })?;
            }
        }
        Ok(())
    }

    fn record_of(&self, ino: Ino) -> Option<pb::InodeRecord> {
        if ino == JJ {
            return None;
        }
        let i = self.inodes.get(&ino)?;
        let mut r = pb::InodeRecord {
            ino,
            parent: i.parent,
            name: os_bytes(&i.name),
            extra_links: i
                .extra
                .iter()
                .map(|(p, n)| pb::Link {
                    parent: *p,
                    name: os_bytes(n),
                })
                .collect(),
            mode: i.meta.mode,
            mtime_sec: i.meta.mtime.0,
            mtime_nsec: i.meta.mtime.1,
            ctime_sec: i.meta.ctime.0,
            ctime_nsec: i.meta.ctime.1,
            atime_sec: i.meta.atime.0,
            atime_nsec: i.meta.atime.1,
            symlink_target: i.target.clone(),
            checkout_conflict: i.checkout_conflict,
            conflict_marker_len: i.marker_len,
            redirect: i.redirect,
            xattrs: i
                .meta
                .xattrs
                .iter()
                .map(|(k, v)| pb::Xattr {
                    name: os_bytes(k),
                    value: v.clone(),
                })
                .collect(),
            overlay: self
                .inodes
                .get(&i.parent)
                .and_then(|p| p.dir.as_ref())
                .and_then(|d| d.overlay.as_ref())
                .is_some_and(|ov| ov.slots.get(&i.name) == Some(&Slot::Node(ino))),
            ..Default::default()
        };
        r.set_kind(match i.kind {
            Kind::Dir => pb::inode_record::Kind::Dir,
            Kind::File => pb::inode_record::Kind::File,
            Kind::Symlink => pb::inode_record::Kind::Symlink,
            Kind::Fifo => pb::inode_record::Kind::Fifo,
            Kind::Socket => pb::inode_record::Kind::Socket,
        });
        match &i.content {
            Content::Derived => r.set_state(pb::inode_record::State::Base),
            Content::Fixed(v) => {
                r.set_state(pb::inode_record::State::Fixed);
                r.value = Some(value_out(v));
            }
            Content::Materialized => {
                r.set_state(pb::inode_record::State::Materialized);
                if let Some(ov) = i.dir.as_ref().and_then(|d| d.overlay.as_ref()) {
                    match &ov.under {
                        Under::Base => r.under_base = true,
                        Under::Tree(t) => r.under = Some(terms_out(t)),
                        Under::Empty => {}
                    }
                }
            }
        }
        Some(r)
    }

    /// Every record that rebuilds the model as it stands: what compaction writes.
    pub fn records(&self) -> Vec<pb::Record> {
        let mut out = vec![pb::Record {
            record: Some(pb::record::Record::Base(pb::Base {
                base: Some(terms_out(&self.base)),
                recorded: Some(terms_out(&self.recorded)),
                sparse: self
                    .sparse
                    .iter()
                    .map(|p| p.as_internal_file_string().to_owned())
                    .collect(),
                recorded_commit: self.recorded_commit.clone(),
            })),
        }];
        let mut inos: Vec<&Ino> = self.inodes.keys().collect();
        inos.sort();
        for ino in inos {
            if let Some(r) = self.record_of(*ino) {
                out.push(pb::Record {
                    record: Some(pb::record::Record::Inode(r)),
                });
            }
            if let Some(ov) = self.inodes[ino]
                .dir
                .as_ref()
                .and_then(|d| d.overlay.as_ref())
            {
                for (name, slot) in &ov.slots {
                    if *slot == Slot::Whiteout {
                        out.push(pb::Record {
                            record: Some(pb::record::Record::Whiteout(pb::Whiteout {
                                dir: *ino,
                                name: os_bytes(name),
                            })),
                        });
                    }
                }
            }
        }
        out
    }

    /// Compacts the log: every record the model stands on, replacing what was logged.
    pub fn compact(&mut self) -> io::Result<()> {
        let records = self.records();
        self.log.compact(&records)
    }

    pub fn log_len(&self) -> u64 {
        self.log.logged()
    }

    /// A model rebuilt from its records. Inodes the kernel no longer references (none, on a fresh
    /// mount: `fresh`) and no name reaches are dropped.
    pub fn load(
        records: Vec<pb::Record>,
        overlay_dir: PathBuf,
        jj_target: Vec<u8>,
        log: Box<dyn Log>,
        clock: fn() -> Time,
        fresh: bool,
    ) -> io::Result<Self> {
        let mut m = Model::empty(Merge::resolved(None), overlay_dir, jj_target, log, clock)?;
        let mut inodes: BTreeMap<Ino, pb::InodeRecord> = BTreeMap::new();
        let mut whiteouts: BTreeSet<(Ino, Vec<u8>)> = BTreeSet::new();
        for r in records {
            match r.record {
                Some(pb::record::Record::Inode(i)) => {
                    inodes.insert(i.ino, i);
                }
                Some(pb::record::Record::Drop(ino)) => {
                    inodes.remove(&ino);
                }
                Some(pb::record::Record::Base(b)) => {
                    m.base = terms_in(b.base.as_ref());
                    m.recorded = terms_in(b.recorded.as_ref());
                    m.recorded_commit = b.recorded_commit;
                    m.sparse = b
                        .sparse
                        .iter()
                        .filter_map(|p| RepoPathBuf::from_internal_string(p.as_str()).ok())
                        .collect();
                }
                Some(pb::record::Record::Whiteout(w)) => {
                    whiteouts.insert((w.dir, w.name));
                }
                Some(pb::record::Record::Unwhiteout(w)) => {
                    whiteouts.remove(&(w.dir, w.name));
                }
                // A snapshot's mark, which the log's reader takes off before the model sees it.
                Some(pb::record::Record::Compaction(_)) | None => {}
            }
        }
        let t = clock();
        for (ino, r) in &inodes {
            let kind = match r.kind() {
                pb::inode_record::Kind::Dir => Kind::Dir,
                pb::inode_record::Kind::Symlink => Kind::Symlink,
                pb::inode_record::Kind::Fifo => Kind::Fifo,
                pb::inode_record::Kind::Socket => Kind::Socket,
                _ => Kind::File,
            };
            let content = match r.state() {
                pb::inode_record::State::Fixed => match r.value.as_ref().and_then(value_in) {
                    Some(v) => Content::Fixed(v),
                    None => continue,
                },
                pb::inode_record::State::Materialized => Content::Materialized,
                _ => Content::Derived,
            };
            let mut dir = (kind == Kind::Dir).then(DirState::default);
            if let (Some(d), Content::Materialized) = (&mut dir, &content) {
                d.overlay = Some(Overlay {
                    under: if r.under_base {
                        Under::Base
                    } else if let Some(u) = &r.under {
                        Under::Tree(terms_in(Some(u)))
                    } else {
                        Under::Empty
                    },
                    slots: BTreeMap::new(),
                });
            }
            let size = if kind == Kind::File && matches!(content, Content::Materialized) {
                std::fs::metadata(m.overlay_path(*ino))
                    .map(|md| md.len())
                    .unwrap_or(0)
            } else {
                0
            };
            let inode = Inode {
                parent: r.parent,
                name: OsString::from_vec(r.name.clone()),
                extra: r
                    .extra_links
                    .iter()
                    .map(|l| (l.parent, OsString::from_vec(l.name.clone())))
                    .collect(),
                kind,
                content,
                derived: None,
                dir,
                target: r.symlink_target.clone(),
                meta: Meta {
                    mode: r.mode,
                    mtime: (r.mtime_sec, r.mtime_nsec),
                    ctime: (r.ctime_sec, r.ctime_nsec),
                    atime: (r.atime_sec, r.atime_nsec),
                    xattrs: {
                        let mut xattrs: Xattrs = r
                            .xattrs
                            .iter()
                            .map(|x| (OsString::from_vec(x.name.clone()), x.value.clone()))
                            .collect();
                        acl::reconcile(&mut xattrs, r.mode, kind == Kind::Dir);
                        xattrs
                    },
                },
                version: 1,
                size,
                file_id: None,
                checkout_conflict: r.checkout_conflict,
                marker_len: r.conflict_marker_len,
                redirect: r.redirect,
                nlookup: if fresh { 0 } else { u64::MAX / 2 },
                open: 0,
            };
            if *ino == ROOT {
                let root = m.inodes.get_mut(&ROOT).unwrap();
                root.meta = inode.meta;
                continue;
            }
            m.inodes.insert(*ino, inode);
            m.next = m.next.max(ino + 1);
        }
        let _ = t;
        // Links: slots for overlay children, the children cache for derived ones.
        for (ino, r) in &inodes {
            if *ino == ROOT || !m.inodes.contains_key(ino) {
                continue;
            }
            let mut links = vec![(r.parent, OsString::from_vec(r.name.clone()))];
            links.extend(
                r.extra_links
                    .iter()
                    .map(|l| (l.parent, OsString::from_vec(l.name.clone()))),
            );
            for (p, n) in links {
                if p == 0 {
                    continue;
                }
                let Some(d) = m.inodes.get_mut(&p).and_then(|pi| pi.dir.as_mut()) else {
                    continue;
                };
                if r.overlay
                    && let Some(ov) = d.overlay.as_mut()
                {
                    ov.slots.insert(n, Slot::Node(*ino));
                    continue;
                }
                d.children.insert(n, *ino);
            }
        }
        for (dir, name) in whiteouts {
            if let Some(ov) = m
                .inodes
                .get_mut(&dir)
                .and_then(|d| d.dir.as_mut())
                .and_then(|d| d.overlay.as_mut())
            {
                ov.slots
                    .entry(OsString::from_vec(name))
                    .or_insert(Slot::Whiteout);
            }
        }
        if fresh {
            let orphans: Vec<Ino> = m
                .inodes
                .iter()
                .filter(|(i, n)| **i != ROOT && **i != JJ && n.parent == 0)
                .map(|(i, _)| *i)
                .collect();
            for o in orphans {
                m.maybe_drop(o);
            }
            m.flush().map_err(|e| match e {
                Error::Errno(n) => io::Error::from_raw_os_error(n),
                Error::Need(_) => io::Error::other("a need while loading"),
            })?;
        }
        Ok(m)
    }

    /// Keeps a redirection at `path`: a symlink to `target` (a directory on local disk), its parent
    /// directories made as needed. Refused (`EEXIST`) where something else — a tracked file or
    /// directory, or the user's own — already stands.
    pub fn set_redirect(&mut self, path: &RepoPath, target: &[u8]) -> R<()> {
        let Some((parent_path, name)) = path.split() else {
            return errno(libc::EINVAL);
        };
        let mut dir = ROOT;
        for c in parent_path.components() {
            let n = OsStr::new(c.as_internal_str());
            dir = match self.find(dir, n)? {
                Some(i) if self.inode(i)?.kind == Kind::Dir => i,
                Some(_) => return errno(libc::EEXIST),
                None => {
                    let i = self.create(dir, n, Kind::Dir, 0o755, b"")?;
                    self.inode_mut(i)?.nlookup = 0;
                    i
                }
            };
        }
        let name = OsStr::new(name.as_internal_str());
        if let Some(old) = self.find(dir, name)? {
            let i = self.inode(old)?;
            if !i.redirect {
                return errno(libc::EEXIST);
            }
            if i.target == target {
                return Ok(());
            }
            self.unlist(dir, name, old)?;
            self.maybe_drop(old);
        }
        let ino = self.create(dir, name, Kind::Symlink, 0o777, target)?;
        let i = self.inode_mut(ino)?;
        i.redirect = true;
        i.nlookup = 0;
        self.touch(ino);
        self.flush()
    }

    /// The redirections the overlay holds, by path.
    pub fn redirects(&self) -> Vec<RepoPathBuf> {
        let mut out: Vec<RepoPathBuf> = self
            .inodes
            .iter()
            .filter(|(_, i)| i.redirect && i.parent != 0)
            .filter_map(|(ino, _)| self.path_of(*ino))
            .collect();
        out.sort();
        out
    }

    /// Removes the redirection at `path`, if one is there.
    pub fn remove_redirect(&mut self, path: &RepoPath) -> R<()> {
        let Some(ino) = self.resolve(path)? else {
            return Ok(());
        };
        if !self.inode(ino)?.redirect {
            return Ok(());
        }
        let (parent, name) = {
            let i = self.inode(ino)?;
            (i.parent, i.name.clone())
        };
        self.unlist(parent, &name, ino)?;
        self.maybe_drop(ino);
        self.flush()
    }

    /// Shows a file at the root beside the base's — a presentation's: a file a service puts beside
    /// a commit's, such as the job context — replacing what was there. Returns what the kernel must
    /// drop.
    pub fn put_file(&mut self, name: &OsStr, contents: &[u8]) -> R<Vec<Stale>> {
        let mut stale = vec![Stale::Entry(ROOT, name.to_owned())];
        if let Some(old) = self.find(ROOT, name)? {
            if self.inode(old)?.kind == Kind::Dir {
                return errno(libc::EISDIR);
            }
            stale.push(Stale::Inode(old));
            self.unlist(ROOT, name, old)?;
            self.maybe_drop(old);
        }
        let ino = self.create(ROOT, name, Kind::File, 0o644, b"")?;
        self.overlay_file(ino)?.write_all_at(contents, 0)?;
        let i = self.inode_mut(ino)?;
        i.size = contents.len() as u64;
        i.version += 1;
        i.nlookup = 0;
        self.touch(ino);
        self.flush()?;
        Ok(stale)
    }

    /// Removes what `put_file` put at the root: the base's entry of that name, if it has one, shows
    /// again. Returns what the kernel must drop.
    pub fn drop_file(&mut self, name: &OsStr) -> R<Vec<Stale>> {
        let slot = self
            .inode(ROOT)?
            .dir
            .as_ref()
            .and_then(|d| d.overlay.as_ref())
            .and_then(|ov| ov.slots.get(name).copied());
        let mut stale = Vec::new();
        match slot {
            Some(Slot::Node(ino)) => self.drop_slot(ROOT, name, Some(ino), &mut stale),
            Some(Slot::Whiteout) => self.drop_slot(ROOT, name, None, &mut stale),
            None => return Ok(stale),
        }
        self.flush()?;
        Ok(stale)
    }

    /// Statistics for `jj fs status`: materialized files and their bytes.
    pub fn overlay_stats(&self) -> (u64, u64) {
        let mut files = 0;
        let mut bytes = 0;
        for i in self.inodes.values() {
            if i.kind == Kind::File && matches!(i.content, Content::Materialized) {
                files += 1;
                bytes += i.size;
            }
        }
        (files, bytes)
    }

    /// Every file ID ok holds content for under a path, to prefetch (derived and fixed files
    /// only: what is written is already local).
    pub fn files_under(&mut self, path: &RepoPath, out: &mut Vec<FileId>, limit: usize) -> R<()> {
        let Some(ino) = self.resolve(path)? else {
            return Ok(());
        };
        let mut stack = vec![ino];
        while let Some(i) = stack.pop() {
            if out.len() >= limit {
                return Ok(());
            }
            match self.inode(i)?.kind {
                Kind::Dir => {
                    for e in self.list(i)? {
                        if i == ROOT && e.name == JJ_NAME {
                            continue;
                        }
                        if let Some(c) = self.find(i, &e.name)? {
                            stack.push(c);
                        }
                    }
                }
                Kind::File => {
                    if !matches!(self.inode(i)?.content, Content::Materialized)
                        && let Value::File { id, .. } = self.value_of(i)?
                    {
                        out.push(id);
                    }
                }
                _ => {}
            }
        }
        self.flush()?;
        Ok(())
    }
}

fn os_bytes(s: &OsStr) -> Vec<u8> {
    s.as_bytes().to_vec()
}

pub fn terms_out(t: &Terms) -> pb::Tree {
    pb::Tree {
        ids: t
            .iter()
            .map(|id| id.as_ref().map(|id| id.to_bytes()).unwrap_or_default())
            .collect(),
    }
}

pub fn terms_in(t: Option<&pb::Tree>) -> Terms {
    match t {
        None => Merge::resolved(None),
        Some(t) if t.ids.is_empty() => Merge::resolved(None),
        Some(t) => Merge::from_vec(
            t.ids
                .iter()
                .map(|b| (!b.is_empty()).then(|| TreeId::new(b.clone())))
                .collect::<Vec<_>>(),
        ),
    }
}

pub fn value_out(v: &Value) -> pb::TreeValue {
    let mut out = pb::TreeValue::default();
    match v {
        Value::File {
            id,
            executable,
            copy_id,
        } => {
            out.set_type(pb::tree_value::Type::File);
            out.id = id.to_bytes();
            out.executable = *executable;
            out.copy_id = copy_id.to_bytes();
        }
        Value::Symlink(id) => {
            out.set_type(pb::tree_value::Type::Symlink);
            out.id = id.to_bytes();
        }
        Value::Tree(t) => {
            out.set_type(pb::tree_value::Type::Tree);
            if let Some(Some(id)) = t.as_resolved() {
                out.id = id.to_bytes();
            }
        }
        Value::Conflict => {}
    }
    out
}

pub fn value_in(v: &pb::TreeValue) -> Option<Value> {
    Some(match v.r#type() {
        pb::tree_value::Type::File => Value::File {
            id: FileId::new(v.id.clone()),
            executable: v.executable,
            copy_id: CopyId::new(v.copy_id.clone()),
        },
        pb::tree_value::Type::Symlink => Value::Symlink(SymlinkId::new(v.id.clone())),
        pb::tree_value::Type::Tree => Value::Tree(resolved_terms(TreeId::new(v.id.clone()))),
        _ => return None,
    })
}

/// Copies a file: a reflink where the file system supports one (btrfs, XFS), bytes otherwise.
pub fn copy_file(from: &std::path::Path, to: &std::path::Path) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    let src = File::open(from)?;
    let dst = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(to)?;
    const FICLONE: libc::c_ulong = 0x4004_9409;
    if unsafe { libc::ioctl(dst.as_raw_fd(), FICLONE, src.as_raw_fd()) } == 0 {
        return Ok(());
    }
    drop(dst);
    std::fs::copy(from, to)?;
    Ok(())
}
