//! A virtual workspace's snapshot (docs/okfs.md, "jj's working copy"): the tree its working
//! copy holds, computed from what the file system wrote rather than from every file. One walk
//! serves jj's snapshot (over the control socket) and the daemon's checkpointer (in process).
//!
//! It descends from the root through directories that differ from the recorded tree: a directory
//! the overlay never touched, whose source is the recorded tree's own, is the same and is not
//! listed. Within a directory, every name of its source, its overlay, and the recorded tree is
//! compared, with jj's rules for what a new file is: ignored by `.ignore` and `.gitignore` (both
//! read in each directory walked, `.gitignore` winning), not auto-tracked, or too large, it stays
//! untracked; a file the recorded tree tracks stays tracked.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::io;
use std::path::Path;
use std::sync::Arc;

use futures::AsyncReadExt as _;
use jj_lib::backend::BackendError;
use jj_lib::backend::CopyId;
use jj_lib::backend::FileId;
use jj_lib::backend::MergedTreeValue;
use jj_lib::backend::MergedTreeValueExt as _;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::conflicts;
use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::matchers::Matcher;
use jj_lib::merge::Merge;
use jj_lib::merge::SameChange;
use jj_lib::merged_tree::MergedTree;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::repo_path::RepoPathComponent;
use jj_lib::store::Store;
use jj_lib::tree::Tree;
use jj_lib::working_copy::SnapshotError;
use jj_lib::working_copy::UntrackedReason;
use ok_proto::ok::okfs::v1 as pb;

use crate::model::terms_in;
use crate::model::value_in;

/// The file system, as a snapshot reads it.
pub trait FsView: Send + Sync {
    /// A directory: its source and its overlay's entries.
    fn list_dir(&self, path: &RepoPath) -> io::Result<pb::ListDirReply>;
    /// Remembers the content IDs computed for written files' versions.
    fn remember(&self, files: Vec<pb::Remembered>) -> io::Result<()>;
    /// Stores a written secret's value in ok secrets and puts its pointer in the overlay
    /// (docs/secrets.md, "In a virtual workspace"), returning the file's entry as it now is; None
    /// when it is not to be recorded.
    fn seal_secret(&self, path: &RepoPath) -> io::Result<Option<pb::DirEntry>>;
}

/// Which new files become tracked (jj's `SnapshotOptions`).
pub struct Tracking<'a> {
    pub base_ignores: Arc<GitIgnoreFile>,
    pub start_tracking: &'a dyn Matcher,
    pub force_tracking: &'a dyn Matcher,
    pub max_new_file_size: u64,
    /// The sparse patterns: paths outside them are not looked at, and keep their recorded values.
    pub sparse: &'a dyn Matcher,
}

/// A snapshot's result.
pub struct Snapshot {
    pub tree: MergedTree,
    pub untracked: BTreeMap<RepoPathBuf, UntrackedReason>,
    pub invalid_utf8: BTreeSet<(RepoPathBuf, OsString)>,
}

fn other(
    message: impl Into<String>,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> SnapshotError {
    SnapshotError::Other {
        message: message.into(),
        err: err.into(),
    }
}

/// What the file system holds at a name.
enum Fs {
    Absent,
    /// A value from ok: the source's, or one a rename moved.
    Value(MergedTreeValue),
    /// A written file.
    Written(pb::DirEntry),
    /// A written symlink.
    Link(Vec<u8>),
    /// A directory: its listing is read when it is walked.
    Dir,
    /// A FIFO or socket, which jj does not record.
    Other,
}

/// Reads the trees of a merge's terms at a directory (None where a term has none).
async fn trees_at(
    store: &Arc<Store>,
    dir: &RepoPath,
    terms: &Merge<Option<TreeId>>,
) -> Result<Merge<Option<Tree>>, BackendError> {
    let mut out = Vec::new();
    for t in terms.iter() {
        out.push(match t {
            Some(id) => Some(store.get_tree(dir.to_owned(), id).await?),
            None => None,
        });
    }
    Ok(Merge::from_vec(out))
}

/// A name's value across a merge's terms, simplified: in a conflicted commit, whose tree has a term
/// per side, a name no term holds is absent — not a merge of absences, which jj takes for present
/// and for a tree — and one every side holds alike is that value, as the file system shows it.
fn value_at(trees: &Merge<Option<Tree>>, name: &RepoPathComponent) -> MergedTreeValue {
    trees
        .map(|t| t.as_ref().and_then(|t| t.value(name).cloned()))
        .simplify()
}

/// The terms of a tree-valued merge (None where a term is not a tree).
fn tree_terms(v: &MergedTreeValue) -> Merge<Option<TreeId>> {
    v.map(|t| match t {
        Some(TreeValue::Tree(id)) => Some(id.clone()),
        _ => None,
    })
}

fn tree_ids(t: &Merge<Option<Tree>>) -> Merge<Option<TreeId>> {
    t.map(|t| t.as_ref().map(|t| t.id().clone()))
}

async fn read_all(
    store: &Arc<Store>,
    path: &RepoPath,
    id: &FileId,
) -> Result<Vec<u8>, SnapshotError> {
    let mut out = Vec::new();
    store
        .read_file(path, id)
        .await?
        .read_to_end(&mut out)
        .await
        .map_err(|e| other(format!("reading {}", path.as_internal_file_string()), e))?;
    Ok(out)
}

/// A directory to walk.
struct Work {
    path: RepoPathBuf,
    listing: pb::ListDirReply,
    recorded: Merge<Option<Tree>>,
    ignores: Arc<GitIgnoreFile>,
    /// Whether the recorded tree has this directory (a new one is skipped whole when it holds a
    /// nested repository).
    tracked: bool,
}

/// Computes the working copy's tree from what the file system holds, relative to `recorded`.
pub async fn snapshot(
    view: &dyn FsView,
    store: &Arc<Store>,
    recorded: &MergedTree,
    t: &Tracking<'_>,
) -> Result<Snapshot, SnapshotError> {
    let mut builder = MergedTreeBuilder::new(recorded.clone());
    let mut untracked = BTreeMap::new();
    let mut invalid_utf8 = BTreeSet::new();
    let mut remembered = Vec::new();
    let root_listing = view
        .list_dir(RepoPath::root())
        .map_err(|e| other("reading the workspace's root", e))?;
    let root_trees = recorded.trees().await?.map(|t| Some(t.clone()));
    let mut stack = vec![Work {
        path: RepoPathBuf::root(),
        listing: root_listing,
        recorded: root_trees,
        ignores: t.base_ignores.clone(),
        tracked: true,
    }];
    while let Some(w) = stack.pop() {
        let source_terms = terms_in(w.listing.source.as_ref());
        let source_terms = if w.listing.source.is_none() {
            Merge::resolved(None)
        } else {
            source_terms
        };
        if !w.listing.materialized && source_terms == tree_ids(&w.recorded) {
            continue;
        }
        let source = trees_at(store, &w.path, &source_terms).await?;
        let overlay: BTreeMap<Vec<u8>, pb::DirEntry> = w
            .listing
            .entries
            .iter()
            .map(|e| (e.name.clone(), e.clone()))
            .collect();

        // Names: the source's, the recorded tree's, and the overlay's.
        let mut names: BTreeSet<Vec<u8>> = BTreeSet::new();
        for tree in source.iter().chain(w.recorded.iter()).flatten() {
            for n in tree.data().names() {
                names.insert(n.as_internal_str().as_bytes().to_vec());
            }
        }
        names.extend(overlay.keys().cloned());

        // A new directory holding a nested repository is left alone, as jj does.
        if !w.tracked && (names.contains(b".git".as_slice()) || names.contains(b".jj".as_slice())) {
            continue;
        }

        // The directory's ignore files, .ignore first and .gitignore over it. One the overlay does not
        // hold is the source's, read as the file system shows it (model.rs): in a conflicted
        // commit, whose tree has a term per side, a file each side holds alike is that file.
        let mut ignores = w.ignores.clone();
        for file in [".ignore", ".gitignore"] {
            let Ok(c) = RepoPathComponent::new(file) else {
                continue;
            };
            let contents = match overlay.get(file.as_bytes()) {
                Some(e) => fs_contents(store, &w.path.join(c), e).await?,
                None => match value_at(&source, c).resolve_trivial(SameChange::Accept) {
                    Some(Some(TreeValue::File { id, .. })) => {
                        Some(read_all(store, &w.path.join(c), id).await?)
                    }
                    _ => None,
                },
            };
            if let Some(contents) = contents {
                ignores = ignores.chain(&w.path, Path::new(file), &contents)?;
            }
        }

        for name in names {
            let Some(name_str) = std::str::from_utf8(&name).ok() else {
                invalid_utf8.insert((
                    w.path.clone(),
                    OsString::from(String::from_utf8_lossy(&name).into_owned()),
                ));
                continue;
            };
            let Ok(c) = RepoPathComponent::new(name_str) else {
                continue;
            };
            if w.path.is_root() && (name_str == ".jj" || name_str == ".git") {
                continue;
            }
            let child = w.path.join(c);
            let rec = value_at(&w.recorded, c);
            // A redirection (.ok/redirections) is local disk the daemon keeps, never tracked.
            if overlay.get(&name).is_some_and(|e| e.redirect) {
                continue;
            }
            let fs = match overlay.get(&name) {
                Some(e) => match e.kind() {
                    pb::dir_entry::Kind::Deleted => Fs::Absent,
                    pb::dir_entry::Kind::Dir => Fs::Dir,
                    pb::dir_entry::Kind::Other => Fs::Other,
                    pb::dir_entry::Kind::File | pb::dir_entry::Kind::Symlink
                        if e.value.is_some() =>
                    {
                        match e.value.as_ref().and_then(value_in) {
                            Some(v) => Fs::Value(Merge::normal(match v {
                                crate::model::Value::File {
                                    id,
                                    executable,
                                    copy_id,
                                } => TreeValue::File {
                                    id,
                                    executable,
                                    copy_id,
                                },
                                crate::model::Value::Symlink(id) => TreeValue::Symlink(id),
                                _ => continue,
                            })),
                            None => Fs::Absent,
                        }
                    }
                    pb::dir_entry::Kind::File => Fs::Written(e.clone()),
                    pb::dir_entry::Kind::Symlink => Fs::Link(e.symlink_target.clone()),
                    _ => Fs::Absent,
                },
                None => {
                    let v = value_at(&source, c);
                    if v.is_absent() {
                        Fs::Absent
                    } else if v.is_tree() {
                        Fs::Dir
                    } else {
                        Fs::Value(v)
                    }
                }
            };
            let is_dir = matches!(fs, Fs::Dir);
            if is_dir {
                if t.sparse.visit(&child).is_nothing() {
                    continue;
                }
            } else if !t.sparse.matches(&child) {
                continue;
            }
            // A new file: one the recorded tree does not hold as a file.
            let tracked_file = rec.is_present() && !rec.is_tree();
            match fs {
                Fs::Absent | Fs::Other => {
                    if rec.is_present() {
                        builder.set_or_remove(child, Merge::absent());
                    }
                }
                Fs::Dir => {
                    if tracked_file {
                        // A file the recorded tree tracks became a directory.
                        builder.set_or_remove(child.clone(), Merge::absent());
                    }
                    let tracked = rec.is_tree();
                    if !tracked
                        && ignores.matches_dir(&child)
                        && t.force_tracking.visit(&child).is_nothing()
                    {
                        continue;
                    }
                    // A directory whose source is unchanged from the recorded one is the same.
                    let src = value_at(&source, c);
                    let in_overlay = overlay.contains_key(&name);
                    if !in_overlay && tracked && tree_terms(&src) == tree_terms(&rec) {
                        continue;
                    }
                    let listing = if in_overlay {
                        view.list_dir(&child).map_err(|e| {
                            other(format!("reading {}", child.as_internal_file_string()), e)
                        })?
                    } else {
                        // A directory of the source the overlay does not list (an overlay
                        // directory's parent is one): its tree, whole.
                        pb::ListDirReply {
                            source: Some(crate::model::terms_out(&tree_terms(&src))),
                            materialized: false,
                            entries: Vec::new(),
                        }
                    };
                    let recorded = if rec.is_tree() {
                        trees_at(store, &child, &tree_terms(&rec)).await?
                    } else {
                        Merge::resolved(None)
                    };
                    stack.push(Work {
                        path: child,
                        listing,
                        recorded,
                        ignores: ignores.clone(),
                        tracked,
                    });
                }
                Fs::Value(v) => {
                    if v == rec {
                        continue;
                    }
                    if !tracked_file && !new_file_tracked(&child, &ignores, t, None, &mut untracked)
                    {
                        if rec.is_present() {
                            builder.set_or_remove(child, Merge::absent());
                        }
                        continue;
                    }
                    builder.set_or_remove(child, v);
                }
                Fs::Link(target) => {
                    if !tracked_file && !new_file_tracked(&child, &ignores, t, None, &mut untracked)
                    {
                        if rec.is_present() {
                            builder.set_or_remove(child, Merge::absent());
                        }
                        continue;
                    }
                    let target = String::from_utf8(target).map_err(|_| {
                        SnapshotError::InvalidUtf8SymlinkTarget {
                            path: child.to_fs_path_unchecked(Path::new("")),
                        }
                    })?;
                    let id = store.write_symlink(&child, &target).await?;
                    let v = Merge::normal(TreeValue::Symlink(id));
                    if v != rec {
                        builder.set_or_remove(child, v);
                    }
                }
                Fs::Written(e) => {
                    // A secret whose value was lost before it was sealed: the recorded tree keeps
                    // what it had.
                    if e.secret_lost {
                        continue;
                    }
                    if !tracked_file
                        && !new_file_tracked(&child, &ignores, t, Some(e.size), &mut untracked)
                    {
                        if rec.is_present() {
                            builder.set_or_remove(child, Merge::absent());
                        }
                        continue;
                    }
                    // A secret written and tracked: its value is stored now, and its pointer is
                    // what the snapshot records.
                    let e = if e.secret_pending {
                        match view.seal_secret(&child).map_err(|err| {
                            other(
                                format!("storing the secret {}", child.as_internal_file_string()),
                                err,
                            )
                        })? {
                            Some(e) => e,
                            None => continue,
                        }
                    } else {
                        e
                    };
                    let v = written_value(store, &child, &e, &rec, &mut remembered).await?;
                    if v != rec {
                        builder.set_or_remove(child, v);
                    }
                }
            }
        }
    }
    if !remembered.is_empty() {
        view.remember(remembered)
            .map_err(|e| other("remembering content IDs", e))?;
    }
    let tree = builder.write_tree().await?;
    Ok(Snapshot {
        tree,
        untracked,
        invalid_utf8,
    })
}

/// Whether a new file becomes tracked; records why when it does not. A file the ignore rules name
/// is untracked without a reason, as jj leaves it.
fn new_file_tracked(
    path: &RepoPath,
    ignores: &GitIgnoreFile,
    t: &Tracking<'_>,
    size: Option<u64>,
    untracked: &mut BTreeMap<RepoPathBuf, UntrackedReason>,
) -> bool {
    let forced = t.force_tracking.matches(path);
    if !forced && ignores.matches_file(path) {
        return false;
    }
    if !forced && !t.start_tracking.matches(path) {
        untracked.insert(path.to_owned(), UntrackedReason::FileNotAutoTracked);
        return false;
    }
    if let Some(size) = size
        && !forced
        && size > t.max_new_file_size
    {
        untracked.insert(
            path.to_owned(),
            UntrackedReason::FileTooLarge {
                size,
                max_size: t.max_new_file_size,
            },
        );
        return false;
    }
    true
}

/// A written file's contents: from the overlay, or ok for a value moved there.
async fn fs_contents(
    store: &Arc<Store>,
    path: &RepoPath,
    e: &pb::DirEntry,
) -> Result<Option<Vec<u8>>, SnapshotError> {
    match e.kind() {
        pb::dir_entry::Kind::File if !e.overlay_path.is_empty() => {
            Ok(Some(std::fs::read(&e.overlay_path).map_err(|err| {
                other(format!("reading {}", path.as_internal_file_string()), err)
            })?))
        }
        pb::dir_entry::Kind::File => match e.value.as_ref().and_then(value_in) {
            Some(crate::model::Value::File { id, .. }) => {
                Ok(Some(read_all(store, path, &id).await?))
            }
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

/// A written file's value: its content ID (known for its version, or computed and remembered), its
/// executable bit, and the recorded file's copy ID — or, over a recorded conflict, the conflict as
/// its markers now say.
async fn written_value(
    store: &Arc<Store>,
    path: &RepoPath,
    e: &pb::DirEntry,
    rec: &MergedTreeValue,
    remembered: &mut Vec<pb::Remembered>,
) -> Result<MergedTreeValue, SnapshotError> {
    if rec.as_resolved().is_none() {
        if e.checkout_conflict {
            return Ok(rec.clone());
        }
        if let Some(old_ids) = rec.to_file_merge() {
            let contents = std::fs::read(&e.overlay_path)
                .map_err(|err| other(format!("reading {}", path.as_internal_file_string()), err))?;
            let marker_len = if e.conflict_marker_len > 0 {
                e.conflict_marker_len as usize
            } else {
                conflicts::MIN_CONFLICT_MARKER_LEN
            };
            let new_ids =
                conflicts::update_from_content(&old_ids, store, path, &contents, marker_len)
                    .await?;
            let copy_id = rec
                .to_copy_id_merge()
                .and_then(|m| m.resolve_trivial(SameChange::Accept).cloned().flatten())
                .unwrap_or_else(CopyId::placeholder);
            return Ok(match new_ids.into_resolved() {
                Ok(id) => Merge::normal(TreeValue::File {
                    id: id.ok_or_else(|| {
                        other(
                            format!("{} resolved to nothing", path.as_internal_file_string()),
                            "an absent file",
                        )
                    })?,
                    executable: e.executable,
                    copy_id,
                }),
                Err(new_ids) => {
                    if new_ids != old_ids {
                        rec.with_new_file_ids(&new_ids)
                    } else {
                        rec.clone()
                    }
                }
            });
        }
    }
    let id = if !e.file_id.is_empty() {
        FileId::new(e.file_id.clone())
    } else {
        let file = std::fs::File::open(&e.overlay_path)
            .map_err(|err| other(format!("reading {}", path.as_internal_file_string()), err))?;
        let mut reader = futures::io::AllowStdIo::new(file);
        let id = store.write_file(path, &mut reader).await?;
        remembered.push(pb::Remembered {
            ino: e.ino,
            version: e.version,
            file_id: id.to_bytes(),
        });
        id
    };
    let copy_id = match rec.as_resolved() {
        Some(Some(TreeValue::File { copy_id, .. })) => copy_id.clone(),
        _ => CopyId::placeholder(),
    };
    Ok(Merge::normal(TreeValue::File {
        id,
        executable: e.executable,
        copy_id,
    }))
}
