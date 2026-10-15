//! okfs's jj working copy (docs/okfs.md, "jj's working copy"): a virtual workspace, whose
//! files the daemon serves. A snapshot reads only what the overlay holds (the walk in
//! [`crate::snapshot`], over the control socket); a checkout moves the daemon's base without
//! writing a file, but for conflicts, which arrive materialized.
//!
//! Its state (`.jj/working_copy/okfs.toml`) is the workspace's ID, the tree and operation it was
//! last updated to, the commit it was checked out at, and its sparse patterns.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use jj_lib::backend::TreeId;
use jj_lib::commit::Commit;
use jj_lib::conflict_labels::ConflictLabels;
use jj_lib::conflicts::ConflictMarkerStyle;
use jj_lib::conflicts::ConflictMaterializeOptions;
use jj_lib::conflicts::MaterializedTreeValue;
use jj_lib::conflicts::choose_materialized_conflict_marker_len;
use jj_lib::conflicts::materialize_merge_result_to_bytes;
use jj_lib::conflicts::materialize_tree_value;
use jj_lib::lock::FileLock;
use jj_lib::matchers::DifferenceMatcher;
use jj_lib::matchers::PrefixMatcher;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use jj_lib::working_copy::CheckoutError;
use jj_lib::working_copy::CheckoutStats;
use jj_lib::working_copy::LockedWorkingCopy;
use jj_lib::working_copy::ResetError;
use jj_lib::working_copy::SnapshotError;
use jj_lib::working_copy::SnapshotOptions;
use jj_lib::working_copy::SnapshotStats;
use jj_lib::working_copy::WorkingCopy;
use jj_lib::working_copy::WorkingCopyFactory;
use jj_lib::working_copy::WorkingCopyStateError;
use ok_proto::ok::okfs::v1 as pb;
use serde::Deserialize;
use serde::Serialize;

use crate::control::Client;
use crate::control::Paths;
use crate::control::private_dir;
use crate::snapshot::FsView;
use crate::snapshot::Tracking;

/// The working copy's type, as a workspace records it.
pub const NAME: &str = "okfs";

const STATE: &str = "okfs.toml";

fn state_err(
    message: impl Into<String>,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> WorkingCopyStateError {
    WorkingCopyStateError {
        message: message.into(),
        err: err.into(),
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct State {
    workspace_id: String,
    workspace_name: String,
    /// The tree, as its terms' hex IDs ("" for a term with none).
    tree: Vec<String>,
    operation_id: String,
    commit: String,
    sparse: Vec<String>,
}

fn tree_terms(t: &MergedTree) -> Vec<String> {
    t.tree_ids().iter().map(|id| id.hex()).collect()
}

fn tree_pb(t: &MergedTree) -> pb::Tree {
    pb::Tree {
        ids: t.tree_ids().iter().map(|id| id.to_bytes()).collect(),
    }
}

impl State {
    fn read(state_path: &Path) -> Result<Self, WorkingCopyStateError> {
        let p = state_path.join(STATE);
        let text = std::fs::read_to_string(&p)
            .map_err(|e| state_err(format!("reading {}", p.display()), e))?;
        toml::from_str(&text).map_err(|e| state_err(format!("parsing {}", p.display()), e))
    }

    fn write(&self, state_path: &Path) -> Result<(), WorkingCopyStateError> {
        let p = state_path.join(STATE);
        let text =
            toml::to_string(self).map_err(|e| state_err("encoding the working copy's state", e))?;
        let tmp = state_path.join(format!("{STATE}.tmp"));
        std::fs::write(&tmp, text)
            .map_err(|e| state_err(format!("writing {}", tmp.display()), e))?;
        std::fs::rename(&tmp, &p).map_err(|e| state_err(format!("writing {}", p.display()), e))
    }

    fn tree(&self, store: &Arc<Store>) -> Result<MergedTree, WorkingCopyStateError> {
        if self.tree.is_empty() {
            return Ok(store.empty_merged_tree());
        }
        let ids: Vec<TreeId> = self
            .tree
            .iter()
            .map(|h| {
                TreeId::try_from_hex(h)
                    .ok_or_else(|| state_err("a tree ID in the working copy's state", h.clone()))
            })
            .collect::<Result<_, _>>()?;
        Ok(MergedTree::new(
            store.clone(),
            Merge::from_vec(ids),
            ConflictLabels::unlabeled(),
        ))
    }

    fn sparse(&self) -> Vec<RepoPathBuf> {
        self.sparse
            .iter()
            .filter_map(|p| RepoPathBuf::from_internal_string(p.as_str()).ok())
            .collect()
    }
}

/// The daemon's connection, for one call.
fn daemon() -> Result<Client, WorkingCopyStateError> {
    crate::client::connect(&Paths::from_env()).map_err(|e| state_err("reaching the okfs daemon", e))
}

fn call(req: pb::request::Request) -> Result<pb::response::Response, WorkingCopyStateError> {
    daemon()?
        .call(req)
        .map_err(|e| state_err("the okfs daemon", e))
}

/// The workspace's directory under the daemon's state, from the working copy's state path (which
/// `.jj` links there).
pub fn workspace_state_dir(state_path: &Path) -> PathBuf {
    let real = dunce::canonicalize(state_path).unwrap_or_else(|_| state_path.to_owned());
    // <workspaces>/<id>/jj/working_copy
    real.parent()
        .and_then(Path::parent)
        .map(Path::to_owned)
        .unwrap_or(real)
}

/// Makes and loads okfs's working copy.
pub struct OkfsWorkingCopyFactory;

/// A new virtual workspace's ID and its directory in the daemon's state, made now: the
/// workspace's own repository (`repo` in it) is made there before the workspace is mounted, so
/// forgetting the workspace removes its repository with it.
pub fn new_workspace() -> Result<(String, std::path::PathBuf), WorkingCopyStateError> {
    let paths = Paths::from_env();
    let mut b = [0u8; 8];
    getrandom(&mut b).map_err(|e| state_err("making a workspace ID", e))?;
    let id = hex::encode(b);
    let state = paths.workspace(&id);
    private_dir(&state).map_err(|e| state_err(format!("making {}", state.display()), e))?;
    Ok((id, state))
}

/// Makes virtual workspace `id` (see new_workspace) at `root`: its `.jj` (jj laid it out there)
/// moves into the daemon's state, the repository it names becomes absolute, and the daemon mounts
/// the workspace over `root` with `tree` — or, attaching, at the workspace's latest checkpoint.
/// Returns the checkpoint it was attached at (0: none).
fn make_virtual(
    id: &str,
    root: &Path,
    repo_dir: &Path,
    workspace_name: &WorkspaceName,
    tree: &MergedTree,
    sparse: &[RepoPathBuf],
    attach: bool,
) -> Result<i64, WorkingCopyStateError> {
    // Resolved while .jj is still here: jj may have named the repository relative to it.
    let repo = dunce::canonicalize(repo_dir)
        .map_err(|e| state_err(format!("{}", repo_dir.display()), e))?;
    let paths = Paths::from_env();
    let id = id.to_owned();
    let state = paths.workspace(&id);
    private_dir(&state).map_err(|e| state_err(format!("making {}", state.display()), e))?;
    let jj = state.join("jj");
    let old_jj = root.join(".jj");
    if old_jj.is_dir() {
        move_dir(&old_jj, &jj).map_err(|e| {
            state_err(
                format!("moving {} into the daemon's state", old_jj.display()),
                e,
            )
        })?;
    } else {
        std::fs::create_dir_all(jj.join("working_copy"))
            .map_err(|e| state_err("making the working copy's state", e))?;
    }
    std::fs::write(jj.join("repo"), repo.as_os_str().as_encoded_bytes())
        .map_err(|e| state_err("writing .jj/repo", e))?;
    let reply = call(pb::request::Request::Mount(pb::Mount {
        workspace_id: id.clone(),
        mount_point: root.display().to_string(),
        repo_dir: repo.display().to_string(),
        workspace_name: workspace_name.as_str().to_owned(),
        tree: Some(tree_pb(tree)),
        sparse: sparse
            .iter()
            .map(|p| p.as_internal_file_string().to_owned())
            .collect(),
        attach,
    }));
    match reply {
        Ok(pb::response::Response::Mount(m)) => Ok(m.checkpoint_id),
        Ok(_) => Err(state_err(
            "the okfs daemon",
            "answered Mount with something else",
        )),
        Err(e) => {
            // Nothing was mounted: the state goes, and .jj comes back for jj to clean up.
            let _ = move_dir(&jj, &old_jj);
            let _ = std::fs::remove_dir_all(&state);
            Err(e)
        }
    }
}

fn new_state(
    root: &Path,
    id: String,
    name: &WorkspaceName,
    tree: &MergedTree,
    operation_id: &OperationId,
    commit: String,
) -> Result<(State, PathBuf), WorkingCopyStateError> {
    let state_path = root.join(".jj").join("working_copy");
    let st = State {
        workspace_id: id,
        workspace_name: name.as_str().to_owned(),
        tree: tree_terms(tree),
        operation_id: operation_id.hex(),
        commit,
        sparse: vec![String::new()],
    };
    st.write(&state_path)?;
    std::fs::write(state_path.join("type"), NAME)
        .map_err(|e| state_err("writing .jj/working_copy/type", e))?;
    Ok((st, state_path))
}

/// New virtual workspace `id` (see new_workspace) at `root` (an empty directory), of the
/// repository at `repo_dir`: what `jj clone` makes. Its working copy starts at the empty tree; the
/// clone checks out its commit.
pub fn init_virtual(
    id: &str,
    store: Arc<Store>,
    root: &Path,
    repo_dir: &Path,
    name: &WorkspaceName,
    operation_id: OperationId,
    settings: &UserSettings,
) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
    let tree = store.empty_merged_tree();
    make_virtual(
        id,
        root,
        repo_dir,
        name,
        &tree,
        &[RepoPathBuf::root()],
        false,
    )?;
    let (st, state_path) = new_state(
        root,
        id.to_owned(),
        name,
        &tree,
        &operation_id,
        String::new(),
    )?;
    Ok(Box::new(OkfsWorkingCopy::new(
        store,
        root.to_owned(),
        state_path,
        st,
        settings,
    )?))
}

/// An existing workspace, attached as virtual workspace `id` (see new_workspace) at `root` at its
/// latest checkpoint (or, with none, at its working-copy commit): `jj clone --attach`. Returns
/// the working copy and the checkpoint.
#[allow(clippy::too_many_arguments)]
pub fn attach_virtual(
    id: &str,
    store: Arc<Store>,
    root: &Path,
    repo_dir: &Path,
    name: &WorkspaceName,
    operation_id: OperationId,
    commit: &Commit,
    settings: &UserSettings,
) -> Result<(Box<dyn WorkingCopy>, i64), WorkingCopyStateError> {
    let tree = commit.tree();
    let checkpoint = make_virtual(
        id,
        root,
        repo_dir,
        name,
        &tree,
        &[RepoPathBuf::root()],
        true,
    )?;
    let (st, state_path) = new_state(
        root,
        id.to_owned(),
        name,
        &tree,
        &operation_id,
        commit.id().hex(),
    )?;
    Ok((
        Box::new(OkfsWorkingCopy::new(
            store,
            root.to_owned(),
            state_path,
            st,
            settings,
        )?),
        checkpoint,
    ))
}

fn getrandom(b: &mut [u8]) -> std::io::Result<()> {
    use std::io::Read as _;
    std::fs::File::open("/dev/urandom")?.read_exact(b)
}

/// Moves a directory, copying across file systems.
fn move_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    match std::fs::rename(from, to) {
        Ok(()) => return Ok(()),
        Err(e) if e.raw_os_error() != Some(libc::EXDEV) => return Err(e),
        Err(_) => {}
    }
    copy_tree(from, to)?;
    std::fs::remove_dir_all(from)
}

fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(from)? {
        let e = e?;
        let t = e.file_type()?;
        let dst = to.join(e.file_name());
        if t.is_dir() {
            copy_tree(&e.path(), &dst)?;
        } else if t.is_symlink() {
            std::os::unix::fs::symlink(std::fs::read_link(e.path())?, dst)?;
        } else {
            std::fs::copy(e.path(), dst)?;
        }
    }
    Ok(())
}

impl WorkingCopyFactory for OkfsWorkingCopyFactory {
    fn init_working_copy(
        &self,
        store: Arc<Store>,
        working_copy_path: PathBuf,
        state_path: PathBuf,
        operation_id: OperationId,
        workspace_name: WorkspaceNameBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        // jj makes a working copy this way only for a workspace it adds to a repository another
        // workspace has: an ok workspace is one jj workspace, isolated from every other
        // (docs/scm.md, "Decisions", 3), and `jj clone` makes another.
        let _ = (store, working_copy_path, state_path, operation_id, settings);
        Err(state_err(
            format!("workspace {}", workspace_name.as_symbol()),
            "an ok workspace is one jj workspace, isolated from every other: `jj clone` makes another",
        ))
    }

    fn load_working_copy(
        &self,
        store: Arc<Store>,
        working_copy_path: PathBuf,
        state_path: PathBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let st = State::read(&state_path)?;
        Ok(Box::new(OkfsWorkingCopy::new(
            store,
            working_copy_path,
            state_path,
            st,
            settings,
        )?))
    }
}

pub struct OkfsWorkingCopy {
    store: Arc<Store>,
    root: PathBuf,
    state_path: PathBuf,
    st: State,
    tree: MergedTree,
    operation_id: OperationId,
    workspace_name: WorkspaceNameBuf,
    sparse: Vec<RepoPathBuf>,
    marker_style: ConflictMarkerStyle,
}

impl OkfsWorkingCopy {
    fn new(
        store: Arc<Store>,
        root: PathBuf,
        state_path: PathBuf,
        st: State,
        settings: &UserSettings,
    ) -> Result<Self, WorkingCopyStateError> {
        let tree = st.tree(&store)?;
        let operation_id = OperationId::try_from_hex(&st.operation_id)
            .ok_or_else(|| state_err("the working copy's operation", st.operation_id.clone()))?;
        let marker_style = settings
            .get("ui.conflict-marker-style")
            .map_err(|e| state_err("ui.conflict-marker-style", e))?;
        Ok(Self {
            workspace_name: WorkspaceNameBuf::from(st.workspace_name.clone()),
            sparse: st.sparse(),
            store,
            root,
            state_path,
            st,
            tree,
            operation_id,
            marker_style,
        })
    }

    /// The workspace's ID in the daemon.
    pub fn workspace_id(&self) -> &str {
        &self.st.workspace_id
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[async_trait(?Send)]
impl WorkingCopy for OkfsWorkingCopy {
    fn name(&self) -> &str {
        NAME
    }

    fn workspace_name(&self) -> &WorkspaceName {
        &self.workspace_name
    }

    fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    fn tree(&self) -> Result<&MergedTree, WorkingCopyStateError> {
        Ok(&self.tree)
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        Ok(&self.sparse)
    }

    async fn start_mutation(&self) -> Result<Box<dyn LockedWorkingCopy>, WorkingCopyStateError> {
        let lock = FileLock::lock(self.state_path.join("working_copy.lock"))
            .map_err(|e| state_err("locking the working copy", e))?;
        // Read again under the lock: another process may have changed it.
        let st = State::read(&self.state_path)?;
        let tree = st.tree(&self.store)?;
        Ok(Box::new(LockedOkfsWorkingCopy {
            store: self.store.clone(),
            root: self.root.clone(),
            state_path: self.state_path.clone(),
            old_operation_id: OperationId::try_from_hex(&st.operation_id).ok_or_else(|| {
                state_err("the working copy's operation", st.operation_id.clone())
            })?,
            old_tree: tree.clone(),
            tree,
            sparse: st.sparse(),
            st,
            marker_style: self.marker_style,
            _lock: lock,
        }))
    }
}

struct LockedOkfsWorkingCopy {
    store: Arc<Store>,
    root: PathBuf,
    state_path: PathBuf,
    old_operation_id: OperationId,
    old_tree: MergedTree,
    tree: MergedTree,
    sparse: Vec<RepoPathBuf>,
    st: State,
    marker_style: ConflictMarkerStyle,
    _lock: FileLock,
}

/// The file system over the control socket, for a snapshot.
struct RemoteView {
    client: Mutex<Client>,
    workspace_id: String,
}

impl FsView for RemoteView {
    fn list_dir(&self, path: &RepoPath) -> std::io::Result<pb::ListDirReply> {
        match self
            .client
            .lock()
            .unwrap()
            .call(pb::request::Request::ListDir(pb::ListDir {
                workspace_id: self.workspace_id.clone(),
                path: path.as_internal_file_string().to_owned(),
            }))? {
            pb::response::Response::ListDir(l) => Ok(l),
            _ => Err(std::io::Error::other(
                "the okfs daemon answered ListDir with something else",
            )),
        }
    }

    fn remember(&self, files: Vec<pb::Remembered>) -> std::io::Result<()> {
        self.client
            .lock()
            .unwrap()
            .call(pb::request::Request::RememberFiles(pb::RememberFiles {
                workspace_id: self.workspace_id.clone(),
                files,
            }))?;
        Ok(())
    }
}

fn checkout_err(
    message: impl Into<String>,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> CheckoutError {
    CheckoutError::Other {
        message: message.into(),
        err: err.into(),
    }
}

impl LockedOkfsWorkingCopy {
    /// Moves the daemon's workspace from the current tree to `new_tree` (over `matcher`'s
    /// paths): materializes the new tree's conflicts that differ, and counts what changed.
    async fn move_to(&mut self, new_tree: &MergedTree) -> Result<CheckoutStats, CheckoutError> {
        let matcher = PrefixMatcher::new(&self.sparse);
        // No file is written, so none is counted: the checkout's cost is the overlay's, not the
        // change's. Only conflicts are written, materialized: those the new tree has that differ
        // from the old (or whose labels alone changed, as jj's own working copy rewrites them).
        let mut stats = CheckoutStats {
            updated_files: 0,
            added_files: 0,
            removed_files: 0,
            skipped_files: 0,
        };
        let mut conflicts = Vec::new();
        if !new_tree.tree_ids().is_resolved() {
            let relabeled = self.tree.labels() != new_tree.labels();
            for (path, value) in new_tree.conflicts_matching(&matcher) {
                let value = value?;
                if !relabeled && self.tree.path_value(&path).await? == value {
                    continue;
                }
                if let Some(c) = self.materialize(&path, value, new_tree).await? {
                    conflicts.push(c);
                }
            }
        }
        let reply = match call(pb::request::Request::CheckOut(pb::CheckOut {
            workspace_id: self.st.workspace_id.clone(),
            old_tree: Some(tree_pb(&self.tree)),
            new_tree: Some(tree_pb(new_tree)),
            conflicts,
        }))
        .map_err(|e| checkout_err("checking out through the okfs daemon", e))?
        {
            pb::response::Response::CheckOut(r) => r,
            _ => {
                return Err(checkout_err(
                    "the okfs daemon",
                    "answered CheckOut with something else",
                ));
            }
        };
        stats.skipped_files = reply.skipped.len() as u32;
        self.tree = new_tree.clone();
        Ok(stats)
    }

    async fn materialize(
        &self,
        path: &RepoPath,
        value: jj_lib::backend::MergedTreeValue,
        new_tree: &MergedTree,
    ) -> Result<Option<pb::Materialized>, CheckoutError> {
        Ok(
            match materialize_tree_value(&self.store, path, value, new_tree.labels()).await? {
                MaterializedTreeValue::FileConflict(file) => {
                    let len = choose_materialized_conflict_marker_len(&file.contents);
                    let options = ConflictMaterializeOptions {
                        marker_style: self.marker_style,
                        marker_len: Some(len),
                        merge: self.store.merge_options().clone(),
                    };
                    Some(pb::Materialized {
                        path: path.as_internal_file_string().to_owned(),
                        contents: materialize_merge_result_to_bytes(
                            &file.contents,
                            &file.labels,
                            &options,
                        )
                        .into(),
                        executable: file.executable.unwrap_or(false),
                        conflict_marker_len: len as u32,
                    })
                }
                MaterializedTreeValue::OtherConflict { id, labels } => {
                    use jj_lib::backend::MergedTreeValueExt as _;
                    Some(pb::Materialized {
                        path: path.as_internal_file_string().to_owned(),
                        contents: id.describe(&labels).into_bytes(),
                        executable: false,
                        conflict_marker_len: 0,
                    })
                }
                _ => None,
            },
        )
    }

    fn record(&self, tree: &MergedTree, checkpoint: bool) -> Result<(), WorkingCopyStateError> {
        call(pb::request::Request::SetRecorded(pb::SetRecorded {
            workspace_id: self.st.workspace_id.clone(),
            tree: Some(tree_pb(tree)),
            commit_id: hex::decode(&self.st.commit).unwrap_or_default(),
            checkpoint,
        }))?;
        Ok(())
    }
}

#[async_trait]
impl LockedWorkingCopy for LockedOkfsWorkingCopy {
    fn old_operation_id(&self) -> &OperationId {
        &self.old_operation_id
    }

    fn old_tree(&self) -> &MergedTree {
        &self.old_tree
    }

    async fn snapshot(
        &mut self,
        options: &SnapshotOptions,
    ) -> Result<(MergedTree, SnapshotStats), SnapshotError> {
        let client = daemon().map_err(SnapshotError::WorkingCopyStateError)?;
        let view = RemoteView {
            client: Mutex::new(client),
            workspace_id: self.st.workspace_id.clone(),
        };
        let sparse = PrefixMatcher::new(&self.sparse);
        let t = Tracking {
            base_ignores: options.base_ignores.clone(),
            start_tracking: options.start_tracking_matcher,
            force_tracking: options.force_tracking_matcher,
            max_new_file_size: options.max_new_file_size,
            sparse: &sparse,
        };
        let snap = crate::snapshot::snapshot(&view, &self.store, &self.tree, &t).await?;
        // The daemon records it, and keeps it as a checkpoint.
        self.record(&snap.tree, snap.tree.tree_ids() != self.tree.tree_ids())
            .map_err(SnapshotError::WorkingCopyStateError)?;
        self.tree = snap.tree.clone();
        let stats = SnapshotStats {
            untracked_paths: snap.untracked,
            invalid_utf8_paths: snap.invalid_utf8,
        };
        Ok((snap.tree, stats))
    }

    async fn check_out(&mut self, commit: &Commit) -> Result<CheckoutStats, CheckoutError> {
        let new_tree = commit.tree();
        let stats = self.move_to(&new_tree).await?;
        self.st.commit = commit.id().hex();
        Ok(stats)
    }

    fn rename_workspace(&mut self, new_workspace_name: WorkspaceNameBuf) {
        self.st.workspace_name = new_workspace_name.as_str().to_owned();
    }

    async fn reset(&mut self, commit: &Commit) -> Result<(), ResetError> {
        let tree = commit.tree();
        self.st.commit = commit.id().hex();
        self.record(&tree, false).map_err(|e| ResetError::Other {
            message: "recording the tree with the okfs daemon".into(),
            err: e.into(),
        })?;
        self.tree = tree;
        Ok(())
    }

    async fn recover(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.reset(commit).await
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        Ok(&self.sparse)
    }

    async fn set_sparse_patterns(
        &mut self,
        new_sparse_patterns: Vec<RepoPathBuf>,
    ) -> Result<CheckoutStats, CheckoutError> {
        let old = PrefixMatcher::new(&self.sparse);
        let new = PrefixMatcher::new(&new_sparse_patterns);
        let added = DifferenceMatcher::new(&new, &old);
        let removed = DifferenceMatcher::new(&old, &new);
        let added_files = self.tree.entries_matching(&added).count() as u32;
        let removed_files = self.tree.entries_matching(&removed).count() as u32;
        call(pb::request::Request::SetSparse(pb::SetSparse {
            workspace_id: self.st.workspace_id.clone(),
            patterns: new_sparse_patterns
                .iter()
                .map(|p| p.as_internal_file_string().to_owned())
                .collect(),
        }))
        .map_err(|e| checkout_err("setting sparse patterns through the okfs daemon", e))?;
        self.sparse = new_sparse_patterns;
        Ok(CheckoutStats {
            updated_files: 0,
            added_files,
            removed_files,
            skipped_files: 0,
        })
    }

    async fn finish(
        self: Box<Self>,
        operation_id: OperationId,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let mut st = self.st.clone();
        st.tree = tree_terms(&self.tree);
        st.operation_id = operation_id.hex();
        st.sparse = self
            .sparse
            .iter()
            .map(|p| p.as_internal_file_string().to_owned())
            .collect();
        st.write(&self.state_path)?;
        Ok(Box::new(OkfsWorkingCopy {
            workspace_name: WorkspaceNameBuf::from(st.workspace_name.clone()),
            sparse: self.sparse,
            store: self.store,
            root: self.root,
            state_path: self.state_path,
            tree: self.tree,
            operation_id,
            st,
            marker_style: self.marker_style,
        }))
    }
}
