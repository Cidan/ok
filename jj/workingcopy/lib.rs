//! ok's working copy (docs/jj.md, "Ignoring files"): jj's own local working copy,
//! which also honors `.ignore` files.
//!
//! jj reads a `.gitignore` in every directory it snapshots. ok's jj adds `.ignore`, in the
//! same syntax and read the same way: a directory's `.ignore` applies beneath it, and a deeper
//! file's rules win over a shallower one's. `.ignore` is the name ripgrep and fd read in any
//! directory; they read `.gitignore` only inside a git repository, which an ok checkout is not.
//!
//! Before each snapshot the working copy walks the tree for `.ignore` files — skipping every
//! directory jj's snapshot would skip — and hands them to jj as the snapshot's base ignores. jj
//! consults its own `.gitignore` rules after them, so where both name a path, `.gitignore` wins.
//! Everything else, and the state on disk, is jj's local working copy.

use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use jj_lib::commit::Commit;
use jj_lib::gitignore::GitIgnoreError;
use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::local_working_copy::LocalWorkingCopy;
use jj_lib::local_working_copy::LocalWorkingCopyFactory;
use jj_lib::matchers::Matcher;
use jj_lib::merged_tree::MergedTree;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::RepoLoader;
use jj_lib::repo::StoreFactories;
use jj_lib::repo::StoreLoadError;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::repo_path::RepoPathComponent;
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
use jj_lib::workspace::DefaultWorkspaceLoaderFactory;
use jj_lib::workspace::WorkingCopyFactories;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::WorkspaceLoadError;
use jj_lib::workspace::WorkspaceLoader;
use jj_lib::workspace::WorkspaceLoaderFactory;
use jj_lib::workspace::get_working_copy_factory;

/// The working copy's type, as a workspace records it.
pub const NAME: &str = "ok";

/// The ignore file ok's jj reads in every directory, beside jj's own `.gitignore`.
pub const IGNORE_FILE: &str = ".ignore";

/// The ignore file jj reads in every directory.
const GITIGNORE_FILE: &str = ".gitignore";

/// Entries jj's snapshot never looks at, and whose presence in a directory makes it a nested
/// repository it skips (jj-lib's `RESERVED_DIR_NAMES`).
const RESERVED_DIR_NAMES: &[&str] = &[".git", ".jj"];

/// The working copy factories ok's jj registers.
pub fn factories() -> WorkingCopyFactories {
    let mut out = WorkingCopyFactories::new();
    out.insert(
        NAME.to_owned(),
        Box::new(OkWorkingCopyFactory) as Box<dyn WorkingCopyFactory>,
    );
    out
}

/// Makes and loads ok's working copy: jj's local working copy, wrapped.
pub struct OkWorkingCopyFactory;

impl WorkingCopyFactory for OkWorkingCopyFactory {
    fn init_working_copy(
        &self,
        store: Arc<Store>,
        working_copy_path: PathBuf,
        state_path: PathBuf,
        operation_id: OperationId,
        workspace_name: WorkspaceNameBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let inner = LocalWorkingCopyFactory {}.init_working_copy(
            store,
            working_copy_path.clone(),
            state_path,
            operation_id,
            workspace_name,
            settings,
        )?;
        Ok(Box::new(OkWorkingCopy {
            inner,
            root: working_copy_path,
        }))
    }

    fn load_working_copy(
        &self,
        store: Arc<Store>,
        working_copy_path: PathBuf,
        state_path: PathBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let inner = LocalWorkingCopyFactory {}.load_working_copy(
            store,
            working_copy_path.clone(),
            state_path,
            settings,
        )?;
        Ok(Box::new(OkWorkingCopy {
            inner,
            root: working_copy_path,
        }))
    }
}

/// jj's local working copy, snapshotting with the working copy's `.ignore` files.
pub struct OkWorkingCopy {
    inner: Box<dyn WorkingCopy>,
    root: PathBuf,
}

#[async_trait(?Send)]
impl WorkingCopy for OkWorkingCopy {
    fn name(&self) -> &str {
        NAME
    }

    fn workspace_name(&self) -> &WorkspaceName {
        self.inner.workspace_name()
    }

    fn operation_id(&self) -> &OperationId {
        self.inner.operation_id()
    }

    fn tree(&self) -> Result<&MergedTree, WorkingCopyStateError> {
        self.inner.tree()
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        self.inner.sparse_patterns()
    }

    async fn start_mutation(&self) -> Result<Box<dyn LockedWorkingCopy>, WorkingCopyStateError> {
        Ok(Box::new(LockedOkWorkingCopy {
            inner: self.inner.start_mutation().await?,
            root: self.root.clone(),
        }))
    }
}

/// ok's working copy, locked for a change.
struct LockedOkWorkingCopy {
    inner: Box<dyn LockedWorkingCopy>,
    root: PathBuf,
}

/// A working copy's move from one tree to another, in an ok repository.
struct Move {
    shared: std::sync::Arc<ok_store::Shared>,
    old: jj_lib::backend::TreeId,
    new: jj_lib::backend::TreeId,
}

/// Has local, before jj's checkout reads it, what a move changes: the trees the old tree does not
/// hold at the same path, in one request, and the changed files, each as a delta against the file
/// it replaces where that is smaller (docs/scm.md, "Deltas"). jj's checkout reads anything this did
/// not bring itself, as it would have.
async fn read_ahead(m: Option<Move>) {
    let Some(m) = m else { return };
    // A failure is jj's checkout's to meet, reading the same objects.
    let _ = ok_store::checkouts::move_tree(m.shared, &m.old, &m.new).await;
}

impl LockedOkWorkingCopy {
    /// The move to `commit`, where the repository is ok's and the working copy is not sparse.
    fn move_of(&self, commit: &Commit) -> Option<Move> {
        let shared = commit
            .store()
            .backend_impl::<ok_store::OkBackend>()?
            .shared()
            .clone();
        if !matches!(self.inner.sparse_patterns(), Ok([p]) if p.is_root()) {
            return None;
        }
        Some(Move {
            shared,
            old: self.inner.old_tree().tree_ids().as_resolved().cloned()?,
            new: commit.tree().tree_ids().as_resolved().cloned()?,
        })
    }
}

#[async_trait]
impl LockedWorkingCopy for LockedOkWorkingCopy {
    fn old_operation_id(&self) -> &OperationId {
        self.inner.old_operation_id()
    }

    fn old_tree(&self) -> &MergedTree {
        self.inner.old_tree()
    }

    async fn snapshot(
        &mut self,
        options: &SnapshotOptions,
    ) -> Result<(MergedTree, SnapshotStats), SnapshotError> {
        let base_ignores = ignores(
            &self.root,
            &options.base_ignores,
            options.force_tracking_matcher,
        )?;
        let options = SnapshotOptions {
            base_ignores,
            progress: options.progress,
            start_tracking_matcher: options.start_tracking_matcher,
            force_tracking_matcher: options.force_tracking_matcher,
            max_new_file_size: options.max_new_file_size,
        };
        self.inner.snapshot(&options).await
    }

    async fn check_out(&mut self, commit: &Commit) -> Result<CheckoutStats, CheckoutError> {
        read_ahead(self.move_of(commit)).await;
        self.inner.check_out(commit).await
    }

    fn rename_workspace(&mut self, new_workspace_name: WorkspaceNameBuf) {
        self.inner.rename_workspace(new_workspace_name);
    }

    async fn reset(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.inner.reset(commit).await
    }

    async fn recover(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.inner.recover(commit).await
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        self.inner.sparse_patterns()
    }

    async fn set_sparse_patterns(
        &mut self,
        new_sparse_patterns: Vec<RepoPathBuf>,
    ) -> Result<CheckoutStats, CheckoutError> {
        self.inner.set_sparse_patterns(new_sparse_patterns).await
    }

    async fn finish(
        self: Box<Self>,
        operation_id: OperationId,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let inner = self.inner.finish(operation_id).await?;
        Ok(Box::new(OkWorkingCopy {
            inner,
            root: self.root,
        }))
    }
}

/// Loads ok's workspaces: jj's loader, except that a working copy recorded as jj's local one
/// loads as ok's. Every workspace ok's jj opens has ok's stores, and a clone made before
/// ok's jj had its own working copy recorded jj's; the state on disk is the same.
pub struct OkWorkspaceLoaderFactory;

impl WorkspaceLoaderFactory for OkWorkspaceLoaderFactory {
    fn create(
        &self,
        workspace_root: &Path,
    ) -> Result<Box<dyn WorkspaceLoader>, WorkspaceLoadError> {
        Ok(Box::new(OkWorkspaceLoader {
            inner: DefaultWorkspaceLoaderFactory.create(workspace_root)?,
        }))
    }
}

struct OkWorkspaceLoader {
    inner: Box<dyn WorkspaceLoader>,
}

impl WorkspaceLoader for OkWorkspaceLoader {
    fn workspace_root(&self) -> &Path {
        self.inner.workspace_root()
    }

    fn repo_path(&self) -> &Path {
        self.inner.repo_path()
    }

    // jj's loader, choosing the working copy by this loader's type.
    fn load(
        &self,
        user_settings: &UserSettings,
        store_factories: &StoreFactories,
        working_copy_factories: &WorkingCopyFactories,
    ) -> Result<Workspace, WorkspaceLoadError> {
        let repo_loader =
            RepoLoader::init_from_file_system(user_settings, self.repo_path(), store_factories)?;
        let working_copy = get_working_copy_factory(self, working_copy_factories)?
            .load_working_copy(
                repo_loader.store().clone(),
                self.workspace_root().to_owned(),
                self.workspace_root().join(".jj").join("working_copy"),
                user_settings,
            )?;
        Ok(Workspace::new(
            self.workspace_root(),
            self.repo_path().to_owned(),
            working_copy,
            repo_loader,
        )?)
    }

    fn get_working_copy_type(&self) -> Result<String, StoreLoadError> {
        let recorded = self.inner.get_working_copy_type()?;
        if recorded == LocalWorkingCopy::name() {
            return Ok(NAME.to_owned());
        }
        Ok(recorded)
    }
}

/// The `.ignore` rules of the working copy at `root`, chained onto `base`: every directory's
/// `.ignore`, each applying beneath its directory, parents before children. It reads the
/// directories jj's snapshot reads — deciding, as jj does, from the `.ignore` rules and then
/// jj's `.gitignore` rules, which directories are ignored and so not looked into, unless
/// `force_tracking` looks into them — and no others, in parallel as jj does.
pub fn ignores(
    root: &Path,
    base: &Arc<GitIgnoreFile>,
    force_tracking: &dyn Matcher,
) -> Result<Arc<GitIgnoreFile>, SnapshotError> {
    let walk = Walk {
        found: Mutex::new(Vec::new()),
        error: Mutex::new(None),
        force_tracking,
    };
    rayon::scope(|scope| {
        walk.spawn(
            scope,
            Directory {
                disk_dir: root.to_owned(),
                dir: RepoPathBuf::root(),
                ignores: base.clone(),
                rules: base.clone(),
                gitignores: None,
            },
        );
    });
    if let Some(err) = walk.error.into_inner().unwrap() {
        return Err(err);
    }
    // Chained parents first, a directory's path sorting before its descendants', so a deeper
    // file's rules win.
    let mut found = walk.found.into_inner().unwrap();
    found.sort_by(|a, b| a.dir.cmp(&b.dir));
    let mut all = base.clone();
    for file in &found {
        all = all.chain(&file.dir, &file.path, &file.contents)?;
    }
    Ok(all)
}

struct Walk<'a> {
    /// Every `.ignore` found.
    found: Mutex<Vec<IgnoreFile>>,
    /// The first error.
    error: Mutex<Option<SnapshotError>>,
    force_tracking: &'a dyn Matcher,
}

/// A directory to read, and the rules its entries are decided by.
struct Directory {
    disk_dir: PathBuf,
    dir: RepoPathBuf,
    /// The `.ignore` rules of its ancestors.
    ignores: Arc<GitIgnoreFile>,
    /// The rules jj decides its ancestors' entries by: those, then their `.gitignore` rules.
    rules: Arc<GitIgnoreFile>,
    /// Its ancestors' `.gitignore` files, the nearest first.
    gitignores: Option<Arc<GitIgnores>>,
}

/// A `.gitignore`, and those of its directory's ancestors.
struct GitIgnores {
    file: IgnoreFile,
    parent: Option<Arc<GitIgnores>>,
}

/// An ignore file's directory, path, and contents.
struct IgnoreFile {
    dir: RepoPathBuf,
    path: PathBuf,
    contents: Vec<u8>,
}

impl<'a> Walk<'a> {
    fn spawn<'scope>(&'scope self, scope: &rayon::Scope<'scope>, directory: Directory)
    where
        'a: 'scope,
    {
        scope.spawn(move |scope| {
            if let Err(err) = self.directory(scope, directory) {
                self.error.lock().unwrap().get_or_insert(err);
            }
        });
    }

    /// Reads a directory's ignore files, and spawns the reading of each subdirectory jj reads.
    fn directory<'scope>(
        &'scope self,
        scope: &rayon::Scope<'scope>,
        directory: Directory,
    ) -> Result<(), SnapshotError>
    where
        'a: 'scope,
    {
        let Directory {
            disk_dir,
            dir,
            mut ignores,
            mut rules,
            mut gitignores,
        } = directory;
        let entries: Vec<(fs::DirEntry, OsString)> = disk_dir
            .read_dir()
            .and_then(|entries| {
                entries
                    .map(|e| {
                        e.map(|e| {
                            let name = e.file_name();
                            (e, name)
                        })
                    })
                    .collect()
            })
            .map_err(|err| read_dir_error(&disk_dir, err))?;
        // jj skips another repository inside the working copy: a directory holding a .git or
        // a .jj of any kind.
        if !dir.is_root()
            && entries
                .iter()
                .any(|(_, name)| RESERVED_DIR_NAMES.iter().any(|r| name == *r))
        {
            return Ok(());
        }
        if let Some(file) = read_ignore_file(&entries, &dir, IGNORE_FILE)? {
            ignores = ignores.chain(&dir, &file.path, &file.contents)?;
            // jj consults every .ignore rule before any .gitignore rule.
            let mut ancestors = Vec::new();
            let mut next = gitignores.as_deref();
            while let Some(g) = next {
                ancestors.push(&g.file);
                next = g.parent.as_deref();
            }
            rules = ignores.clone();
            for g in ancestors.iter().rev() {
                rules = rules.chain(&g.dir, &g.path, &g.contents)?;
            }
            self.found.lock().unwrap().push(file);
        }
        if let Some(file) = read_ignore_file(&entries, &dir, GITIGNORE_FILE)? {
            rules = rules.chain(&dir, &file.path, &file.contents)?;
            gitignores = Some(Arc::new(GitIgnores {
                file,
                parent: gitignores,
            }));
        }
        for (entry, name) in &entries {
            let file_type = entry
                .file_type()
                .map_err(|err| read_dir_error(&disk_dir, err))?;
            if !file_type.is_dir() {
                continue;
            }
            // jj never tracks a name that is not UTF-8.
            let Some(name) = name.to_str() else {
                continue;
            };
            if RESERVED_DIR_NAMES.contains(&name) {
                continue;
            }
            let Ok(component) = RepoPathComponent::new(name) else {
                continue;
            };
            let path = dir.join(component);
            if rules.matches_dir(&path) && self.force_tracking.visit(&path).is_nothing() {
                continue;
            }
            self.spawn(
                scope,
                Directory {
                    disk_dir: entry.path(),
                    dir: path,
                    ignores: ignores.clone(),
                    rules: rules.clone(),
                    gitignores: gitignores.clone(),
                },
            );
        }
        Ok(())
    }
}

/// A directory's ignore file named `name`, if it has one: a file, or a link to one, as jj reads
/// its `.gitignore`.
fn read_ignore_file(
    entries: &[(fs::DirEntry, OsString)],
    dir: &RepoPath,
    name: &str,
) -> Result<Option<IgnoreFile>, GitIgnoreError> {
    let Some((entry, _)) = entries.iter().find(|(_, n)| n == name) else {
        return Ok(None);
    };
    let path = entry.path();
    let is_file = match entry.file_type() {
        Ok(t) if t.is_symlink() => path.is_file(),
        Ok(t) => t.is_file(),
        Err(_) => false,
    };
    if !is_file {
        return Ok(None);
    }
    let contents = fs::read(&path).map_err(|source| GitIgnoreError::ReadFile {
        path: path.clone(),
        source,
    })?;
    Ok(Some(IgnoreFile {
        dir: dir.to_owned(),
        path,
        contents,
    }))
}

fn read_dir_error(disk_dir: &Path, err: io::Error) -> SnapshotError {
    SnapshotError::Other {
        message: format!("Failed to read directory {}", disk_dir.display()),
        err: err.into(),
    }
}

#[cfg(test)]
mod tests;
