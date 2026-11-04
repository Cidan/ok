//! An ok workspace as git sees it (docs/git.md, "The model"): HEAD is the working-copy change's
//! parent, the working change is the index and the working tree at once, branches are local
//! bookmarks, `origin/main` is `main@ok`, and git's revision syntax and pathspecs resolve here.

use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt as _;
use futures::TryStreamExt as _;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::revset_util::RevsetExpressionEvaluator;
use jj_lib::backend::CommitId;
use jj_lib::backend::MergedTreeValue;
use jj_lib::backend::TreeValue;
use jj_lib::commit::Commit;
use jj_lib::copies::CopyRecords;
use jj_lib::matchers::Matcher;
use jj_lib::matchers::Visit;
use jj_lib::matchers::VisitDirs;
use jj_lib::matchers::VisitFiles;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::ref_name::RefName;
use jj_lib::ref_name::RemoteName;
use jj_lib::repo::Repo;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::revset::RevsetExpression;
use jj_lib::revset::UserRevsetExpression;

/// The remote git sees: `origin`, which is ok.
pub const ORIGIN: &str = "origin";
/// The branch every repository has, and every branch's upstream: `origin/main` is `main@ok`.
pub const MAIN: &str = "main";

/// Where HEAD is: on a branch, or detached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Head {
    Branch(String),
    Detached,
}

impl Head {
    pub fn branch(&self) -> Option<&str> {
        match self {
            Head::Branch(b) => Some(b),
            Head::Detached => None,
        }
    }
}

/// What a workspace remembers of git's HEAD, beside its `.jj`: the branch it is on (or the commit
/// it is detached at), and the one before, for `git checkout -`. It only breaks ties — which of
/// several branches at HEAD is current — and is rebuilt from the bookmarks when it is missing or
/// stale, so a workspace attached on another machine still has a HEAD.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeadHint {
    pub head: Option<HintRef>,
    pub previous: Option<HintRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HintRef {
    Branch(String),
    Commit(String),
}

impl HintRef {
    fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if let Some(b) = s.strip_prefix("ref: refs/heads/") {
            Some(HintRef::Branch(b.to_owned()))
        } else if !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit()) {
            Some(HintRef::Commit(s.to_owned()))
        } else {
            None
        }
    }

    fn render(&self) -> String {
        match self {
            HintRef::Branch(b) => format!("ref: refs/heads/{b}"),
            HintRef::Commit(c) => c.clone(),
        }
    }
}

/// The file the hint is kept in.
fn hint_path(root: &Path) -> PathBuf {
    root.join(".jj").join("git-HEAD")
}

pub fn read_hint(root: &Path) -> HeadHint {
    let Ok(text) = std::fs::read_to_string(hint_path(root)) else {
        return HeadHint::default();
    };
    let mut lines = text.lines();
    HeadHint {
        head: lines.next().and_then(HintRef::parse),
        previous: lines.next().and_then(HintRef::parse),
    }
}

/// Records that HEAD is now `head`, keeping where it was as the previous.
pub fn write_hint(root: &Path, old: &Head, old_head: &CommitId, new: &Head, new_head: &CommitId) {
    let as_ref = |h: &Head, id: &CommitId| match h {
        Head::Branch(b) => HintRef::Branch(b.clone()),
        Head::Detached => HintRef::Commit(id.hex()),
    };
    let head = as_ref(new, new_head);
    let before = as_ref(old, old_head);
    let mut previous = read_hint(root).previous;
    if before != head {
        previous = Some(before);
    }
    let mut text = head.render();
    text.push('\n');
    if let Some(p) = previous {
        text.push_str(&p.render());
        text.push('\n');
    }
    // The hint is a convenience: a workspace whose `.jj` cannot take it rebuilds HEAD from its
    // bookmarks.
    let _ = std::fs::write(hint_path(root), text);
}

/// `main@ok`, git's `origin/main`.
pub fn origin_main(repo: &dyn Repo) -> Option<CommitId> {
    ok_cl::main_target(repo)
}

/// The commit a local bookmark names, when it names one.
pub fn branch_target(repo: &dyn Repo, name: &str) -> Option<CommitId> {
    repo.view()
        .get_local_bookmark(RefName::new(name))
        .as_normal()
        .cloned()
}

/// Local bookmarks: git's branches, by name, with the commit each names (a conflicted bookmark
/// names none).
pub fn branches(repo: &dyn Repo) -> Vec<(String, Option<CommitId>)> {
    repo.view()
        .local_bookmarks()
        .map(|(n, t)| (n.as_str().to_owned(), t.as_normal().cloned()))
        .collect()
}

/// Local tags, with the commit each names.
pub fn tags(repo: &dyn Repo) -> Vec<(String, Option<CommitId>)> {
    repo.view()
        .local_tags()
        .map(|(n, t)| (n.as_str().to_owned(), t.as_normal().cloned()))
        .collect()
}

/// ok's remote bookmarks — `main` and each `cl/<n>` brought in — as git's remote branches.
pub fn remote_branches(repo: &dyn Repo) -> Vec<(String, CommitId)> {
    repo.view()
        .remote_bookmarks(RemoteName::new(ok_cl::REMOTE))
        .filter_map(|(n, r)| Some((n.as_str().to_owned(), r.target.as_normal()?.clone())))
        .collect()
}

/// The working-copy commit and HEAD (its first parent) of a workspace.
pub struct State {
    pub wc: Commit,
    pub head: Commit,
    /// Whether HEAD is the root commit: git's unborn branch.
    pub unborn: bool,
}

pub async fn state(ws: &WorkspaceCommandHelper) -> Result<State, CommandError> {
    let repo = ws.repo();
    let Some(wc_id) = ws.get_wc_commit_id() else {
        return Err(jj_cli::command_error::user_error(
            "this workspace has no working copy",
        ));
    };
    let wc = repo.store().get_commit_async(wc_id).await?;
    let head_id = wc
        .parent_ids()
        .first()
        .cloned()
        .unwrap_or_else(|| repo.store().root_commit_id().clone());
    let head = repo.store().get_commit_async(&head_id).await?;
    let unborn = head.id() == repo.store().root_commit_id();
    Ok(State { wc, head, unborn })
}

/// The branch HEAD is on: the one the hint names if it is at HEAD; else the one bookmark at HEAD
/// (the first by name, of several); else `main` while there is no local `main` and HEAD is
/// `origin/main`; else none — detached.
pub fn current(repo: &dyn Repo, root: &Path, head: &CommitId) -> Head {
    let hint = read_hint(root);
    let at_head: BTreeSet<String> = repo
        .view()
        .local_bookmarks_for_commit(head)
        .map(|(n, _)| n.as_str().to_owned())
        .collect();
    let virtual_main = branch_target(repo, MAIN).is_none()
        && origin_main(repo).unwrap_or_else(|| repo.store().root_commit_id().clone()) == *head;
    match &hint.head {
        Some(HintRef::Branch(b)) if at_head.contains(b) => return Head::Branch(b.clone()),
        Some(HintRef::Branch(b)) if b == MAIN && virtual_main => return Head::Branch(b.clone()),
        Some(HintRef::Commit(c)) if *c == head.hex() => return Head::Detached,
        _ => {}
    }
    if let Some(b) = at_head.iter().next() {
        return Head::Branch(b.clone());
    }
    if virtual_main {
        return Head::Branch(MAIN.to_owned());
    }
    Head::Detached
}

/// The previous place HEAD was, for `git checkout -` and `@{-1}`.
pub fn previous(root: &Path) -> Option<HintRef> {
    read_hint(root).previous
}

/// A commit ID as git abbreviates it: at least seven digits, more where seven are ambiguous.
pub async fn short(repo: &dyn Repo, id: &CommitId) -> Result<String, CommandError> {
    let len = repo
        .index()
        .shortest_unique_commit_id_prefix_len(id)
        .await?
        .max(7);
    let hex = id.hex();
    Ok(hex[..len.min(hex.len())].to_owned())
}

/// A short form of any object ID, as a hash in a diff's `index` line: seven digits.
pub fn short_hex(hex: &str) -> &str {
    &hex[..7.min(hex.len())]
}

/// What git names: a commit, a directory's tree, or a file, at a commit.
#[derive(Clone)]
pub enum Object {
    Commit(Commit),
    /// A directory: `path` in the commit's (or the working copy's) `root` tree.
    Tree {
        path: RepoPathBuf,
        root: MergedTree,
    },
    File {
        path: RepoPathBuf,
        value: MergedTreeValue,
    },
}

/// Why a revision did not resolve.
#[derive(Debug)]
pub enum RevError {
    /// Nothing by that name: git's "unknown revision".
    Unknown,
    /// HEAD with no commit yet.
    Unborn,
    /// A form git has and ok does not (reflogs, `ORIG_HEAD`, …).
    Unsupported(String),
    /// A path that is not in the commit named.
    NoPath {
        path: String,
        rev: String,
    },
    Command(CommandError),
}

impl From<CommandError> for RevError {
    fn from(e: CommandError) -> Self {
        RevError::Command(e)
    }
}

impl From<jj_lib::backend::BackendError> for RevError {
    fn from(e: jj_lib::backend::BackendError) -> Self {
        RevError::Command(e.into())
    }
}

impl RevError {
    /// The message git gives, without its `fatal: `.
    pub fn message(&self, spec: &str) -> Option<String> {
        match self {
            RevError::Unknown => Some(format!(
                "ambiguous argument '{spec}': unknown revision or path not in the working tree."
            )),
            RevError::Unborn => Some(format!(
                "ambiguous argument '{spec}': unknown revision or path not in the working tree."
            )),
            RevError::Unsupported(what) => Some(format!("{what} is not something ok keeps")),
            RevError::NoPath { path, rev } => {
                Some(format!("path '{path}' does not exist in '{rev}'"))
            }
            RevError::Command(_) => None,
        }
    }
}

/// Resolves git's revisions against a repository: the workspace's at its operation, or a
/// transaction's.
pub struct Resolver<'a> {
    pub ws: &'a WorkspaceCommandHelper,
    pub repo: &'a dyn Repo,
    pub extensions: Arc<jj_lib::revset::RevsetExtensions>,
    /// The workspace's working-copy commit in `repo`.
    pub wc: Commit,
}

impl<'a> Resolver<'a> {
    pub async fn new(
        command: &jj_cli::cli_util::CommandHelper,
        ws: &'a WorkspaceCommandHelper,
    ) -> Result<Self, CommandError> {
        let st = state(ws).await?;
        Ok(Self {
            ws,
            repo: ws.repo().as_ref(),
            extensions: command.revset_extensions().clone(),
            wc: st.wc,
        })
    }

    async fn commit(&self, id: &CommitId) -> Result<Commit, RevError> {
        Ok(self.repo.store().get_commit_async(id).await?)
    }

    async fn head(&self) -> Result<Commit, RevError> {
        let id = self
            .wc
            .parent_ids()
            .first()
            .cloned()
            .unwrap_or_else(|| self.repo.store().root_commit_id().clone());
        if id == *self.repo.store().root_commit_id() {
            return Err(RevError::Unborn);
        }
        self.commit(&id).await
    }

    fn origin(&self, name: &str) -> Option<CommitId> {
        self.repo
            .view()
            .get_remote_bookmark(
                RefName::new(name).to_remote_symbol(RemoteName::new(ok_cl::REMOTE)),
            )
            .target
            .as_normal()
            .cloned()
    }

    async fn upstream(&self) -> Result<Commit, RevError> {
        match self.origin(MAIN) {
            Some(id) if id != *self.repo.store().root_commit_id() => self.commit(&id).await,
            _ => Err(RevError::Unknown),
        }
    }

    /// A symbol jj resolves: a commit or change ID prefix, a bookmark, a tag, `cl/<n>@ok`.
    async fn symbol(&self, name: &str) -> Result<Commit, RevError> {
        let expression: Arc<UserRevsetExpression> = RevsetExpression::symbol(name.to_owned());
        let evaluator = RevsetExpressionEvaluator::new(
            self.repo,
            self.extensions.clone(),
            self.ws.id_prefix_context(),
            expression,
        );
        let Ok(ids) = evaluator.evaluate_to_commit_ids() else {
            return Err(RevError::Unknown);
        };
        let ids: Vec<CommitId> = ids.try_collect().await.map_err(|_| RevError::Unknown)?;
        match ids.as_slice() {
            [id] => self.commit(id).await,
            _ => Err(RevError::Unknown),
        }
    }

    /// The commit a ref-like name names, as git looks names up: full refs, `origin/…`, branches,
    /// tags, then what jj resolves.
    async fn named(&self, name: &str) -> Result<Commit, RevError> {
        let root = self.repo.store().root_commit_id().clone();
        let found = |id: Option<CommitId>| id.filter(|id| *id != root);
        match name {
            "HEAD" | "@" => return self.head().await,
            "FETCH_HEAD" => return self.upstream().await,
            "ORIG_HEAD" | "MERGE_HEAD" | "CHERRY_PICK_HEAD" | "REVERT_HEAD" | "REBASE_HEAD"
            | "AUTO_MERGE" => return Err(RevError::Unsupported(name.to_owned())),
            _ => {}
        }
        if let Some(b) = name
            .strip_prefix("refs/heads/")
            .or_else(|| name.strip_prefix("heads/"))
        {
            return match found(branch_target(self.repo, b)) {
                Some(id) => self.commit(&id).await,
                None if b == MAIN => self.upstream().await,
                None => Err(RevError::Unknown),
            };
        }
        if let Some(t) = name
            .strip_prefix("refs/tags/")
            .or_else(|| name.strip_prefix("tags/"))
        {
            return match self
                .repo
                .view()
                .get_local_tag(RefName::new(t))
                .as_normal()
                .cloned()
            {
                Some(id) => self.commit(&id).await,
                None => Err(RevError::Unknown),
            };
        }
        let remote = name
            .strip_prefix("refs/remotes/")
            .or_else(|| name.strip_prefix("remotes/"))
            .unwrap_or(name);
        if remote == ORIGIN || remote == format!("{ORIGIN}/HEAD") {
            return self.upstream().await;
        }
        if let Some(b) = remote.strip_prefix(&format!("{ORIGIN}/")) {
            return match found(self.origin(b)) {
                Some(id) => self.commit(&id).await,
                None => Err(RevError::Unknown),
            };
        }
        if let Some(id) = found(branch_target(self.repo, name)) {
            return self.commit(&id).await;
        }
        if let Some(id) = self
            .repo
            .view()
            .get_local_tag(RefName::new(name))
            .as_normal()
            .cloned()
        {
            return self.commit(&id).await;
        }
        if name == MAIN {
            return self.upstream().await;
        }
        if name.is_empty() {
            return Err(RevError::Unknown);
        }
        self.symbol(name).await
    }

    /// Resolves a git revision to a commit: `<name>`, then `~<n>`, `^<n>`, `^{commit}`, `^{}`,
    /// and `@{upstream}` (`@{u}`, `@{push}`), `@{-1}`.
    pub async fn commit_of(&self, spec: &str) -> Result<Commit, RevError> {
        match self.object(spec).await? {
            Object::Commit(c) => Ok(c),
            _ => Err(RevError::Unknown),
        }
    }

    /// Resolves any git object name: a revision, `<rev>:<path>`, `:<path>` (the index, which is
    /// the working tree), and `<rev>^{tree}`.
    pub async fn object(&self, spec: &str) -> Result<Object, RevError> {
        if let Some((rev, path)) = split_rev_path(spec) {
            let tree = if rev.is_empty() {
                self.wc.tree()
            } else {
                Box::pin(self.commit_of(rev)).await?.tree()
            };
            let path = path.strip_prefix("./").unwrap_or(path);
            let repo_path = if path.is_empty() {
                RepoPathBuf::root()
            } else {
                RepoPathBuf::from_internal_string(path.trim_end_matches('/'))
                    .map_err(|_| RevError::Unknown)?
            };
            return self.at_path(tree, repo_path, rev).await;
        }
        let (base, mut rest) = split_base(spec);
        let mut commit = if base.is_empty() {
            if rest.starts_with("@{") {
                self.head().await?
            } else {
                return Err(RevError::Unknown);
            }
        } else if base.starts_with("@{-") {
            return Err(RevError::Unsupported(base.to_owned()));
        } else {
            self.named(base).await?
        };
        while !rest.is_empty() {
            if let Some(r) = rest.strip_prefix("@{") {
                let Some(end) = r.find('}') else {
                    return Err(RevError::Unknown);
                };
                match &r[..end] {
                    "u" | "upstream" | "push" => commit = self.upstream().await?,
                    other => return Err(RevError::Unsupported(format!("@{{{other}}}"))),
                }
                rest = &r[end + 1..];
            } else if let Some(r) = rest.strip_prefix("^{") {
                let Some(end) = r.find('}') else {
                    return Err(RevError::Unknown);
                };
                match &r[..end] {
                    "" | "commit" => {}
                    "tree" => {
                        if !r[end + 1..].is_empty() {
                            return Err(RevError::Unknown);
                        }
                        return Ok(Object::Tree {
                            path: RepoPathBuf::root(),
                            root: commit.tree(),
                        });
                    }
                    other => return Err(RevError::Unsupported(format!("^{{{other}}}"))),
                }
                rest = &r[end + 1..];
            } else if let Some(r) = rest.strip_prefix('~') {
                let digits = r.bytes().take_while(u8::is_ascii_digit).count();
                let n: usize = if digits == 0 {
                    1
                } else {
                    r[..digits].parse().map_err(|_| RevError::Unknown)?
                };
                for _ in 0..n {
                    let Some(p) = commit.parent_ids().first().cloned() else {
                        return Err(RevError::Unknown);
                    };
                    if p == *self.repo.store().root_commit_id() {
                        return Err(RevError::Unknown);
                    }
                    commit = self.commit(&p).await?;
                }
                rest = &r[digits..];
            } else if let Some(r) = rest.strip_prefix('^') {
                let digits = r.bytes().take_while(u8::is_ascii_digit).count();
                let n: usize = if digits == 0 {
                    1
                } else {
                    r[..digits].parse().map_err(|_| RevError::Unknown)?
                };
                if n > 0 {
                    let Some(p) = commit.parent_ids().get(n - 1).cloned() else {
                        return Err(RevError::Unknown);
                    };
                    if p == *self.repo.store().root_commit_id() {
                        return Err(RevError::Unknown);
                    }
                    commit = self.commit(&p).await?;
                }
                rest = &r[digits..];
            } else {
                return Err(RevError::Unknown);
            }
        }
        Ok(Object::Commit(commit))
    }

    async fn at_path(
        &self,
        tree: MergedTree,
        path: RepoPathBuf,
        rev: &str,
    ) -> Result<Object, RevError> {
        if path.is_root() {
            return Ok(Object::Tree { path, root: tree });
        }
        let value = tree.path_value(&path).await?;
        if value.is_absent() {
            return Err(RevError::NoPath {
                path: path.as_internal_file_string().to_owned(),
                rev: rev.to_owned(),
            });
        }
        if let Some(Some(TreeValue::Tree(_))) = value.as_resolved() {
            return Ok(Object::Tree { path, root: tree });
        }
        Ok(Object::File { path, value })
    }
}

/// `<rev>:<path>` split at its colon, unless it is a `:/<text>` search, which is not a path.
fn split_rev_path(spec: &str) -> Option<(&str, &str)> {
    if spec.starts_with(":/") {
        return None;
    }
    // A colon inside `@{…}` or `^{…}` is not the path's.
    let mut depth = 0;
    for (i, c) in spec.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            ':' if depth == 0 => return Some((&spec[..i], &spec[i + 1..])),
            _ => {}
        }
    }
    None
}

/// A revision's name, and what follows it (`~`, `^`, `@{…}`).
fn split_base(spec: &str) -> (&str, &str) {
    let mut i = 0;
    let b = spec.as_bytes();
    while i < b.len() {
        match b[i] {
            b'~' | b'^' => break,
            b'@' if b.get(i + 1) == Some(&b'{') && !spec[i..].starts_with("@{-") => break,
            _ => i += 1,
        }
    }
    (&spec[..i], &spec[i..])
}

/// A range or set of revisions as git's commands take them: `A..B`, `A...B`, `^A`, and plain
/// revisions, which are their ancestors.
pub struct Walk {
    pub include: Vec<CommitId>,
    pub exclude: Vec<CommitId>,
    /// `A...B`: the commits of either side, not both; its two sides.
    pub symmetric: Option<(CommitId, CommitId)>,
}

pub async fn walk(
    r: &Resolver<'_>,
    specs: &[String],
    default_head: bool,
) -> Result<Walk, (String, RevError)> {
    let mut w = Walk {
        include: Vec::new(),
        exclude: Vec::new(),
        symmetric: None,
    };
    let resolve = async |s: &str| -> Result<CommitId, (String, RevError)> {
        let s = if s.is_empty() { "HEAD" } else { s };
        r.commit_of(s)
            .await
            .map(|c| c.id().clone())
            .map_err(|e| (s.to_owned(), e))
    };
    for spec in specs {
        if let Some((a, b)) = spec.split_once("...") {
            let (a, b) = (resolve(a).await?, resolve(b).await?);
            w.include.push(a.clone());
            w.include.push(b.clone());
            w.symmetric = Some((a, b));
        } else if let Some((a, b)) = spec.split_once("..") {
            w.exclude.push(resolve(a).await?);
            w.include.push(resolve(b).await?);
        } else if let Some(a) = spec.strip_prefix('^') {
            w.exclude.push(resolve(a).await?);
        } else {
            w.include.push(resolve(spec).await?);
        }
    }
    if w.include.is_empty() && default_head {
        w.include.push(resolve("HEAD").await?);
    }
    Ok(w)
}

impl Walk {
    /// The commits of the walk, as a revset.
    pub fn expression(&self, repo: &dyn Repo) -> Arc<jj_lib::revset::ResolvedRevsetExpression> {
        let _ = repo;
        let heads = RevsetExpression::commits(self.include.clone());
        let mut set = heads.ancestors();
        if let Some((a, b)) = &self.symmetric {
            let both = RevsetExpression::commits(vec![a.clone()])
                .ancestors()
                .intersection(&RevsetExpression::commits(vec![b.clone()]).ancestors());
            set = set.minus(&both);
        }
        if !self.exclude.is_empty() {
            set = set.minus(&RevsetExpression::commits(self.exclude.clone()).ancestors());
        }
        set.minus(&RevsetExpression::root())
    }
}

/// Git's pathspecs, relative to the current directory: paths (a directory names everything
/// under it), wildcards (`*` crossing `/`, as in git), `:/` and `:(top)` from the root, and
/// `:!`, `:^`, `:(exclude)` to leave paths out.
#[derive(Debug)]
pub struct Pathspec {
    include: Vec<Spec>,
    exclude: Vec<Spec>,
    /// Whether nothing was given: everything matches.
    everything: bool,
}

#[derive(Debug)]
enum Spec {
    /// A path and everything under it.
    Prefix(RepoPathBuf),
    /// A wildcard pattern, against the whole repository path.
    Glob(globset::GlobMatcher),
}

impl Spec {
    fn matches(&self, path: &RepoPath) -> bool {
        match self {
            Spec::Prefix(p) => path.starts_with(p),
            Spec::Glob(g) => g.is_match(path.as_internal_file_string()),
        }
    }
}

/// A pathspec that does not parse, as git says it.
#[derive(Debug)]
pub struct BadPathspec(pub String);

impl Pathspec {
    /// Parses `specs`, relative to `cwd` within the workspace at `root`.
    pub fn parse(
        root: &Path,
        cwd: &Path,
        specs: &[String],
        literal: bool,
    ) -> Result<Self, BadPathspec> {
        let mut p = Self {
            include: Vec::new(),
            exclude: Vec::new(),
            everything: specs.is_empty(),
        };
        let prefix = cwd
            .strip_prefix(root)
            .map_err(|_| BadPathspec(format!("'{}' is outside the repository", cwd.display())))?
            .to_owned();
        for raw in specs {
            let (mut exclude, mut top, mut glob, mut lit, mut icase) =
                (false, false, false, literal, false);
            let mut s = raw.as_str();
            if !literal && let Some(rest) = s.strip_prefix(':') {
                if let Some(rest) = rest.strip_prefix('(') {
                    let Some(end) = rest.find(')') else {
                        return Err(BadPathspec(format!(
                            "Missing ')' at the end of pathspec magic in '{raw}'"
                        )));
                    };
                    for magic in rest[..end].split(',') {
                        match magic.trim() {
                            "exclude" => exclude = true,
                            "top" => top = true,
                            "glob" => glob = true,
                            "literal" => lit = true,
                            "icase" => icase = true,
                            "" => {}
                            other => {
                                return Err(BadPathspec(format!(
                                    "Invalid pathspec magic '{other}' in '{raw}'"
                                )));
                            }
                        }
                    }
                    s = &rest[end + 1..];
                } else {
                    let mut rest = rest;
                    loop {
                        match rest.chars().next() {
                            Some('!') | Some('^') => exclude = true,
                            Some('/') => top = true,
                            _ => break,
                        }
                        rest = &rest[1..];
                    }
                    s = rest.strip_prefix(':').unwrap_or(rest);
                }
            }
            let base = if top { PathBuf::new() } else { prefix.clone() };
            let joined = normalize(&base.join(s))
                .ok_or_else(|| BadPathspec(format!("{raw}: '{s}' is outside the repository")))?;
            let has_wild = !lit && s.contains(['*', '?', '[']);
            let spec = if has_wild {
                let pattern = joined.to_string_lossy().replace('\\', "/");
                let g = globset::GlobBuilder::new(&pattern)
                    .literal_separator(glob)
                    .case_insensitive(icase)
                    .backslash_escape(true)
                    .build()
                    .map_err(|e| BadPathspec(format!("{raw}: {e}")))?;
                Spec::Glob(g.compile_matcher())
            } else {
                let internal = joined.to_string_lossy().replace('\\', "/");
                let path = if internal.is_empty() {
                    RepoPathBuf::root()
                } else {
                    RepoPathBuf::from_internal_string(internal)
                        .map_err(|e| BadPathspec(format!("{raw}: {e}")))?
                };
                Spec::Prefix(path)
            };
            if exclude {
                p.exclude.push(spec);
            } else {
                p.include.push(spec);
            }
        }
        if p.include.is_empty() && !p.exclude.is_empty() {
            // Only exclusions: everything else, from the current directory.
            p.include
                .push(Spec::Prefix(if prefix.as_os_str().is_empty() {
                    RepoPathBuf::root()
                } else {
                    RepoPathBuf::from_internal_string(prefix.to_string_lossy().replace('\\', "/"))
                        .map_err(|e| BadPathspec(e.to_string()))?
                }));
        }
        Ok(p)
    }

    pub fn is_everything(&self) -> bool {
        self.everything
    }

    /// The literal paths given (not wildcards), to tell which matched nothing.
    pub fn literal_paths(&self) -> Vec<RepoPathBuf> {
        self.include
            .iter()
            .filter_map(|s| match s {
                Spec::Prefix(p) => Some(p.clone()),
                Spec::Glob(_) => None,
            })
            .collect()
    }
}

/// `p` with `.` and `..` resolved, or `None` if it leaves the root.
fn normalize(p: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !out.pop() {
                    return None;
                }
            }
            std::path::Component::Normal(n) => out.push(n),
            _ => return None,
        }
    }
    Some(out)
}

impl Matcher for Pathspec {
    fn matches(&self, file: &RepoPath) -> bool {
        if self.everything {
            return true;
        }
        self.include.iter().any(|s| s.matches(file))
            && !self.exclude.iter().any(|s| s.matches(file))
    }

    fn visit(&self, dir: &RepoPath) -> Visit {
        if self.everything {
            return Visit::AllRecursively;
        }
        if self
            .exclude
            .iter()
            .any(|s| matches!(s, Spec::Prefix(p) if dir.starts_with(p)))
        {
            return Visit::Nothing;
        }
        let mut any = false;
        for s in &self.include {
            match s {
                Spec::Prefix(p) => {
                    if dir.starts_with(p) {
                        if self.exclude.is_empty() {
                            return Visit::AllRecursively;
                        }
                        any = true;
                    } else if p.starts_with(dir) {
                        any = true;
                    }
                }
                Spec::Glob(_) => any = true,
            }
        }
        if any {
            Visit::Specific {
                dirs: VisitDirs::All,
                files: VisitFiles::All,
            }
        } else {
            Visit::Nothing
        }
    }
}

/// The copy records between two commits, for the renames a diff shows.
pub async fn copy_records(
    repo: &dyn Repo,
    from: &[CommitId],
    to: &CommitId,
    matcher: &dyn Matcher,
) -> Result<CopyRecords, CommandError> {
    let mut records = CopyRecords::default();
    for f in from {
        let found = jj_cli::diff_util::get_copy_records(repo.store(), f, to, matcher).await?;
        records.add_records(found);
    }
    Ok(records)
}

/// Every file of a tree under a matcher, with its value, in path order.
pub async fn files(
    tree: &MergedTree,
    matcher: &dyn Matcher,
) -> Result<Vec<(RepoPathBuf, MergedTreeValue)>, CommandError> {
    let mut out = Vec::new();
    for (path, value) in tree.entries_matching(matcher) {
        out.push((path, value?));
    }
    Ok(out)
}

/// Whether a commit changes anything a matcher names, against its parents.
pub async fn touches(
    repo: &dyn Repo,
    commit: &Commit,
    matcher: &dyn Matcher,
) -> Result<bool, CommandError> {
    let parent = commit.parent_tree(repo).await?;
    let mut diff = parent.diff_stream(&commit.tree(), matcher);
    Ok(diff.next().await.is_some())
}

/// Whether `ancestor` is `descendant` or one of its ancestors.
pub async fn is_ancestor(
    repo: &dyn Repo,
    ancestor: &CommitId,
    descendant: &CommitId,
) -> Result<bool, CommandError> {
    Ok(repo.index().is_ancestor(ancestor, descendant).await?)
}

/// How many commits are in `from..to`.
pub async fn count_range(
    repo: &dyn Repo,
    from: &CommitId,
    to: &CommitId,
) -> Result<usize, CommandError> {
    let expression = RevsetExpression::commits(vec![from.clone()])
        .range(&RevsetExpression::commits(vec![to.clone()]));
    let ids: Vec<CommitId> = expression
        .evaluate(repo)
        .map_err(jj_cli::command_error::internal_error)?
        .stream()
        .try_collect()
        .await
        .map_err(jj_cli::command_error::internal_error)?;
    Ok(ids.len())
}
