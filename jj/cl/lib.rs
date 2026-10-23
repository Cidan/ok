//! ok's ChangeList workflow over jj-lib (docs/scm.md, "Commands"): what `jj cl`
//! uploads and refuses, `jj cl get`, and `jj sync`. The `jj` binary's commands are the
//! command line over these; tests drive them through jj-lib directly.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use futures::TryStreamExt as _;
use jj_lib::backend::BackendError;
use jj_lib::backend::ChangeId;
use jj_lib::backend::CommitId;
use jj_lib::commit::Commit;
use jj_lib::index::IndexError;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RefTarget;
use jj_lib::op_store::RemoteRef;
use jj_lib::op_store::RemoteRefState;
use jj_lib::ref_name::RefName;
use jj_lib::ref_name::RemoteName;
use jj_lib::repo::MutableRepo;
use jj_lib::repo::Repo;
use jj_lib::revset::ResolvedRevsetExpression;
use jj_lib::revset::RevsetEvaluationError;
use jj_lib::revset::RevsetExpression;
use jj_lib::revset::RevsetResolutionError;
use jj_lib::revset::RevsetStreamExt as _;
use jj_lib::rewrite::EmptyBehavior;
use jj_lib::rewrite::RebaseOptions;
use jj_lib::rewrite::RebasedCommit;
use jj_lib::rewrite::RewriteRefsOptions;
use jj_lib::rewrite::merge_commit_trees;
use jj_lib::rewrite::rebase_commit_with_options;
use ok_store::OkBackend;
use ok_store::Shared;
use ok_store::changelists;
use ok_store::pb;
use tonic::Status;

/// The remote ok's bookmarks live under: `main@ok` is `trunk()`, and `cl/<n>@ok` is
/// someone else's CL brought in with `jj cl get`.
pub const REMOTE: &str = "ok";

/// The bookmark `jj cl get <n>` sets on the `ok` remote.
pub fn cl_bookmark(number: i64) -> String {
    format!("cl/{number}")
}

#[derive(Debug, thiserror::Error)]
pub enum ClError {
    #[error("this repository is not on ok (it was not made by jj clone or jj create)")]
    NotOk,
    #[error(transparent)]
    Scm(#[from] Status),
    #[error(transparent)]
    Publish(#[from] ok_store::PublishError),
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    Revset(#[from] RevsetEvaluationError),
    #[error(transparent)]
    Resolution(#[from] RevsetResolutionError),
    #[error("the SCM returned {0}")]
    Malformed(&'static str),
    #[error(transparent)]
    Edit(#[from] jj_lib::repo::EditCommitError),
    #[error("CL {number} has landed on main as {commit}")]
    Landed { number: i64, commit: CommitId },
    #[error("CL {number}'s change is already in your view, as {commit}")]
    AlreadyInView { number: i64, commit: CommitId },
}

/// A repository's ok state, from its backend.
pub fn shared(repo: &dyn Repo) -> Result<Arc<Shared>, ClError> {
    repo.store()
        .backend_impl::<OkBackend>()
        .map(|b| b.shared().clone())
        .ok_or(ClError::NotOk)
}

fn commit_id(bytes: &[u8]) -> Result<CommitId, ClError> {
    if bytes.len() != ok_encoding::ID_LEN {
        return Err(ClError::Malformed("a commit ID that is not 32 bytes"));
    }
    Ok(CommitId::from_bytes(bytes))
}

fn remote_symbol(name: &str) -> jj_lib::ref_name::RemoteRefSymbol<'_> {
    RefName::new(name).to_remote_symbol(RemoteName::new(REMOTE))
}

/// `main@ok`, if the view has it.
pub fn main_target(repo: &dyn Repo) -> Option<CommitId> {
    repo.view()
        .get_remote_bookmark(remote_symbol("main"))
        .target
        .as_normal()
        .cloned()
}

/// Why a change cannot be uploaded (docs/scm.md, "Commands": `jj cl`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotUploadable {
    Merge,
    Conflicted,
    NoDescription,
    Empty,
}

/// The first commit, bottom first, that `jj cl` refuses before asking the SCM, and why.
pub async fn check_uploadable(
    repo: &dyn Repo,
    commits: &[Commit],
) -> Result<Option<(Commit, NotUploadable)>, ClError> {
    for c in commits {
        let why = if c.parent_ids().len() != 1 {
            Some(NotUploadable::Merge)
        } else if c.has_conflict() {
            Some(NotUploadable::Conflicted)
        } else if c.description().trim().is_empty() {
            Some(NotUploadable::NoDescription)
        } else if c.is_empty(repo).await? {
            Some(NotUploadable::Empty)
        } else {
            None
        };
        if let Some(why) = why {
            return Ok(Some((c.clone(), why)));
        }
    }
    Ok(None)
}

/// Uploads commits, bottom of the stack first: publishes anything still staged, so every
/// commit is on the SCM, then asks it to create and update their CLs.
pub async fn upload(
    repo: &dyn Repo,
    commits: &[Commit],
    readiness: pb::upload_request::Readiness,
    reviewers: Vec<String>,
) -> Result<pb::UploadResponse, ClError> {
    let shared = shared(repo)?;
    changelists::flush(&shared).await?;
    let ids: Vec<CommitId> = commits.iter().map(|c| c.id().clone()).collect();
    Ok(changelists::upload(&shared, &ids, readiness, reviewers).await?)
}

/// What `jj cl get` brought in.
#[derive(Debug)]
pub struct Got {
    pub change_list: pb::ChangeList,
    pub commit: Commit,
}

/// `jj cl get <n>`: brings CL n's current patchset into the view as the untracked remote
/// bookmark `cl/<n>@ok`, which jj treats as immutable. A landed CL is on main already,
/// and a CL whose change is in the view as anything but that bookmark (your own CL) would
/// only turn divergent. Getting a CL already there moves the bookmark to the current
/// patchset, and what is built on it follows, as `jj sync` does.
pub async fn get(
    repo: &mut MutableRepo,
    number: i64,
    immutable: &Immutable<'_>,
) -> Result<Got, ClError> {
    let shared = shared(repo)?;
    let cl = changelists::get_change_list(&shared, number).await?;
    if cl.status == pb::ChangeListStatus::Submitted as i32 {
        return Err(ClError::Landed {
            number,
            commit: commit_id(&cl.landed_commit)?,
        });
    }
    let current = cl
        .patchsets
        .get((cl.current_patchset as usize).wrapping_sub(1))
        .ok_or(ClError::Malformed("a CL without its current patchset"))?;
    let target = commit_id(&current.commit_id)?;
    if cl.change_id.len() != ok_encoding::CHANGE_ID_LEN {
        return Err(ClError::Malformed("a change ID that is not 16 bytes"));
    }
    let bookmark = cl_bookmark(number);
    let old = repo
        .view()
        .get_remote_bookmark(remote_symbol(&bookmark))
        .target
        .as_normal()
        .cloned();
    let change = ChangeId::new(cl.change_id.clone());
    if let Some(found) = repo.resolve_change_id(&change).await?
        && let Some((_, id)) = found
            .visible_with_offsets()
            .find(|(_, id)| Some(*id) != old.as_ref())
    {
        return Err(ClError::AlreadyInView {
            number,
            commit: id.clone(),
        });
    }
    let have: Vec<CommitId> = main_target(repo).into_iter().collect();
    changelists::prefetch(&shared, std::slice::from_ref(&target), &have).await?;
    let commit = repo.store().get_commit_async(&target).await?;
    repo.add_head(&commit).await?;
    repo.set_remote_bookmark(
        remote_symbol(&bookmark),
        RemoteRef {
            target: RefTarget::normal(target.clone()),
            state: RemoteRefState::New,
        },
    );
    if let Some(old) = old.filter(|old| *old != target) {
        repo.set_rewritten_commit(old, target);
        let imm = immutable(repo)?;
        repo.rebase_descendants_with_options(&imm, &rebase_options(), |_, _| {})
            .await?;
    }
    Ok(Got {
        change_list: cl,
        commit,
    })
}

/// A local change whose CL landed, and what replaced it.
#[derive(Debug)]
pub struct Retired {
    pub number: i64,
    pub local: Commit,
    pub landed: CommitId,
    /// The new change carrying edits made after the landed patchset, when there were any.
    pub kept: Option<Commit>,
}

/// What `jj sync` did to a `cl/<n>@ok` bookmark.
#[derive(Debug)]
pub enum Followed {
    /// Moved to the CL's new current patchset.
    Moved { number: i64, patchset: i32 },
    /// Removed: the CL landed as this commit.
    Landed { number: i64, commit: CommitId },
}

/// What `jj sync` did.
#[derive(Debug, Default)]
pub struct SyncReport {
    /// main before and after.
    pub old_main: Option<CommitId>,
    pub main: Option<CommitId>,
    /// Commits of main newly fetched.
    pub fetched: usize,
    pub retired: Vec<Retired>,
    pub followed: Vec<Followed>,
    /// Changes rebased (onto main, a landed commit, or a moved CL).
    pub rebased: usize,
    /// Changes the rebase emptied, and so abandoned.
    pub emptied: usize,
    /// Local changes whose CL is abandoned, left alone (the author may restore the CL).
    pub abandoned: Vec<(i64, Commit)>,
}

/// Resolves the user's immutable set (`::immutable_heads()`) against a repository state.
pub type Immutable<'a> = dyn Fn(&dyn Repo) -> Result<Arc<ResolvedRevsetExpression>, ClError> + 'a;

fn rebase_options() -> RebaseOptions {
    RebaseOptions {
        empty: EmptyBehavior::AbandonNewlyEmpty,
        ..Default::default()
    }
}

/// Brings the repository, a fork, its upstream's newer work on ok (`jj sync --upstream`): true
/// when its main moved to the upstream's.
pub async fn sync_fork(repo: &dyn Repo) -> Result<bool, ClError> {
    Ok(changelists::sync_fork(&shared(repo)?).await?)
}

/// `jj sync` (docs/scm.md, "Commands"), in the caller's transaction:
///
/// 1. `main@ok` moves to the SCM's head, with main's new commits fetched.
/// 2. The CLs of every mutable change and every `cl/<n>@ok` are looked up.
/// 3. A local change whose CL landed is retired onto the landed commit when it adds nothing
///    beyond what landed; otherwise a new change carrying its later edits replaces it.
/// 4. Each `cl/<n>@ok` moves to its CL's current patchset, or goes once the CL lands, and
///    what was stacked on it follows.
/// 5. Every remaining root of the mutable changes built on main's history is rebased onto
///    main's head with jj's own merge; changes the rebase empties are abandoned. A working
///    copy left on main's history (a change that landed as itself) gets a new change on
///    main's head.
/// 6. Changes whose CL is abandoned are reported and left alone.
pub async fn sync(
    repo: &mut MutableRepo,
    immutable: &Immutable<'_>,
) -> Result<SyncReport, ClError> {
    let shared = shared(repo)?;
    let store = repo.store().clone();
    let mut report = SyncReport::default();

    // 1. main.
    let remote = changelists::repository(&shared).await?;
    let head = remote.head.clone();
    report.old_main = main_target(repo);
    let have: Vec<CommitId> = report.old_main.iter().cloned().collect();
    report.fetched = changelists::prefetch(&shared, std::slice::from_ref(&head), &have).await?;
    if head != *store.root_commit_id() {
        let commit = store.get_commit_async(&head).await?;
        repo.add_head(&commit).await?;
    }
    repo.set_remote_bookmark(
        remote_symbol("main"),
        RemoteRef {
            target: RefTarget::normal(head.clone()),
            state: RemoteRefState::New,
        },
    );
    report.main = Some(head.clone());

    // 2. The CLs of the mutable changes, and of the cl/<n>@ok bookmarks.
    let imm = immutable(repo)?;
    let mutable: Vec<Commit> = RevsetExpression::all()
        .minus(&imm)
        .evaluate(repo)?
        .stream()
        .commits(&store)
        .try_collect()
        .await?;
    let mut changes: Vec<ChangeId> = mutable.iter().map(|c| c.change_id().clone()).collect();
    changes.sort();
    changes.dedup();
    let by_change: HashMap<Vec<u8>, pb::ChangeList> =
        changelists::lookup_changes(&shared, &changes)
            .await?
            .into_iter()
            .map(|cl| (cl.change_id.clone(), cl))
            .collect();
    let bookmarks: Vec<(i64, CommitId)> = repo
        .view()
        .remote_bookmarks(RemoteName::new(REMOTE))
        .filter_map(|(name, r)| {
            let n = name.as_str().strip_prefix("cl/")?.parse().ok()?;
            Some((n, r.target.as_normal()?.clone()))
        })
        .collect();

    // 3. Local changes whose CL landed.
    for x in &mutable {
        let Some(cl) = by_change.get(x.change_id().as_bytes()) else {
            continue;
        };
        if cl.status == pb::ChangeListStatus::Abandoned as i32 {
            report.abandoned.push((cl.number, x.clone()));
            continue;
        }
        if cl.status != pb::ChangeListStatus::Submitted as i32 {
            continue;
        }
        let landed_id = commit_id(&cl.landed_commit)?;
        if *x.id() == landed_id {
            continue; // it landed as itself and is on main now
        }
        let landed = store.get_commit_async(&landed_id).await?;
        let kept = retire(repo, x, cl, &landed).await?;
        report.retired.push(Retired {
            number: cl.number,
            local: x.clone(),
            landed: landed_id,
            kept,
        });
    }

    // 4. Other people's CLs.
    for (number, old) in bookmarks {
        let cl = changelists::get_change_list(&shared, number).await?;
        let (new, followed) = if cl.status == pb::ChangeListStatus::Submitted as i32 {
            let landed = commit_id(&cl.landed_commit)?;
            repo.set_remote_bookmark(remote_symbol(&cl_bookmark(number)), RemoteRef::absent());
            (
                landed.clone(),
                Followed::Landed {
                    number,
                    commit: landed,
                },
            )
        } else {
            let current = cl
                .patchsets
                .get((cl.current_patchset as usize).wrapping_sub(1))
                .ok_or(ClError::Malformed("a CL without its current patchset"))?;
            let new = commit_id(&current.commit_id)?;
            if new == old {
                continue;
            }
            changelists::prefetch(
                &shared,
                std::slice::from_ref(&new),
                std::slice::from_ref(&head),
            )
            .await?;
            let commit = store.get_commit_async(&new).await?;
            repo.add_head(&commit).await?;
            repo.set_remote_bookmark(
                remote_symbol(&cl_bookmark(number)),
                RemoteRef {
                    target: RefTarget::normal(new.clone()),
                    state: RemoteRefState::New,
                },
            );
            (
                new,
                Followed::Moved {
                    number,
                    patchset: cl.current_patchset,
                },
            )
        };
        if new != old {
            repo.set_rewritten_commit(old, new);
        }
        report.followed.push(followed);
    }

    // What was built on retired changes and moved CLs follows them.
    let imm = immutable(repo)?;
    let (mut rebased, mut emptied) = (0, 0);
    repo.rebase_descendants_with_options(&imm, &rebase_options(), |_, r| match r {
        RebasedCommit::Rewritten(_) => rebased += 1,
        RebasedCommit::Abandoned { .. } => emptied += 1,
    })
    .await?;

    // A working copy on main's history — a change that landed as itself, so is main's
    // own commit now — gets a new change on main's head, as one on a retired change does.
    let on_main: Vec<_> = {
        let mut out = Vec::new();
        for (name, id) in repo.view().wc_commit_ids() {
            if repo.index().is_ancestor(id, &head).await? {
                out.push(name.clone());
            }
        }
        out
    };
    if !on_main.is_empty() {
        let main = store.get_commit_async(&head).await?;
        for name in on_main {
            let wc = repo
                .new_commit(vec![head.clone()], main.tree())
                .write()
                .await?;
            repo.edit(name, &wc).await?;
        }
    }

    // 5. Every root built on main's history moves onto main's head.
    let imm = immutable(repo)?;
    let main_history = RevsetExpression::commits(vec![head.clone()]).ancestors();
    let roots: Vec<Commit> = RevsetExpression::all()
        .minus(&main_history)
        .minus(&imm)
        .roots()
        .evaluate(repo)?
        .stream()
        .commits(&store)
        .try_collect()
        .await?;
    let mut new_parents: HashMap<CommitId, Vec<CommitId>> = HashMap::new();
    for root in &roots {
        let mut parents: Vec<CommitId> = Vec::new();
        let mut moved = false;
        for p in root.parent_ids() {
            let onto = if repo.index().is_ancestor(p, &head).await? {
                moved |= *p != head;
                head.clone()
            } else {
                p.clone()
            };
            if !parents.contains(&onto) {
                parents.push(onto);
            }
        }
        if moved {
            new_parents.insert(root.id().clone(), parents);
        }
    }
    if !new_parents.is_empty() {
        let roots: Vec<CommitId> = new_parents.keys().cloned().collect();
        let options = rebase_options();
        repo.transform_descendants_with_options(
            roots,
            &imm,
            &new_parents,
            &RewriteRefsOptions::default(),
            async |rewriter| {
                if rewriter.parents_changed() {
                    match rebase_commit_with_options(rewriter, &options).await? {
                        RebasedCommit::Rewritten(_) => rebased += 1,
                        RebasedCommit::Abandoned { .. } => emptied += 1,
                    }
                }
                Ok(())
            },
        )
        .await?;
        // The transform leaves its rewrites recorded; settle them, as a transaction must
        // before it commits.
        repo.rebase_descendants_with_options(&imm, &rebase_options(), |_, r| match r {
            RebasedCommit::Rewritten(_) => rebased += 1,
            RebasedCommit::Abandoned { .. } => emptied += 1,
        })
        .await?;
    }
    report.rebased = rebased;
    report.emptied = emptied;
    Ok(report)
}

/// Retires a local change X whose CL landed as `landed` (step 3). X adds nothing beyond
/// what landed when it is one of the CL's patchsets, or when its tree is the landed
/// patchset C rebased onto X's parents (jj's merge of X's parents and C over C's base):
/// it is recorded as abandoned onto the landed commit, which carries the same change, so
/// its descendants and bookmarks move there and a working copy on it gets a new change.
/// Otherwise X holds edits made after C, and a new change — X's description and author,
/// on the landed commit, its tree X's edits since C applied there — replaces it. Either way
/// the change never looks divergent. Returns the new change, when there is one.
async fn retire(
    repo: &mut MutableRepo,
    x: &Commit,
    cl: &pb::ChangeList,
    landed: &Commit,
) -> Result<Option<Commit>, ClError> {
    let store = repo.store().clone();
    let patchsets: HashSet<&[u8]> = cl
        .patchsets
        .iter()
        .map(|p| p.commit_id.as_slice())
        .collect();
    if !patchsets.contains(x.id().as_bytes()) {
        let current = cl
            .patchsets
            .get((cl.current_patchset as usize).wrapping_sub(1))
            .ok_or(ClError::Malformed("a CL without its current patchset"))?;
        let c = store
            .get_commit_async(&commit_id(&current.commit_id)?)
            .await?;
        let p = store
            .get_commit_async(&commit_id(&current.base_commit_id)?)
            .await?;
        let x_base = merge_commit_trees(repo, &x.parents().await?).await?;
        let c_prime = MergedTree::merge(Merge::from_vec(vec![
            (x_base, "parents of the local change".to_owned()),
            (p.tree(), "base of the landed patchset".to_owned()),
            (c.tree(), "landed patchset".to_owned()),
        ]))
        .await?;
        if x.tree_ids() != c_prime.tree_ids() {
            let tree = MergedTree::merge(Merge::from_vec(vec![
                (landed.tree(), "landed".to_owned()),
                (c_prime, "landed patchset".to_owned()),
                (x.tree(), "edits since the landed patchset".to_owned()),
            ]))
            .await?;
            if tree.tree_ids() != landed.tree_ids() {
                let kept = repo
                    .rewrite_commit(x)
                    .generate_new_change_id()
                    .set_parents(vec![landed.id().clone()])
                    .set_tree(tree)
                    .write()
                    .await?;
                return Ok(Some(kept));
            }
        }
    }
    repo.record_abandoned_commit_with_parents(x.id().clone(), [landed.id().clone()]);
    Ok(None)
}
