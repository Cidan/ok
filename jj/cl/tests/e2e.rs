//! ok's ChangeList workflow end to end against a real SCM (docs/scm.md, build plan
//! phase 5): uploads, patchsets, stacking on someone else's CL, landing, and `jj sync`.
//!
//! The SCM is an ok scm the test runs in its own sandbox (jj/testscm). Each test
//! creates its own repository; alice and bob are clones that act as different people, and
//! ok control lands through the protocol, as ok control will.

use std::sync::Arc;

use futures::AsyncReadExt as _;
use jj_lib::backend::ChangeId;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyId;
use jj_lib::backend::TreeValue;
use jj_lib::commit::Commit;
use jj_lib::config::ConfigLayer;
use jj_lib::config::ConfigSource;
use jj_lib::config::StackedConfig;
use jj_lib::default_backend_factories::default_backend_factories;
use jj_lib::merge::Merge;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RemoteRefState;
use jj_lib::ref_name::RefName;
use jj_lib::ref_name::RemoteName;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo;
use jj_lib::repo::RepoLoader;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::revset::RemoteRefSymbolExpression;
use jj_lib::revset::ResolvedRevsetExpression;
use jj_lib::revset::RevsetExpression;
use jj_lib::revset::SymbolResolver;
use jj_lib::revset::SymbolResolverExtension;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::str_util::StringExpression;
use ok_cl::ClError;
use ok_cl::Followed;
use ok_store::RepoConfig;
use ok_store::pb;
use tonic::Status;

const ALICE: &str = "alice.human.example.com";
const BOB: &str = "bob.human.example.com";

fn settings(who: &str) -> UserSettings {
    let mut config = StackedConfig::with_defaults();
    let handle = who.split('.').next().unwrap();
    config.add_layer(
        ConfigLayer::parse(
            ConfigSource::User,
            &format!(
                "user.name = \"{handle}\"\nuser.email = \"{handle}@example.com\"\noperation.hostname = \"e2e\"\noperation.username = \"{handle}\"\n"
            ),
        )
        .unwrap(),
    );
    UserSettings::from_config(config).unwrap()
}

/// jj's default immutable set with ok's trunk(): every untracked remote bookmark
/// (`main@ok` and each `cl/<n>@ok`) and its ancestors.
fn immutable(repo: &dyn Repo) -> Result<Arc<ResolvedRevsetExpression>, ClError> {
    let heads = RevsetExpression::remote_bookmarks(
        RemoteRefSymbolExpression {
            name: StringExpression::all(),
            remote: StringExpression::all(),
        },
        Some(RemoteRefState::New),
    )
    .union(&RevsetExpression::root());
    let resolver = SymbolResolver::new(repo, &[] as &[Box<dyn SymbolResolverExtension>]);
    Ok(heads.ancestors().resolve_user_expression(repo, &resolver)?)
}

/// One person's clone of a repository.
struct Clone {
    _dir: tempfile::TempDir,
    loader: RepoLoader,
    who: &'static str,
    /// The workspace this clone is: its operation log is its own.
    workspace: WorkspaceNameBuf,
}

impl Clone {
    /// `who`'s clone as their workspace `default`.
    async fn new(endpoint: &str, repo: &ok_store::Repository, who: &'static str) -> Self {
        Self::in_workspace(endpoint, repo, who, WorkspaceName::DEFAULT.as_str()).await
    }

    /// `who`'s clone as a new workspace of theirs, `workspace`.
    async fn in_workspace(
        endpoint: &str,
        repo: &ok_store::Repository,
        who: &'static str,
        workspace: &str,
    ) -> Self {
        ok_testscm::create_workspace(who, repo.id, workspace).await;
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("repo");
        std::fs::create_dir(&repo_dir).unwrap();
        let config = RepoConfig {
            endpoint: endpoint.to_owned(),
            repository_id: repo.id,
            repository: repo.name.clone(),
            dev_principal: Some(who.to_owned()),
            cache: Some(dir.path().join("cache")),
            workspace: Some(workspace.to_owned()),
        };
        let s = settings(who);
        ok_store::init_repo(&s, &repo_dir, config, Signer::from_settings(&s).unwrap())
            .await
            .unwrap();
        let mut factories = default_backend_factories();
        factories.merge(ok_store::factories());
        let loader = RepoLoader::init_from_file_system(&s, &repo_dir, &factories).unwrap();
        let clone = Self {
            _dir: dir,
            loader,
            who,
            workspace: WorkspaceNameBuf::from(workspace),
        };
        clone.sync().await;
        clone
    }

    async fn repo(&self) -> Arc<ReadonlyRepo> {
        self.loader.load_at_head().await.unwrap()
    }

    async fn sync(&self) -> ok_cl::SyncReport {
        let repo = self.repo().await;
        let mut tx = repo.start_transaction();
        let report = ok_cl::sync(tx.repo_mut(), &immutable).await.unwrap();
        tx.commit("sync").await.unwrap();
        report
    }

    async fn get(&self, number: i64) -> Result<ok_cl::Got, ClError> {
        let repo = self.repo().await;
        let mut tx = repo.start_transaction();
        let got = ok_cl::get(tx.repo_mut(), number, &immutable).await?;
        tx.commit("get").await.unwrap();
        Ok(got)
    }

    /// A new change on `parent` with `files` set, as this person.
    async fn change(&self, parent: &CommitId, files: &[(&str, &str)], description: &str) -> Commit {
        self.write(parent, None, files, description).await
    }

    /// A new revision of `commit`'s change on `parent`, with `files` set in its tree.
    async fn amend(
        &self,
        commit: &Commit,
        parent: &CommitId,
        files: &[(&str, &str)],
        description: &str,
    ) -> Commit {
        self.write(parent, Some(commit), files, description).await
    }

    async fn write(
        &self,
        parent: &CommitId,
        of: Option<&Commit>,
        files: &[(&str, &str)],
        description: &str,
    ) -> Commit {
        let repo = self.repo().await;
        let store = repo.store().clone();
        let mut tx = repo.start_transaction();
        let base = match of {
            Some(c) => store.get_commit_async(c.id()).await.unwrap().tree(),
            None => store.get_commit_async(parent).await.unwrap().tree(),
        };
        let mut builder = MergedTreeBuilder::new(base);
        for (p, contents) in files {
            let p = RepoPathBuf::from_internal_string(*p).unwrap();
            let id = store
                .write_file(
                    &p,
                    &mut futures::io::Cursor::new(contents.as_bytes().to_vec()),
                )
                .await
                .unwrap();
            builder.set_or_remove(
                p,
                Merge::normal(TreeValue::File {
                    id,
                    executable: false,
                    copy_id: CopyId::placeholder(),
                }),
            );
        }
        let tree = builder.write_tree().await.unwrap();
        let commit = match of {
            Some(c) => {
                let c = store.get_commit_async(c.id()).await.unwrap();
                tx.repo_mut()
                    .rewrite_commit(&c)
                    .set_parents(vec![parent.clone()])
                    .set_tree(tree)
                    .set_description(description)
                    .write()
                    .await
                    .unwrap()
            }
            None => tx
                .repo_mut()
                .new_commit(vec![parent.clone()], tree)
                .set_description(description)
                .write()
                .await
                .unwrap(),
        };
        tx.repo_mut().rebase_descendants().await.unwrap();
        tx.commit("write").await.unwrap();
        commit
    }

    /// Makes `commit` this clone's working-copy commit, as `jj edit` would.
    async fn edit(&self, commit: &Commit) {
        let repo = self.repo().await;
        let mut tx = repo.start_transaction();
        tx.repo_mut()
            .set_wc_commit(self.workspace.clone(), commit.id().clone())
            .unwrap();
        tx.commit("edit").await.unwrap();
    }

    async fn wc(&self) -> Commit {
        let repo = self.repo().await;
        let id = repo
            .view()
            .get_wc_commit_id(&self.workspace)
            .unwrap()
            .clone();
        repo.store().get_commit_async(&id).await.unwrap()
    }

    async fn upload(
        &self,
        commits: &[&Commit],
        readiness: pb::upload_request::Readiness,
    ) -> Result<pb::UploadResponse, ClError> {
        let repo = self.repo().await;
        let mut loaded = Vec::new();
        for c in commits {
            loaded.push(repo.store().get_commit_async(c.id()).await.unwrap());
        }
        ok_cl::upload(repo.as_ref(), &loaded, readiness, Vec::new()).await
    }

    async fn upload_ok(&self, commits: &[&Commit]) -> Vec<pb::upload_response::Result> {
        self.upload(commits, pb::upload_request::Readiness::Ready)
            .await
            .unwrap_or_else(|e| panic!("{} upload: {e}", self.who))
            .results
    }

    /// The visible commits of a change.
    async fn visible(&self, change: &ChangeId) -> Vec<CommitId> {
        let repo = self.repo().await;
        match repo.resolve_change_id(change).await.unwrap() {
            None => Vec::new(),
            Some(t) => t.visible_with_offsets().map(|(_, id)| id.clone()).collect(),
        }
    }

    async fn only(&self, change: &ChangeId) -> Commit {
        let visible = self.visible(change).await;
        assert_eq!(
            visible.len(),
            1,
            "{}: change {} is visible as {visible:?}",
            self.who,
            change.hex()
        );
        self.repo()
            .await
            .store()
            .get_commit_async(&visible[0])
            .await
            .unwrap()
    }

    async fn main(&self) -> CommitId {
        ok_cl::main_target(self.repo().await.as_ref()).unwrap()
    }

    async fn read(&self, commit: &CommitId, p: &str) -> Option<String> {
        let repo = self.repo().await;
        let commit = repo.store().get_commit_async(commit).await.unwrap();
        let value = commit
            .tree()
            .path_value(&RepoPathBuf::from_internal_string(p).unwrap())
            .await
            .unwrap();
        match value.into_resolved().ok()?? {
            TreeValue::File { id, .. } => {
                let mut out = String::new();
                repo.store()
                    .read_file(RepoPath::from_internal_string(p).unwrap(), &id)
                    .await
                    .unwrap()
                    .read_to_string(&mut out)
                    .await
                    .unwrap();
                Some(out)
            }
            _ => None,
        }
    }
}

/// ok control, over the protocol, and the landing queue, which alone moves main.
struct Control {
    checks: pb::landing_checks_client::LandingChecksClient<ok_testscm::Svc>,
    queue: pb::landing_client::LandingClient<ok_testscm::Svc>,
    repos: pb::repositories_client::RepositoriesClient<ok_testscm::Svc>,
    repo: i64,
}

impl Control {
    async fn new(repo: i64) -> Self {
        Self {
            checks: ok_testscm::landing_checks(ok_testscm::CONTROL).await,
            queue: ok_testscm::landing(ok_testscm::AUTOMATION).await,
            repos: ok_testscm::repositories(ok_testscm::CONTROL).await,
            repo,
        }
    }

    async fn head(&mut self) -> Vec<u8> {
        self.repos
            .get_repository(pb::GetRepositoryRequest {
                key: Some(pb::get_repository_request::Key::Id(self.repo)),
            })
            .await
            .unwrap()
            .into_inner()
            .head
    }

    async fn land_at(
        &mut self,
        expected: Vec<u8>,
        items: &[(i64, i32)],
        dry_run: bool,
    ) -> Result<pb::LandResponse, Status> {
        // ok control dry-runs a landing; the landing queue lands.
        let req = pb::LandRequest {
            repository_id: self.repo,
            items: items
                .iter()
                .map(|&(number, patchset)| pb::land_request::Item {
                    number,
                    patchset,
                    candidate: Vec::new(),
                    actor_id: String::new(),
                })
                .collect(),
            actor_id: ok_testscm::id(ALICE).await,
            expected_head: expected,
            dry_run,
        };
        if dry_run {
            self.checks.dry_run_land(req).await
        } else {
            self.queue.land(req).await
        }
        .map(|r| r.into_inner())
    }

    /// Lands at main's current head.
    async fn land(&mut self, items: &[(i64, i32)]) -> Result<pb::LandResponse, Status> {
        let head = self.head().await;
        self.land_at(head, items, false).await
    }
}

fn land_reason(s: &Status) -> pb::land_rejection::Reason {
    let rej = ok_store::changelists::land_rejection(s)
        .unwrap_or_else(|| panic!("{s} carries no LandRejection"));
    pb::land_rejection::Reason::try_from(rej.reason).unwrap()
}

fn upload_reason(e: ClError) -> pb::upload_rejection::Reason {
    let ClError::Scm(s) = e else { panic!("{e}") };
    let rej = ok_store::changelists::upload_rejection(&s)
        .unwrap_or_else(|| panic!("{s} carries no UploadRejection"));
    pb::upload_rejection::Reason::try_from(rej.reason).unwrap()
}

fn id(bytes: &[u8]) -> CommitId {
    CommitId::from_bytes(bytes)
}

/// A repository for one test, with alice's and bob's clones and ok control.
async fn setup(test: &str) -> (Clone, Clone, Control) {
    let endpoint = ok_testscm::endpoint();
    let remote = ok_testscm::create_repository(test, ALICE).await;
    let alice = Clone::new(endpoint, &remote, ALICE).await;
    let bob = Clone::new(endpoint, &remote, BOB).await;
    let control = Control::new(remote.id).await;
    (alice, bob, control)
}

fn root() -> CommitId {
    CommitId::from_bytes(&[0; 32])
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_a_stack_then_a_patchset() {
    if !ok_testscm::available() {
        return;
    }
    let (alice, _, _) = setup("stack").await;
    let a = alice.change(&root(), &[("a", "1")], "add a").await;
    let b = alice.change(a.id(), &[("b", "1")], "add b").await;
    let res = alice.upload_ok(&[&a, &b]).await;
    assert_eq!(
        res.iter().map(|r| r.outcome).collect::<Vec<_>>(),
        vec![1, 1],
        "{res:?}"
    );
    let (na, nb) = (res[0].number, res[1].number);

    let b2 = alice
        .amend(&b, a.id(), &[("b", "2")], "add b, better")
        .await;
    let res = alice.upload_ok(&[&a, &b2]).await;
    assert_eq!(
        (
            res[0].outcome,
            res[1].outcome,
            res[1].number,
            res[1].patchset
        ),
        (3, 2, nb, 2)
    );

    let shared = ok_cl::shared(alice.repo().await.as_ref()).unwrap();
    let cl = ok_store::changelists::get_change_list(&shared, nb)
        .await
        .unwrap();
    assert_eq!(
        (
            cl.parent_number,
            cl.current_patchset,
            cl.description.as_str()
        ),
        (na, 2, "add b, better")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn uploading_to_someone_elses_cl_is_refused() {
    if !ok_testscm::available() {
        return;
    }
    let (alice, bob, _) = setup("not-author").await;
    let a = alice.change(&root(), &[("a", "1")], "alice's").await;
    let n = alice.upload_ok(&[&a]).await[0].number;

    let got = bob.get(n).await.unwrap();
    assert_eq!(got.commit.id(), a.id());
    // jj keeps bob from rewriting the immutable cl/<n>@ok; forcing it anyway makes a new
    // revision of alice's change, which the SCM refuses.
    let forced = bob
        .amend(&got.commit, &root(), &[("a", "bob")], "bob's take")
        .await;
    let err = bob
        .upload(&[&forced], pb::upload_request::Readiness::Unchanged)
        .await
        .unwrap_err();
    assert_eq!(upload_reason(err), pb::upload_rejection::Reason::NotAuthor);

    // Getting your own CL would only make it divergent.
    assert!(matches!(
        alice.get(n).await,
        Err(ClError::AlreadyInView { .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn stacking_on_someone_elses_cl_follows_it_through_patchsets_and_landing() {
    if !ok_testscm::available() {
        return;
    }
    let (alice, bob, mut control) = setup("follow").await;
    let a = alice.change(&root(), &[("a", "1")], "alice's base").await;
    let na = alice.upload_ok(&[&a]).await[0].number;

    let got = bob.get(na).await.unwrap();
    let b = bob
        .change(got.commit.id(), &[("b", "1")], "bob on alice")
        .await;
    let res = bob.upload_ok(&[&b]).await;
    let nb = res[0].number;
    let shared = ok_cl::shared(bob.repo().await.as_ref()).unwrap();
    assert_eq!(
        ok_store::changelists::get_change_list(&shared, nb)
            .await
            .unwrap()
            .parent_number,
        na
    );

    // alice's new patchset: bob's CL follows it.
    let a2 = alice
        .amend(&a, &root(), &[("a", "2")], "alice's base, revised")
        .await;
    alice.upload_ok(&[&a2]).await;
    let report = bob.sync().await;
    assert!(
        matches!(report.followed[..], [Followed::Moved { number, patchset: 2 }] if number == na),
        "{report:?}"
    );
    let b2 = bob.only(b.change_id()).await;
    assert_eq!(b2.parent_ids(), [a2.id().clone()]);
    assert_eq!(bob.read(b2.id(), "a").await.as_deref(), Some("2"));
    let res = bob.upload_ok(&[&b2]).await;
    assert_eq!((res[0].number, res[0].patchset), (nb, 2));

    // alice's CL lands; bob's bookmark goes, and his CL lands on it.
    control.land(&[(na, 2)]).await.unwrap();
    let report = bob.sync().await;
    assert!(
        matches!(&report.followed[..], [Followed::Landed { number, commit }] if *number == na && commit == a2.id()),
        "{report:?}"
    );
    let repo = bob.repo().await;
    assert!(
        repo.view()
            .get_remote_bookmark(
                RefName::new(&ok_cl::cl_bookmark(na))
                    .to_remote_symbol(RemoteName::new(ok_cl::REMOTE))
            )
            .is_absent()
    );
    assert_eq!(bob.main().await, *a2.id());
    let landed = control.land(&[(nb, 2)]).await.unwrap();
    assert_eq!(
        id(&landed.head),
        *b2.id(),
        "a patchset on main's head fast-forwards"
    );
    bob.sync().await;
    assert_eq!(bob.only(b.change_id()).await.id(), b2.id());
}

#[tokio::test(flavor = "multi_thread")]
async fn land_by_fast_forward_and_by_splice() {
    if !ok_testscm::available() {
        return;
    }
    let (alice, _, mut control) = setup("land").await;
    let x = alice.change(&root(), &[("x", "1")], "x").await;
    let y = alice.change(&root(), &[("y", "1")], "y").await;
    let res = alice.upload_ok(&[&x, &y]).await;
    let first = control.land(&[(res[0].number, 1)]).await.unwrap();
    assert_eq!(id(&first.head), *x.id());
    let second = control.land(&[(res[1].number, 1)]).await.unwrap();
    let spliced = id(&second.head);
    assert_ne!(spliced, *y.id());
    alice.sync().await;
    let landed = alice.only(y.change_id()).await;
    assert_eq!(*landed.id(), spliced);
    assert_eq!(landed.parent_ids(), [x.id().clone()]);
    assert_eq!(
        (
            alice.read(&spliced, "x").await.as_deref(),
            alice.read(&spliced, "y").await.as_deref()
        ),
        (Some("1"), Some("1"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_path_conflict_is_refused_until_synced_and_resolved() {
    if !ok_testscm::available() {
        return;
    }
    let (alice, _, mut control) = setup("conflict").await;
    let p = alice.change(&root(), &[("f", "1")], "f is 1").await;
    let q = alice
        .change(&root(), &[("f", "2"), ("g", "2")], "f is 2")
        .await;
    let res = alice.upload_ok(&[&p, &q]).await;
    let (np, nq) = (res[0].number, res[1].number);
    control.land(&[(np, 1)]).await.unwrap();
    let err = control.land(&[(nq, 1)]).await.unwrap_err();
    assert_eq!(land_reason(&err), pb::land_rejection::Reason::PathConflict);
    assert_eq!(
        ok_store::changelists::land_rejection(&err).unwrap().paths,
        vec!["f".to_owned()]
    );

    // jj sync rebases q onto main with jj's merge: f is a first-class conflict.
    alice.sync().await;
    let q2 = alice.only(q.change_id()).await;
    assert_eq!(q2.parent_ids(), [p.id().clone()]);
    assert!(q2.has_conflict());
    let resolved = alice.amend(&q2, p.id(), &[("f", "3")], "f is 3").await;
    assert!(!resolved.has_conflict());
    let res = alice.upload_ok(&[&resolved]).await;
    assert_eq!((res[0].number, res[0].patchset), (nq, 2));
    let landed = control.land(&[(nq, 2)]).await.unwrap();
    assert_eq!(id(&landed.head), *resolved.id());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_moved_head_is_refused_then_revalidated() {
    if !ok_testscm::available() {
        return;
    }
    let (alice, _, mut control) = setup("moved").await;
    let x = alice.change(&root(), &[("x", "1")], "x").await;
    let y = alice.change(&root(), &[("y", "1")], "y").await;
    let res = alice.upload_ok(&[&x, &y]).await;
    let validated = control.head().await;
    control.land(&[(res[0].number, 1)]).await.unwrap();
    let err = control
        .land_at(validated, &[(res[1].number, 1)], false)
        .await
        .unwrap_err();
    assert_eq!(land_reason(&err), pb::land_rejection::Reason::HeadMoved);
    let head = control.head().await;
    control
        .land_at(head, &[(res[1].number, 1)], false)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stack_lands_bottom_up_and_a_dry_run_agrees() {
    if !ok_testscm::available() {
        return;
    }
    let (alice, _, mut control) = setup("stack-land").await;
    let other = alice.change(&root(), &[("o", "1")], "other").await;
    let a = alice.change(&root(), &[("a", "1")], "a").await;
    let b = alice.change(a.id(), &[("b", "1")], "b").await;
    let res = alice.upload_ok(&[&other, &a, &b]).await;
    let (no, na, nb) = (res[0].number, res[1].number, res[2].number);
    control.land(&[(no, 1)]).await.unwrap();

    let err = control.land(&[(nb, 1)]).await.unwrap_err();
    assert_eq!(
        land_reason(&err),
        pb::land_rejection::Reason::ParentNotSubmitted
    );
    let head = control.head().await;
    let dry = control
        .land_at(head.clone(), &[(na, 1), (nb, 1)], true)
        .await
        .unwrap();
    assert_eq!(control.head().await, head, "a dry run writes nothing");
    let landed = control
        .land_at(head, &[(na, 1), (nb, 1)], false)
        .await
        .unwrap();
    assert_eq!(dry.landed.len(), 2);
    assert_eq!(
        landed.landed.iter().map(|l| l.number).collect::<Vec<_>>(),
        vec![na, nb]
    );
    assert_eq!(landed.head, landed.landed[1].commit_id);

    alice.sync().await;
    let main = alice.main().await;
    for f in ["o", "a", "b"] {
        assert_eq!(alice.read(&main, f).await.as_deref(), Some("1"));
    }
    assert_eq!(*alice.only(b.change_id()).await.id(), main);
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_retires_landed_changes_without_divergence() {
    if !ok_testscm::available() {
        return;
    }
    let (alice, _, mut control) = setup("retire").await;
    let other = alice.change(&root(), &[("o", "1")], "other").await;
    let a = alice.change(&root(), &[("a", "1")], "a").await;
    let res = alice.upload_ok(&[&other, &a]).await;
    let (no, na) = (res[0].number, res[1].number);
    alice.edit(&a).await;

    // main moves, and sync rebases a: a' is no patchset of its CL.
    control.land(&[(no, 1)]).await.unwrap();
    alice.sync().await;
    let rebased = alice.only(a.change_id()).await;
    assert_ne!(rebased.id(), a.id());
    assert_eq!(alice.wc().await.id(), rebased.id());

    // a lands as a splice of patchset 1: sync retires a', whose tree is exactly that.
    let landed = id(&control.land(&[(na, 1)]).await.unwrap().head);
    let report = alice.sync().await;
    assert!(
        matches!(&report.retired[..], [r] if r.number == na && r.kept.is_none()),
        "{report:?}"
    );
    assert_eq!(
        *alice.only(a.change_id()).await.id(),
        landed,
        "the change is the landed commit alone"
    );
    let wc = alice.wc().await;
    assert_ne!(wc.id(), &landed, "the working copy is never left on main");
    assert_eq!(wc.parent_ids(), std::slice::from_ref(&landed));

    // Edits made after the landed patchset survive as a new change.
    let c = alice.change(&landed, &[("c", "1")], "c").await;
    let nc = alice.upload_ok(&[&c]).await[0].number;
    let c2 = alice
        .amend(&c, &landed, &[("later", "edit")], "c, with a later edit")
        .await;
    let c_landed = id(&control.land(&[(nc, 1)]).await.unwrap().head);
    assert_eq!(c_landed, *c.id());
    let report = alice.sync().await;
    let kept = match &report.retired[..] {
        [r] if r.number == nc => r.kept.clone().expect("the later edit was kept"),
        other => panic!("retired {other:?}"),
    };
    assert_eq!(*alice.only(c.change_id()).await.id(), c_landed);
    assert_ne!(kept.change_id(), c2.change_id());
    let kept = alice.only(kept.change_id()).await;
    assert_eq!(kept.parent_ids(), std::slice::from_ref(&c_landed));
    assert_eq!(
        alice.read(kept.id(), "later").await.as_deref(),
        Some("edit")
    );
    assert_eq!(alice.read(kept.id(), "c").await.as_deref(), Some("1"));
    assert!(matches!(alice.get(nc).await, Err(ClError::Landed { .. })));
}

// Every workspace is isolated from every other, the same person's too (docs/scm.md,
// "Decisions", 3): neither sees the other's changes, a sync in one rebases its own changes and
// leaves the other's operation log exactly as it was, and the other syncs on its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_sync_in_one_workspace_leaves_another_alone() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = ok_testscm::endpoint();
    let remote = ok_testscm::create_repository("isolated-sync", ALICE).await;
    let laptop = Clone::new(endpoint, &remote, ALICE).await;
    let desk = Clone::in_workspace(endpoint, &remote, ALICE, "desk").await;
    let bob = Clone::new(endpoint, &remote, BOB).await;
    let mut control = Control::new(remote.id).await;

    let l = laptop.change(&root(), &[("l", "1")], "laptop's").await;
    laptop.edit(&l).await;
    let d = desk.change(&root(), &[("d", "1")], "desk's").await;
    desk.edit(&d).await;
    assert!(laptop.visible(d.change_id()).await.is_empty());
    assert!(desk.visible(l.change_id()).await.is_empty());

    // main moves, and laptop syncs: its change moves onto main; desk's log does not move at all.
    let b = bob.change(&root(), &[("b", "1")], "bob's").await;
    let nb = bob.upload_ok(&[&b]).await[0].number;
    let main = id(&control.land(&[(nb, 1)]).await.unwrap().head);
    let desk_heads = desk.loader.op_heads_store().get_op_heads().await.unwrap();
    let report = laptop.sync().await;
    assert_eq!(report.rebased, 1, "{report:?}");
    assert_eq!(
        laptop.only(l.change_id()).await.parent_ids(),
        std::slice::from_ref(&main)
    );
    assert_eq!(
        desk.loader.op_heads_store().get_op_heads().await.unwrap(),
        desk_heads,
        "desk's operation log is as it was"
    );
    assert_eq!(desk.wc().await.id(), d.id());
    assert_eq!(desk.only(d.change_id()).await.parent_ids(), [root()]);

    // desk catches up when it syncs.
    desk.sync().await;
    let d2 = desk.only(d.change_id()).await;
    assert_eq!(d2.parent_ids(), [main]);
    assert_eq!(desk.wc().await.id(), d2.id());
    assert!(laptop.visible(d.change_id()).await.is_empty());
}

// Work moves between a person's workspaces as it moves between people: as a CL. desk gets
// laptop's CL, builds on it, and follows laptop's next patchset when it syncs; laptop never
// sees desk's change.
#[tokio::test(flavor = "multi_thread")]
async fn a_cl_carries_work_between_a_persons_workspaces() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = ok_testscm::endpoint();
    let remote = ok_testscm::create_repository("isolated-cl", ALICE).await;
    let laptop = Clone::new(endpoint, &remote, ALICE).await;
    let desk = Clone::in_workspace(endpoint, &remote, ALICE, "desk").await;

    let a = laptop.change(&root(), &[("a", "1")], "laptop's").await;
    let na = laptop.upload_ok(&[&a]).await[0].number;
    assert!(desk.visible(a.change_id()).await.is_empty());
    let got = desk.get(na).await.unwrap();
    assert_eq!(got.commit.id(), a.id());
    let on = desk
        .change(got.commit.id(), &[("d", "1")], "desk on laptop's")
        .await;
    desk.edit(&on).await;

    let a2 = laptop
        .amend(&a, &root(), &[("a", "2")], "laptop's, revised")
        .await;
    laptop.upload_ok(&[&a2]).await;
    let report = desk.sync().await;
    assert!(
        matches!(report.followed[..], [Followed::Moved { number, patchset: 2 }] if number == na),
        "{report:?}"
    );
    let on2 = desk.only(on.change_id()).await;
    assert_eq!(on2.parent_ids(), [a2.id().clone()]);
    assert_eq!(desk.read(on2.id(), "a").await.as_deref(), Some("2"));
    assert!(laptop.visible(on.change_id()).await.is_empty());
}
