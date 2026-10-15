//! ok's jj stores against a real SCM (docs/scm.md, "The client"): an ok scm the
//! test runs in its own sandbox (jj/testscm). Each test creates its own repository.
//! A "machine" is a repository directory with its own cache and outbox; two of them in one
//! process are two machines sharing the caller's operation log.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use jj_lib::backend::ChangeId;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyHistory;
use jj_lib::backend::CopyId;
use jj_lib::backend::TreeValue;
use jj_lib::commit::Commit;
use jj_lib::config::ConfigLayer;
use jj_lib::config::ConfigSource;
use jj_lib::config::StackedConfig;
use jj_lib::default_backend_factories::default_backend_factories;
use jj_lib::merge::Merge;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::ObjectId as _;
use jj_lib::object_id::PrefixResolution;
use jj_lib::op_store::Operation;
use jj_lib::op_store::OperationId;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo as _;
use jj_lib::repo::RepoLoader;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use ok_store::RepoConfig;

/// Who every machine acts as.
const PRINCIPAL: &str = "e2e.human.example.com";

/// The SCM under test.
fn endpoint() -> String {
    ok_testscm::endpoint().to_owned()
}

fn settings() -> UserSettings {
    let mut config = StackedConfig::with_defaults();
    config.add_layer(
        ConfigLayer::parse(
            ConfigSource::User,
            "user.name = \"e2e\"\nuser.email = \"e2e@example.com\"\noperation.hostname = \"e2e-host\"\noperation.username = \"e2e\"\n",
        )
        .unwrap(),
    );
    UserSettings::from_config(config).unwrap()
}

/// A repository on the SCM made for one test.
/// The workspace the tests' machines attach: one workspace, one operation log, on every machine
/// it is attached on.
const WORKSPACE: &str = "default";

/// A new repository, with the principal's workspace WORKSPACE.
async fn new_repository(_endpoint: &str, test: &str) -> ok_store::Repository {
    let repo = ok_testscm::create_repository(test, PRINCIPAL).await;
    ok_testscm::create_workspace(PRINCIPAL, repo.id, WORKSPACE).await;
    repo
}

/// One machine's copy of a workspace of a repository: its own `.jj/repo`, cache, and outbox.
struct Machine {
    _dir: tempfile::TempDir,
    repo_dir: PathBuf,
    loader: RepoLoader,
}

impl Machine {
    /// A machine with WORKSPACE attached.
    async fn new(endpoint: &str, repo: &ok_store::Repository) -> Self {
        Self::in_workspace(endpoint, repo, WORKSPACE).await
    }

    /// A machine with `workspace` attached.
    async fn in_workspace(endpoint: &str, repo: &ok_store::Repository, workspace: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("repo");
        std::fs::create_dir(&repo_dir).unwrap();
        let config = RepoConfig {
            endpoint: endpoint.to_owned(),
            repository_id: repo.id,
            repository: repo.name.clone(),
            dev_principal: Some(PRINCIPAL.to_owned()),
            cache: Some(dir.path().join("cache")),
            workspace: Some(workspace.to_owned()),
        };
        let s = settings();
        ok_store::init_repo(&s, &repo_dir, config, Signer::from_settings(&s).unwrap())
            .await
            .unwrap();
        let mut factories = default_backend_factories();
        factories.merge(ok_store::factories());
        let loader = RepoLoader::init_from_file_system(&s, &repo_dir, &factories).unwrap();
        Self {
            _dir: dir,
            repo_dir,
            loader,
        }
    }

    async fn head(&self) -> Arc<ReadonlyRepo> {
        self.loader.load_at_head().await.unwrap()
    }

    async fn op_heads(&self) -> Vec<OperationId> {
        self.loader.op_heads_store().get_op_heads().await.unwrap()
    }

    fn outbox_operations(&self) -> usize {
        ok_store::Shared::for_store_path(&self.repo_dir.join("store"))
            .unwrap()
            .outbox
            .operations()
            .len()
    }
}

enum Content<'a> {
    File(&'a [u8]),
    Symlink(&'a str),
    Absent,
}

fn path(p: &str) -> RepoPathBuf {
    RepoPathBuf::from_internal_string(p).unwrap()
}

/// Writes a child of `parent` with `changes` into the transaction's repository. The parent is
/// read through the transaction's own store: a commit carries the store that loaded it, and
/// its tree writes go wherever that store does.
async fn child(
    tx: &mut jj_lib::transaction::Transaction,
    parent: &Commit,
    changes: &[(&str, Content<'_>)],
    description: &str,
) -> Commit {
    let store = tx.repo().store().clone();
    let parent = store.get_commit_async(parent.id()).await.unwrap();
    let mut builder = MergedTreeBuilder::new(parent.tree());
    for (p, content) in changes {
        let p = path(p);
        let value = match content {
            Content::File(bytes) => {
                let id = store
                    .write_file(&p, &mut futures::io::Cursor::new(bytes.to_vec()))
                    .await
                    .unwrap();
                Merge::normal(TreeValue::File {
                    id,
                    executable: false,
                    copy_id: CopyId::placeholder(),
                })
            }
            Content::Symlink(target) => Merge::normal(TreeValue::Symlink(
                store.write_symlink(&p, target).await.unwrap(),
            )),
            Content::Absent => Merge::absent(),
        };
        builder.set_or_remove(p, value);
    }
    let tree = builder.write_tree().await.unwrap();
    tx.repo_mut()
        .new_commit(vec![parent.id().clone()], tree)
        .set_description(description)
        .write()
        .await
        .unwrap()
}

async fn read(repo: &Arc<ReadonlyRepo>, commit: &CommitId, p: &str) -> Option<Vec<u8>> {
    let commit = repo.store().get_commit_async(commit).await.unwrap();
    let value = commit.tree().path_value(&path(p)).await.unwrap();
    match value.into_resolved().ok()?? {
        TreeValue::File { id, .. } => {
            let mut out = Vec::new();
            repo.store()
                .read_file(RepoPath::from_internal_string(p).unwrap(), &id)
                .await
                .unwrap()
                .read_to_end(&mut out)
                .await
                .unwrap();
            Some(out)
        }
        TreeValue::Symlink(id) => Some(
            repo.store()
                .read_symlink(&path(p), &id)
                .await
                .unwrap()
                .into_bytes(),
        ),
        _ => None,
    }
}

fn noise(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        out.push_str(": ");
        out.push_str(&s.to_string());
        source = s.source();
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_repository_is_shared_by_every_machine() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "share").await;
    assert_eq!(
        remote.head.to_bytes(),
        vec![0; 32],
        "a new repository's main is the root commit"
    );

    let a = Machine::new(&endpoint, &remote).await;
    let repo = a.head().await;
    assert_eq!(
        repo.op_id(),
        repo.op_store().root_operation_id(),
        "a new operation log starts at the root"
    );
    assert_eq!(
        repo.view().heads(),
        &HashSet::from([repo.store().root_commit_id().clone()])
    );

    let big = noise((1 << 20) + 12_345, 1);
    // Compressible: the SCM stores and sends its chunks zstd-compressed.
    let text = text_lines("a line of a compressible file", 20_000);
    let mut tx = repo.start_transaction();
    let root = repo.store().root_commit();
    let first = child(
        &mut tx,
        &root,
        &[
            ("small.txt", Content::File(b"hello\n")),
            ("big.bin", Content::File(&big)),
            ("text.txt", Content::File(&text)),
            ("empty", Content::File(b"")),
            ("link", Content::Symlink("small.txt")),
        ],
        "first",
    )
    .await;
    let repo = tx.commit("first").await.unwrap();

    // A second machine, with a cold cache, reads everything from the SCM.
    let b = Machine::new(&endpoint, &remote).await;
    let on_b = b.head().await;
    assert_eq!(on_b.op_id(), repo.op_id());
    assert!(on_b.view().heads().contains(first.id()));
    assert_eq!(
        read(&on_b, first.id(), "small.txt").await.as_deref(),
        Some(&b"hello\n"[..])
    );
    assert_eq!(read(&on_b, first.id(), "big.bin").await, Some(big));
    assert_eq!(read(&on_b, first.id(), "text.txt").await, Some(text));
    assert_eq!(
        read(&on_b, first.id(), "empty").await.as_deref(),
        Some(&b""[..])
    );
    assert_eq!(
        read(&on_b, first.id(), "link").await.as_deref(),
        Some(&b"small.txt"[..])
    );

    // And what it writes, the first sees.
    let mut tx = on_b.start_transaction();
    let second = child(
        &mut tx,
        &first,
        &[("small.txt", Content::File(b"hello again\n"))],
        "second",
    )
    .await;
    tx.commit("second").await.unwrap();
    let on_a = a.head().await;
    assert!(on_a.view().heads().contains(second.id()));
    assert_eq!(
        read(&on_a, second.id(), "small.txt").await.as_deref(),
        Some(&b"hello again\n"[..])
    );
}

// Every workspace is isolated from every other, the same person's too (docs/scm.md,
// "Decisions", 3): what one does is nowhere in another's operation log or view, and a commit one
// publishes reaches the other only by its ID — as a CL hands it over — never through its view. A
// workspace that was never made has no log at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_persons_workspaces_are_isolated() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "isolated").await;
    ok_testscm::create_workspace(PRINCIPAL, remote.id, "laptop").await;
    ok_testscm::create_workspace(PRINCIPAL, remote.id, "desk").await;
    let laptop = Machine::in_workspace(&endpoint, &remote, "laptop").await;
    let desk = Machine::in_workspace(&endpoint, &remote, "desk").await;

    let repo = laptop.head().await;
    let mut tx = repo.start_transaction();
    let root = repo.store().root_commit();
    let work = child(
        &mut tx,
        &root,
        &[("f", Content::File(b"laptop's\n"))],
        "laptop's work",
    )
    .await;
    let repo = tx.commit("laptop's work").await.unwrap();

    let on_desk = desk.head().await;
    assert_eq!(
        on_desk.op_id(),
        on_desk.op_store().root_operation_id(),
        "desk's operation log is still at the root"
    );
    assert!(!on_desk.view().heads().contains(work.id()));
    assert!(
        desk.loader
            .op_store()
            .read_operation(repo.op_id())
            .await
            .is_err(),
        "laptop's operation is not in desk's log"
    );

    // desk's own work leaves laptop's log as it was.
    let mut tx = on_desk.start_transaction();
    let desk_root = on_desk.store().root_commit();
    child(
        &mut tx,
        &desk_root,
        &[("g", Content::File(b"desk's\n"))],
        "desk's work",
    )
    .await;
    tx.commit("desk's work").await.unwrap();
    assert_eq!(laptop.op_heads().await, vec![repo.op_id().clone()]);

    // The commit is the repository's, public by its ID: what a CL hands another workspace.
    assert_eq!(
        read(&on_desk, work.id(), "f").await.as_deref(),
        Some(&b"laptop's\n"[..])
    );

    // A workspace no one made has no operation log to read, and none is made by asking.
    let ghost = Machine::in_workspace(&endpoint, &remote, "ghost").await;
    let err = ghost
        .loader
        .op_heads_store()
        .get_op_heads()
        .await
        .unwrap_err();
    let chain = error_chain(&err);
    assert!(
        chain.contains("not found") && chain.contains("no workspace") && chain.contains("ghost"),
        "{chain}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unpublished_transaction_stays_invisible_until_the_next_publish() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "outbox").await;
    let a = Machine::new(&endpoint, &remote).await;
    let b = Machine::new(&endpoint, &remote).await;
    let base = a.head().await;
    let root = base.store().root_commit();

    // A command killed before update_op_heads leaves its transaction in the outbox.
    let mut tx = base.start_transaction();
    let staged = child(&mut tx, &root, &[("f", Content::File(b"staged"))], "staged").await;
    let unpublished = tx.write("staged").await.unwrap().leave_unpublished();
    let staged_op = unpublished.op_id().clone();
    assert_eq!(a.outbox_operations(), 1);
    assert_eq!(
        a.op_heads().await,
        vec![base.op_id().clone()],
        "the heads did not move"
    );
    let on_b = b.head().await;
    assert!(!on_b.view().heads().contains(staged.id()));
    assert!(
        on_b.op_store().read_operation(&staged_op).await.is_err(),
        "nothing of it reached the SCM"
    );

    // It resolves by prefix from the outbox, as `--no-integrate-operation` needs.
    let prefix = HexPrefix::try_from_hex(&staged_op.hex()[..12]).unwrap();
    assert_eq!(
        a.head()
            .await
            .op_store()
            .resolve_operation_id_prefix(&prefix)
            .await
            .unwrap(),
        PrefixResolution::SingleMatch(staged_op.clone())
    );

    // The next command publishes the whole outbox with its own transaction.
    let mut tx = base.start_transaction();
    let next = child(&mut tx, &root, &[("g", Content::File(b"next"))], "next").await;
    tx.commit("next").await.unwrap();
    assert_eq!(a.outbox_operations(), 0);
    let on_b = b.head().await;
    assert!(on_b.view().heads().contains(next.id()));
    assert_eq!(
        on_b.op_store()
            .read_operation(&staged_op)
            .await
            .unwrap()
            .parents,
        vec![base.op_id().clone()]
    );
    assert_eq!(
        read(&on_b, staged.id(), "f").await.as_deref(),
        Some(&b"staged"[..])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_transactions_diverge_and_the_next_load_resolves_them() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "diverge").await;
    let a = Machine::new(&endpoint, &remote).await;
    let b = Machine::new(&endpoint, &remote).await;
    let on_a = a.head().await;
    let on_b = b.head().await;
    assert_eq!(on_a.op_id(), on_b.op_id());
    let root = on_a.store().root_commit();

    let mut tx_a = on_a.start_transaction();
    let x = child(&mut tx_a, &root, &[("f", Content::File(b"from a"))], "x").await;
    let mut tx_b = on_b.start_transaction();
    let y = child(&mut tx_b, &root, &[("f", Content::File(b"from b"))], "y").await;
    let (ra, rb) = futures::join!(tx_a.commit("x"), tx_b.commit("y"));
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    let heads: HashSet<OperationId> = a.op_heads().await.into_iter().collect();
    assert_eq!(
        heads,
        HashSet::from([ra.op_id().clone(), rb.op_id().clone()]),
        "both transactions landed as heads"
    );

    let merged = a.head().await;
    assert_eq!(
        a.op_heads().await,
        vec![merged.op_id().clone()],
        "loading merged the divergent operations"
    );
    assert_eq!(merged.operation().parent_ids().len(), 2);
    assert!(merged.view().heads().contains(x.id()) && merged.view().heads().contains(y.id()));
    assert_eq!(b.head().await.op_id(), merged.op_id());
}

#[tokio::test(flavor = "multi_thread")]
async fn operation_prefixes_and_collection() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "prefix").await;
    let a = Machine::new(&endpoint, &remote).await;
    let repo = a.head().await;
    let root = repo.store().root_commit();
    let mut tx = repo.start_transaction();
    child(&mut tx, &root, &[("f", Content::File(b"x"))], "x").await;
    let repo = tx.commit("x").await.unwrap();
    let op_store = repo.op_store();

    let full = HexPrefix::try_from_hex(repo.op_id().hex()).unwrap();
    assert_eq!(
        op_store.resolve_operation_id_prefix(&full).await.unwrap(),
        PrefixResolution::SingleMatch(repo.op_id().clone())
    );
    let odd = HexPrefix::try_from_hex(&repo.op_id().hex()[..13]).unwrap();
    assert_eq!(
        op_store.resolve_operation_id_prefix(&odd).await.unwrap(),
        PrefixResolution::SingleMatch(repo.op_id().clone())
    );
    let root_op = HexPrefix::try_from_hex("0".repeat(64)).unwrap();
    assert_eq!(
        op_store
            .resolve_operation_id_prefix(&root_op)
            .await
            .unwrap(),
        PrefixResolution::SingleMatch(op_store.root_operation_id().clone())
    );
    let nothing = HexPrefix::try_from_hex("f".repeat(40)).unwrap();
    assert_eq!(
        op_store
            .resolve_operation_id_prefix(&nothing)
            .await
            .unwrap(),
        PrefixResolution::NoMatch
    );

    op_store
        .gc(&[repo.op_id().clone()], SystemTime::now())
        .await
        .unwrap();
    assert_eq!(
        a.head().await.op_id(),
        repo.op_id(),
        "collection never takes a head"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operation_that_can_never_publish_is_quarantined() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "quarantine").await;
    let a = Machine::new(&endpoint, &remote).await;
    let repo = a.head().await;

    // An operation whose parent the SCM does not have (collected by `jj op abandon` elsewhere).
    let mut op = Operation::clone(repo.operation().store_operation());
    op.parents = vec![OperationId::new(noise(32, 9))];
    op.metadata.description = "orphan".into();
    let orphan = repo.op_store().write_operation(&op).await.unwrap();
    let err = a
        .loader
        .op_heads_store()
        .update_op_heads(&[repo.op_id().clone()], &orphan)
        .await
        .unwrap_err();
    let chain = error_chain(&err);
    assert!(chain.contains("can never be published"), "{chain}");
    assert_eq!(a.outbox_operations(), 0);
    assert!(
        a.repo_dir
            .join("store/quarantine")
            .join(format!(
                "{:02x}-{}",
                ok_encoding::Kind::Operation as u8,
                orphan.hex()
            ))
            .exists()
    );
    assert_eq!(a.op_heads().await, vec![repo.op_id().clone()]);

    // It no longer blocks anything.
    let mut tx = repo.start_transaction();
    let root = repo.store().root_commit();
    let c = child(&mut tx, &root, &[("f", Content::File(b"after"))], "after").await;
    tx.commit("after").await.unwrap();
    assert!(a.head().await.view().heads().contains(c.id()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_signed_commit_round_trips() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "signed").await;
    let a = Machine::new(&endpoint, &remote).await;
    let repo = a.head().await;
    let store = repo.store();

    let mut unsigned = jj_lib::backend::Commit::clone(store.root_commit().store_commit());
    unsigned.parents = vec![store.root_commit_id().clone()];
    unsigned.change_id = ChangeId::new(noise(16, 3));
    unsigned.description = "signed\n".into();
    let mut sign = |data: &[u8]| {
        Ok::<_, jj_lib::signing::SignError>(format!("sig over {} bytes", data.len()).into_bytes())
    };
    let (id, signed) = store
        .backend()
        .write_commit(unsigned.clone(), Some(&mut sign))
        .await
        .unwrap();
    let sig = signed.secure_sig.clone().unwrap();
    assert_eq!(sig.data, ok_encoding::signed_data(&unsigned).unwrap());

    let mut tx = repo.start_transaction();
    tx.repo_mut()
        .add_head(&store.get_commit_async(&id).await.unwrap())
        .await
        .unwrap();
    tx.commit("add signed").await.unwrap();

    let b = Machine::new(&endpoint, &remote).await;
    let on_b = b.head().await;
    let read = on_b.store().get_commit_async(&id).await.unwrap();
    assert_eq!(read.store_commit().secure_sig.as_ref(), Some(&sig));
    assert_eq!(**read.store_commit(), signed);
}

#[tokio::test(flavor = "multi_thread")]
async fn copies_are_detected_and_related() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "copies").await;
    let a = Machine::new(&endpoint, &remote).await;
    let repo = a.head().await;
    let root = repo.store().root_commit();
    let contents = noise(4096, 5);

    // Still staged: asking for copy records publishes what they need first.
    let mut tx = repo.start_transaction();
    let before = child(
        &mut tx,
        &root,
        &[("a.txt", Content::File(&contents))],
        "before",
    )
    .await;
    let after = child(
        &mut tx,
        &before,
        &[
            ("a.txt", Content::Absent),
            ("b.txt", Content::File(&contents)),
        ],
        "after",
    )
    .await;
    let records: Vec<_> = repo
        .store()
        .backend()
        .get_copy_records(None, before.id(), after.id())
        .unwrap()
        .collect()
        .await;
    let records: Vec<_> = records.into_iter().map(Result::unwrap).collect();
    assert_eq!(records.len(), 1, "{records:?}");
    let r = &records[0];
    assert_eq!(
        (
            r.source.as_internal_file_string(),
            r.target.as_internal_file_string()
        ),
        ("a.txt", "b.txt")
    );
    assert_eq!(
        (&r.source_commit, &r.target_commit),
        (before.id(), after.id())
    );
    tx.commit("rename").await.unwrap();

    let backend = repo.store().backend();
    let original = backend
        .write_copy(&CopyHistory {
            current_path: path("a.txt"),
            parents: vec![],
            salt: noise(8, 6),
        })
        .await
        .unwrap();
    let renamed = backend
        .write_copy(&CopyHistory {
            current_path: path("b.txt"),
            parents: vec![original.clone()],
            salt: vec![],
        })
        .await
        .unwrap();
    let related: HashSet<CopyId> = backend
        .get_related_copies(&renamed)
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert!(
        related.contains(&original) && related.contains(&renamed),
        "{related:?}"
    );
}

// A virtual workspace's reads (docs/okfs.md): a cold machine reads a file's manifest without
// its contents, then a range from its middle, fetching only the chunks the range covers; a read
// of the whole file afterwards fetches the rest, and every byte is the file's.
#[tokio::test(flavor = "multi_thread")]
async fn manifests_and_ranges_read_only_what_they_cover() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "ranges").await;
    let a = Machine::new(&endpoint, &remote).await;
    let repo = a.head().await;
    let big = noise(3 << 20, 7);
    let text = text_lines("a compressible line, read by range", 40_000);
    let mut tx = repo.start_transaction();
    let root = repo.store().root_commit();
    let first = child(
        &mut tx,
        &root,
        &[
            ("big.bin", Content::File(&big)),
            ("small", Content::File(b"tiny\n")),
            ("text.txt", Content::File(&text)),
        ],
        "big",
    )
    .await;
    tx.commit("big").await.unwrap();
    let file_id = |c: &Commit, p: &str| {
        let c = c.clone();
        let p = p.to_owned();
        async move {
            match c
                .tree()
                .path_value(&path(&p))
                .await
                .unwrap()
                .into_resolved()
                .unwrap()
                .unwrap()
            {
                TreeValue::File { id, .. } => <[u8; 32]>::try_from(id.as_bytes()).unwrap(),
                v => panic!("{p}: {v:?}"),
            }
        }
    };
    let big_id = file_id(&first, "big.bin").await;
    let small_id = file_id(&first, "small").await;
    let text_id = file_id(&first, "text.txt").await;

    let b = Machine::new(&endpoint, &remote).await;
    let shared = ok_store::Shared::for_store_path(&b.repo_dir.join("store")).unwrap();
    let chunks_cached = || shared.cache.count(ok_encoding::Kind::Chunk as u8);
    let (m, small) = {
        let shared = shared.clone();
        ok_store::shared::run(async move {
            let m = shared.manifest(big_id).await.unwrap().unwrap();
            let small = shared.manifest(small_id).await.unwrap().unwrap();
            (m, small)
        })
        .await
    };
    assert_eq!(m.size, big.len() as u64);
    assert!(m.chunks.len() > 10, "a 3 MiB file is many chunks");
    assert_eq!(m.chunks.iter().map(|c| u64::from(c.1)).sum::<u64>(), m.size);
    assert_eq!(small.size, 5);
    assert_eq!(chunks_cached(), 0, "a manifest brings no contents");

    let (from, len) = (1_500_000u64, 100_000u64);
    let got = {
        let shared = shared.clone();
        ok_store::shared::run(async move { shared.read_range(big_id, from, len).await.unwrap() })
            .await
    };
    assert_eq!(got, big[from as usize..(from + len) as usize]);
    let after_range = chunks_cached();
    assert!(
        after_range >= 1 && after_range < m.chunks.len() / 2,
        "{after_range} of {} chunks",
        m.chunks.len()
    );
    // Past the end, and across it.
    let tail = {
        let shared = shared.clone();
        ok_store::shared::run(async move {
            shared
                .read_range(big_id, (3 << 20) - 10, 100)
                .await
                .unwrap()
        })
        .await
    };
    assert_eq!(tail, big[big.len() - 10..]);
    let none = {
        let shared = shared.clone();
        ok_store::shared::run(async move { shared.read_range(big_id, 4 << 20, 10).await.unwrap() })
            .await
    };
    assert!(none.is_empty());
    // A range of a compressible file: its chunks arrive zstd-compressed, as stored.
    let (from, len) = (700_000u64, 300_000u64);
    let got = {
        let shared = shared.clone();
        ok_store::shared::run(async move { shared.read_range(text_id, from, len).await.unwrap() })
            .await
    };
    assert_eq!(got, text[from as usize..(from + len) as usize]);
    // The whole file, through jj's read, now reads the rest.
    let on_b = b.head().await;
    assert_eq!(read(&on_b, first.id(), "big.bin").await, Some(big));
}

// What the client asks ok is counted (jj/store/calls.rs): every RPC by method, with its
// bytes and whether it failed, every batched fetch by kind, and the reads that waited on ok. The
// counts are the process's, shared with the tests running beside this one, so it compares before
// and after.
#[tokio::test(flavor = "multi_thread")]
async fn the_client_counts_what_it_asks_the_scm() {
    if !ok_testscm::available() {
        return;
    }
    use ok_store::calls;
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "counted").await;
    let a = Machine::new(&endpoint, &remote).await;
    let repo = a.head().await;
    let big = noise(1 << 20, 11);
    let mut tx = repo.start_transaction();
    let root = repo.store().root_commit();
    let first = child(
        &mut tx,
        &root,
        &[("big.bin", Content::File(&big))],
        "counted",
    )
    .await;
    tx.commit("counted").await.unwrap();
    let big_id = match first
        .tree()
        .path_value(&path("big.bin"))
        .await
        .unwrap()
        .into_resolved()
        .unwrap()
        .unwrap()
    {
        TreeValue::File { id, .. } => <[u8; 32]>::try_from(id.as_bytes()).unwrap(),
        v => panic!("big.bin: {v:?}"),
    };

    let calls_before = calls::calls();
    let fetches_before = calls::fetches();
    let b = Machine::new(&endpoint, &remote).await;
    let shared = ok_store::Shared::for_store_path(&b.repo_dir.join("store")).unwrap();
    assert_eq!(
        shared.waits().reads,
        0,
        "a repository just opened has waited for nothing"
    );
    let got = {
        let shared = shared.clone();
        ok_store::shared::run(async move {
            shared.manifest(big_id).await.unwrap().unwrap();
            shared.read_range(big_id, 1000, 200_000).await.unwrap()
        })
        .await
    };
    assert_eq!(got, big[1000..201_000]);
    let waits = shared.waits();
    assert!(
        waits.reads >= 2,
        "the manifest and the range each waited: {waits:?}"
    );
    assert!(
        waits.seconds > 0.0 && waits.max_seconds <= waits.seconds,
        "{waits:?}"
    );

    // A refused call is a failure.
    let missing = {
        let shared = shared.clone();
        ok_store::shared::run(async move {
            let mut repos = shared.client.repositories().await.unwrap();
            repos
                .get_repository(ok_store::pb::GetRepositoryRequest {
                    key: Some(ok_store::pb::get_repository_request::Key::Name(
                        "no-such-repository-counted".into(),
                    )),
                })
                .await
        })
        .await
    };
    assert_eq!(missing.unwrap_err().code(), tonic::Code::NotFound);

    let after = calls::calls();
    let delta = |method: &str| {
        let was = calls_before.get(method).cloned().unwrap_or_default();
        let now = after.get(method).cloned().unwrap_or_default();
        (
            now.calls - was.calls,
            now.failures - was.failures,
            now.received_bytes - was.received_bytes,
            now.sent_bytes - was.sent_bytes,
        )
    };
    let (n, failed, received, sent) = delta("Store/ReadFiles");
    assert!(
        n >= 2,
        "ReadFiles: {n} calls, want the manifest's and the range's"
    );
    assert_eq!(failed, 0, "ReadFiles failed");
    assert!(
        received >= 100_000,
        "ReadFiles received {received} bytes for a 200 KB range of noise"
    );
    assert!(sent > 0, "ReadFiles sent nothing");
    let (n, failed, ..) = delta("Repositories/GetRepository");
    assert!(
        n >= 1 && failed >= 1,
        "GetRepository: {n} calls, {failed} failed"
    );
    let longest = after["Store/ReadFiles"].max_seconds;
    assert!(longest > 0.0 && longest <= after["Store/ReadFiles"].seconds);

    let fetches = calls::fetches();
    for kind in ["manifests", "chunks"] {
        let was = fetches_before.get(kind).cloned().unwrap_or_default();
        let now = fetches.get(kind).cloned().unwrap_or_default();
        assert!(
            now.batches > was.batches && now.keys > was.keys,
            "{kind}: {was:?} -> {now:?}"
        );
    }
}

// Tree sizes (docs/scm.md, "Tree sizes"): a cold machine's virtual checkout receives every
// directory's sizes with its trees, in one closure, and a presentation's read-ahead then learns
// every file's size without asking for any file's manifest. The sizes are the files' own.
#[tokio::test(flavor = "multi_thread")]
async fn a_virtual_checkout_learns_sizes_a_directory_at_a_time() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "sizes").await;
    let a = Machine::new(&endpoint, &remote).await;
    let repo = a.head().await;
    let big = noise(700_000, 3);
    let mut tx = repo.start_transaction();
    let root = repo.store().root_commit();
    let first = child(
        &mut tx,
        &root,
        &[
            ("a.txt", Content::File(b"hello\n")),
            ("dir/big.bin", Content::File(&big)),
            ("dir/deeper/empty", Content::File(b"")),
        ],
        "sizes",
    )
    .await;
    tx.commit("sizes").await.unwrap();
    let tree_id = first.tree_ids().as_resolved().unwrap().clone();

    let b = Machine::new(&endpoint, &remote).await;
    let shared = ok_store::Shared::for_store_path(&b.repo_dir.join("store")).unwrap();
    ok_store::prefetch::trees(shared.clone(), &tree_id)
        .await
        .unwrap();
    // Every tree's record is local, brought by the closure, with the files' sizes in the trees'
    // order.
    let root_id: [u8; 32] = tree_id.as_bytes().try_into().unwrap();
    let tree = first.tree();
    let dir_id = match tree
        .path_value(&path("dir"))
        .await
        .unwrap()
        .into_resolved()
        .unwrap()
    {
        Some(TreeValue::Tree(id)) => <[u8; 32]>::try_from(id.as_bytes()).unwrap(),
        v => panic!("dir: {v:?}"),
    };
    let root_sizes = shared
        .cache
        .tree_sizes(&root_id)
        .expect("the root's record, from the closure");
    let dir_sizes = shared
        .cache
        .tree_sizes(&dir_id)
        .expect("dir's record, from the closure");
    assert_eq!(root_sizes.sizes, [6, 0], "a.txt, then dir");
    assert_eq!(
        dir_sizes.sizes,
        [big.len() as u64, 0],
        "big.bin, then deeper"
    );

    // A presentation's read-ahead of nothing: every size from the records, no manifest read.
    let wanted: ok_store::prefetch::Wanted = Arc::new(|_| false);
    let got = ok_store::prefetch::tree_and_files(shared.clone(), &tree_id, wanted)
        .await
        .unwrap();
    assert_eq!(got.files, 0);
    for p in ["a.txt", "dir/big.bin", "dir/deeper/empty"] {
        let id = match tree
            .path_value(&path(p))
            .await
            .unwrap()
            .into_resolved()
            .unwrap()
        {
            Some(TreeValue::File { id, .. }) => <[u8; 32]>::try_from(id.as_bytes()).unwrap(),
            v => panic!("{p}: {v:?}"),
        };
        assert!(
            shared.cache.manifest(&id).is_none(),
            "{p}'s manifest was read for its size"
        );
    }
}

/// The operations a machine's cache holds.
fn cached_operations(m: &Machine) -> usize {
    ok_store::Shared::for_store_path(&m.repo_dir.join("store"))
        .unwrap()
        .cache
        .count(ok_encoding::Kind::Operation as u8)
}

/// The machine's operation frontier: operations whose whole ancestry its cache received.
fn op_frontier(m: &Machine) -> Vec<Vec<u8>> {
    std::fs::read(m.repo_dir.parent().unwrap().join("cache/op_frontier"))
        .map(|b| b.chunks(32).map(<[u8]>::to_vec).collect())
        .unwrap_or_default()
}

// A long operation log reaches a cold machine in bulk: a missing operation brings its ancestors
// and their views (PrefetchOperations), a clone streams the whole log first, and the head joins
// the operation frontier. Operations another machine adds later arrive on top of it, and what
// this machine publishes joins it too.
#[tokio::test(flavor = "multi_thread")]
async fn an_operation_log_reaches_a_cold_machine_in_bulk() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "oplog").await;
    let a = Machine::new(&endpoint, &remote).await;
    let mut repo = a.head().await;
    const OPS: usize = 60;
    for i in 0..OPS {
        let mut tx = repo.start_transaction();
        let root = repo.store().root_commit();
        let text = format!("operation {i}\n");
        child(
            &mut tx,
            &root,
            &[("f", Content::File(text.as_bytes()))],
            &text,
        )
        .await;
        repo = tx.commit(text).await.unwrap();
    }
    let head = repo.op_id().to_bytes();
    assert_eq!(
        op_frontier(&a).first(),
        Some(&head),
        "a publish heads the frontier"
    );

    // A cold machine loading the head: jj walks the log, and the first miss brings it all.
    let b = Machine::new(&endpoint, &remote).await;
    assert_eq!(b.head().await.op_id(), repo.op_id());
    assert!(
        cached_operations(&b) >= OPS,
        "{} operations cached",
        cached_operations(&b)
    );
    assert_eq!(op_frontier(&b).first(), Some(&head));

    // A clone streams the log before jj reads it.
    let c = Machine::new(&endpoint, &remote).await;
    ok_store::prefetch::operation_log(&c.repo_dir.join("store"))
        .await
        .unwrap();
    assert!(
        cached_operations(&c) >= OPS,
        "{} operations cached",
        cached_operations(&c)
    );
    assert_eq!(op_frontier(&c).first(), Some(&head));
    assert_eq!(c.head().await.op_id(), repo.op_id());

    // Three more operations on the first machine: the second loads them over its frontier.
    for i in 0..3 {
        let mut tx = repo.start_transaction();
        let root = repo.store().root_commit();
        let text = format!("later {i}\n");
        child(
            &mut tx,
            &root,
            &[("f", Content::File(text.as_bytes()))],
            &text,
        )
        .await;
        repo = tx.commit(text).await.unwrap();
    }
    let on_b = b.head().await;
    assert_eq!(on_b.op_id(), repo.op_id());
    assert_eq!(op_frontier(&b).first(), Some(&repo.op_id().to_bytes()));
    assert!(cached_operations(&b) >= OPS + 3);
}

/// n numbered lines, each `<prefix> <i>`: text that compresses well.
fn text_lines(prefix: &str, n: usize) -> Vec<u8> {
    (0..n)
        .map(|i| format!("{prefix} {i}\n"))
        .collect::<String>()
        .into_bytes()
}

/// A small source file: what a repository's dictionary learns from.
fn go_file(i: usize) -> Vec<u8> {
    format!(
        "// Package p{i} does one thing.\npackage p{i}\n\nimport \"fmt\"\n\n// Get{i} returns the value at key {i}.\nfunc Get{i}(key string) (string, error) {{\n\tif key == \"\" {{\n\t\treturn \"\", fmt.Errorf(\"p{i}: an empty key\")\n\t}}\n\treturn fmt.Sprintf(\"%s={}\", key), nil\n}}\n",
        i * 7919
    )
    .into_bytes()
}

// Once ok scm trains a repository's dictionary and compacts with it, a cold machine reads its
// files as the SCM stores them — compressed with the dictionary, which it fetches once — and
// reads them again from its cache.
#[tokio::test(flavor = "multi_thread")]
async fn chunks_compressed_with_a_dictionary_reach_a_cold_machine() {
    if !ok_testscm::available() {
        return;
    }
    use ok_store::calls;
    let endpoint = endpoint();
    let remote = new_repository(&endpoint, "dictionaries").await;
    let a = Machine::new(&endpoint, &remote).await;
    let repo = a.head().await;
    let files: Vec<(String, Vec<u8>)> = (0..1100)
        .map(|i| (format!("src/p{i:04}.go"), go_file(i)))
        .collect();
    let changes: Vec<(&str, Content)> = files
        .iter()
        .map(|(p, c)| (p.as_str(), Content::File(c)))
        .collect();
    let mut tx = repo.start_transaction();
    let root = repo.store().root_commit();
    let commit = child(&mut tx, &root, &changes, "small files").await;
    tx.commit("small files").await.unwrap();

    let report = ok_testscm::gc(&remote.name);
    assert!(
        report.contains("trained dictionary") && report.contains("small chunks with dictionary"),
        "{report}"
    );

    let dictionary_reads = || {
        calls::calls()
            .get("Store/ReadDictionary")
            .map_or(0, |c| c.calls)
    };
    let before = dictionary_reads();
    let b = Machine::new(&endpoint, &remote).await;
    let head = b.head().await;
    for (p, c) in &files {
        assert_eq!(
            read(&head, commit.id(), p).await.as_deref(),
            Some(c.as_slice()),
            "{p}"
        );
    }
    assert_eq!(
        dictionary_reads() - before,
        1,
        "the dictionary is fetched once"
    );
    let shared = ok_store::Shared::for_store_path(&b.repo_dir.join("store")).unwrap();
    assert_eq!(shared.cache.count(0xd0), 1, "the dictionary is kept");
    // Read again, from the cache alone.
    let waited = shared.waits().reads;
    for (p, c) in files.iter().step_by(50) {
        assert_eq!(
            read(&head, commit.id(), p).await.as_deref(),
            Some(c.as_slice())
        );
    }
    assert_eq!(
        shared.waits().reads,
        waited,
        "a cached file asks ok nothing"
    );
}
