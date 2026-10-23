//! The `ok` commit backend.

use std::fmt;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use futures::stream::BoxStream;
use jj_lib::backend::Backend;
use jj_lib::backend::BackendError;
use jj_lib::backend::BackendResult;
use jj_lib::backend::ChangeId;
use jj_lib::backend::Commit;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyHistory;
use jj_lib::backend::CopyId;
use jj_lib::backend::CopyRecord;
use jj_lib::backend::FileId;
use jj_lib::backend::RelatedCopy;
use jj_lib::backend::SecureSig;
use jj_lib::backend::SigningFn;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Tree;
use jj_lib::backend::TreeId;
use jj_lib::backend::make_root_commit;
use jj_lib::index::Index;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use ok_encoding::Kind;
use ok_encoding::Object;
use ok_proto::ok::scm::v1 as pb;
use tonic::Code;
use tonic::Status;

use crate::chunk;
use crate::ids::Id;
use crate::ids::Key;
use crate::ids::ROOT_ID;
use crate::ids::id_of;
use crate::shared;
use crate::shared::Shared;
use crate::shared::from_wire;

/// How many reads jj may have in flight: enough for the batched reads to fill up.
const CONCURRENCY: usize = 64;

pub struct OkBackend {
    shared: Arc<Shared>,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl fmt::Debug for OkBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OkBackend")
            .field("repository", &self.shared.config.repository)
            .finish_non_exhaustive()
    }
}

impl OkBackend {
    pub fn load(store_path: &Path) -> io::Result<Self> {
        Ok(Self {
            shared: Shared::for_store_path(store_path)?,
            root_commit_id: CommitId::new(ROOT_ID.to_vec()),
            root_change_id: ChangeId::new(vec![0; ok_encoding::CHANGE_ID_LEN]),
            empty_tree_id: TreeId::new(ok_encoding::empty_tree_id().to_vec()),
        })
    }

    /// The shared state, for the commands that prefetch and publish.
    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// Reads and decodes an object, mapping failures to jj's errors.
    async fn read(
        &self,
        kind: Kind,
        id: &impl jj_lib::object_id::ObjectId,
    ) -> BackendResult<Object> {
        let key = Key::new(kind, id_of(id).ok_or_else(|| hash_length(kind, id))?);
        let shared = self.shared.clone();
        let bytes = shared::run(async move { shared.object(key).await })
            .await
            .map_err(|s| read_error(kind, &key.hex(), s))?
            .ok_or_else(|| not_found(kind, &key.hex()))?;
        ok_encoding::decode(&bytes).map_err(|e| BackendError::ReadObject {
            object_type: kind.name().into(),
            hash: key.hex(),
            source: e.into(),
        })
    }

    /// Stages an object's canonical bytes and returns its ID.
    fn write(&self, kind: Kind, canonical: &[u8]) -> BackendResult<Id> {
        let id = ok_encoding::hash(canonical);
        self.shared
            .outbox
            .stage_object(Key::new(kind, id), canonical)
            .map_err(|e| write_error(kind, e))?;
        Ok(id)
    }
}

pub fn not_found(kind: Kind, hash: &str) -> BackendError {
    BackendError::ObjectNotFound {
        object_type: kind.name().into(),
        hash: hash.into(),
        source: "the SCM does not have it".into(),
    }
}

fn hash_length(kind: Kind, id: &impl jj_lib::object_id::ObjectId) -> BackendError {
    BackendError::InvalidHashLength {
        expected: ok_encoding::ID_LEN,
        actual: id.as_bytes().len(),
        object_type: kind.name().into(),
        hash: id.hex(),
    }
}

/// Maps a failed read (docs/scm.md, "Protocol": NOT_FOUND is ObjectNotFound,
/// PERMISSION_DENIED is ReadAccessDenied, anything else is Other).
pub fn read_error(kind: Kind, hash: &str, s: Status) -> BackendError {
    match s.code() {
        Code::NotFound => BackendError::ObjectNotFound {
            object_type: kind.name().into(),
            hash: hash.into(),
            source: s.into(),
        },
        Code::PermissionDenied => BackendError::ReadAccessDenied {
            object_type: kind.name().into(),
            hash: hash.into(),
            source: s.into(),
        },
        _ => {
            BackendError::Other(format!("reading {} {hash} from the SCM: {s}", kind.name()).into())
        }
    }
}

fn write_error(kind: Kind, e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> BackendError {
    BackendError::WriteObject {
        object_type: kind.name(),
        source: e.into(),
    }
}

fn encode_error(kind: Kind, e: ok_encoding::Error) -> BackendError {
    write_error(kind, e)
}

#[async_trait]
impl Backend for OkBackend {
    fn name(&self) -> &str {
        crate::NAME
    }

    fn commit_id_length(&self) -> usize {
        ok_encoding::ID_LEN
    }

    fn change_id_length(&self) -> usize {
        ok_encoding::CHANGE_ID_LEN
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        CONCURRENCY
    }

    async fn read_file(
        &self,
        path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let fid = id_of(id).ok_or_else(|| hash_length(Kind::File, id))?;
        let shared = self.shared.clone();
        let manifest = shared::run(async move { shared.file(fid).await })
            .await
            .map_err(|s| read_error(Kind::File, &id.hex(), s))?
            .ok_or_else(|| not_found(Kind::File, &id.hex()))?;
        let shared = self.shared.clone();
        let (path, fid_hex) = (path.to_owned(), id.clone());
        let chunks = futures::stream::iter(manifest.chunks).map(move |(cid, _)| {
            shared.chunk(&cid).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "chunk {} of file {} at {path:?} is no longer local",
                        hex::encode(cid),
                        fid_hex.hex()
                    ),
                )
            })
        });
        Ok(Box::pin(chunks.into_async_read()))
    }

    async fn write_file(
        &self,
        path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let p = path.as_internal_file_string();
        let keeps = ok_secret::is_secret(p) && {
            let shared = self.shared.clone();
            shared::run(async move { shared.keeps_secrets().await }).await
        };
        let id = if keeps {
            // A secret's value is never stored (docs/secrets.md): only its pointer.
            let mut head = Vec::with_capacity(ok_secret::POINTER_LEN + 1);
            futures::AsyncReadExt::read_to_end(
                &mut futures::AsyncReadExt::take(&mut *contents, ok_secret::POINTER_LEN as u64 + 1),
                &mut head,
            )
            .await
            .map_err(|e| write_error(Kind::File, e))?;
            if ok_secret::parse_pointer(&head).is_none() {
                head.fill(0);
                return Err(write_error(
                    Kind::File,
                    format!(
                        "{p} holds a value, not a pointer: ok's jj never stores a secret's value. \
                         Write it with jj secret write //{p}, or in a virtual workspace, which keep \
                         it in ok secrets; or restore its pointer (jj restore {p}), or delete it \
                         (docs/secrets.md)"
                    ),
                ));
            }
            chunk::write_file(&self.shared, p, &mut futures::io::Cursor::new(head)).await
        } else {
            chunk::write_file(&self.shared, p, contents).await
        }
        .map_err(|e| write_error(Kind::File, e))?;
        Ok(FileId::new(id.to_vec()))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        match self.read(Kind::Symlink, id).await? {
            Object::Symlink(target) => Ok(target),
            _ => Err(not_found(Kind::Symlink, &id.hex())),
        }
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let id = self.write(Kind::Symlink, &ok_encoding::encode_symlink(target))?;
        Ok(SymlinkId::new(id.to_vec()))
    }

    async fn read_copy(&self, id: &CopyId) -> BackendResult<CopyHistory> {
        match self.read(Kind::CopyHistory, id).await? {
            Object::CopyHistory(c) => Ok(c),
            _ => Err(not_found(Kind::CopyHistory, &id.hex())),
        }
    }

    async fn write_copy(&self, copy: &CopyHistory) -> BackendResult<CopyId> {
        let canonical = ok_encoding::encode_copy_history(copy)
            .map_err(|e| encode_error(Kind::CopyHistory, e))?;
        Ok(CopyId::new(
            self.write(Kind::CopyHistory, &canonical)?.to_vec(),
        ))
    }

    async fn get_related_copies(&self, copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        let cid = id_of(copy_id).ok_or_else(|| hash_length(Kind::CopyHistory, copy_id))?;
        // A copy history not yet published is related only to what is published: its own
        // ancestors. Publish what is staged first so the server answers for all of it.
        if self
            .shared
            .outbox
            .object(Key::new(Kind::CopyHistory, cid))
            .is_some()
        {
            self.publish_objects().await?;
        }
        let shared = self.shared.clone();
        let copies = shared::run(async move {
            let mut store = shared.client.store().await?;
            let resp = store
                .get_related_copies(pb::GetRelatedCopiesRequest {
                    repository_id: shared.client.repository_id(),
                    copy_id: cid.to_vec(),
                })
                .await?;
            resp.into_inner()
                .copies
                .into_iter()
                .map(from_wire)
                .collect::<Result<Vec<_>, Status>>()
        })
        .await
        .map_err(|s| read_error(Kind::CopyHistory, &copy_id.hex(), s))?;
        copies
            .into_iter()
            .map(|(key, canonical)| match ok_encoding::decode(&canonical) {
                Ok(Object::CopyHistory(history)) => Ok(RelatedCopy {
                    id: CopyId::new(key.id.to_vec()),
                    history,
                }),
                _ => Err(BackendError::Other(
                    format!("copy history {} from the SCM does not decode", key.hex()).into(),
                )),
            })
            .collect()
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::default());
        }
        match self.read(Kind::Tree, id).await? {
            Object::Tree(tree) => Ok(tree),
            _ => Err(not_found(Kind::Tree, &id.hex())),
        }
    }

    async fn write_tree(&self, _path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        let canonical =
            ok_encoding::encode_tree(contents).map_err(|e| encode_error(Kind::Tree, e))?;
        let id = ok_encoding::hash(&canonical);
        // The empty tree is virtual on both sides and never published.
        if id != ok_encoding::empty_tree_id() {
            self.shared
                .outbox
                .stage_object(Key::new(Kind::Tree, id), &canonical)
                .map_err(|e| write_error(Kind::Tree, e))?;
        }
        Ok(TreeId::new(id.to_vec()))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id.clone(),
                self.empty_tree_id.clone(),
            ));
        }
        match self.read(Kind::Commit, id).await? {
            Object::Commit(commit) => Ok(commit),
            _ => Err(not_found(Kind::Commit, &id.hex())),
        }
    }

    async fn write_commit(
        &self,
        mut commit: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        if commit.secure_sig.is_some() {
            return Err(BackendError::Other(
                "a commit to write already carries a signature".into(),
            ));
        }
        if commit.parents.is_empty() {
            return Err(BackendError::Other(
                "cannot write a commit with no parents".into(),
            ));
        }
        if let Some(sign) = sign_with {
            let data =
                ok_encoding::signed_data(&commit).map_err(|e| encode_error(Kind::Commit, e))?;
            let sig = sign(&data).map_err(|e| BackendError::Other(e.into()))?;
            commit.secure_sig = Some(SecureSig { data, sig });
        }
        let canonical =
            ok_encoding::encode_commit(&commit).map_err(|e| encode_error(Kind::Commit, e))?;
        let id = self.write(Kind::Commit, &canonical)?;
        Ok((CommitId::new(id.to_vec()), commit))
    }

    fn get_copy_records(
        &self,
        paths: Option<&[RepoPathBuf]>,
        root: &CommitId,
        head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        let root_id = id_of(root).ok_or_else(|| hash_length(Kind::Commit, root))?;
        let head_id = id_of(head).ok_or_else(|| hash_length(Kind::Commit, head))?;
        let req = pb::GetCopyRecordsRequest {
            repository_id: self.shared.client.repository_id(),
            all_paths: paths.is_none(),
            paths: paths
                .unwrap_or_default()
                .iter()
                .map(|p| p.as_internal_file_string().to_owned())
                .collect(),
            root: root_id.to_vec(),
            head: head_id.to_vec(),
        };
        let records = async move {
            // The server diffs the two commits' trees, so they must be published.
            let staged = |id| {
                self.shared
                    .outbox
                    .object(Key::new(Kind::Commit, id))
                    .is_some()
            };
            if staged(root_id) || staged(head_id) {
                self.publish_objects().await?;
            }
            let shared = self.shared.clone();
            let records = shared::run(async move {
                let mut store = shared.client.store().await?;
                store
                    .get_copy_records(req)
                    .await?
                    .into_inner()
                    .try_collect::<Vec<pb::CopyRecord>>()
                    .await
            })
            .await
            .map_err(|s| read_error(Kind::Commit, &hex::encode(head_id), s))?;
            records
                .into_iter()
                .map(copy_record)
                .collect::<BackendResult<Vec<_>>>()
        };
        Ok(futures::stream::once(records)
            .map(|r| {
                futures::stream::iter(match r {
                    Ok(records) => records.into_iter().map(Ok).collect::<Vec<_>>(),
                    Err(e) => vec![Err(e)],
                })
            })
            .flatten()
            .boxed())
    }

    fn gc(&self, _index: &dyn Index, _keep_newer: SystemTime) -> BackendResult<()> {
        // The SCM collects unreachable objects itself (docs/scm.md, "GC").
        Ok(())
    }
}

impl OkBackend {
    /// Publishes everything staged, without moving the operation heads: the objects become
    /// known to the server, and stay invisible until an operation references them (or a okfs
    /// checkpoint names them).
    pub async fn publish_objects(&self) -> BackendResult<()> {
        let _lock = self
            .shared
            .outbox
            .lock()
            .map_err(|e| BackendError::Other(e.into()))?;
        let shared = self.shared.clone();
        shared::run(async move { crate::publish::publish(shared, None).await })
            .await
            .map(|_| ())
            .map_err(|e| BackendError::Other(e.into()))
    }
}

fn copy_record(r: pb::CopyRecord) -> BackendResult<CopyRecord> {
    let path =
        |p: &str| RepoPathBuf::from_internal_string(p).map_err(|e| BackendError::Other(e.into()));
    let id = |b: &[u8], kind: Kind| -> BackendResult<Vec<u8>> {
        (b.len() == ok_encoding::ID_LEN)
            .then(|| b.to_vec())
            .ok_or_else(|| {
                BackendError::Other(
                    format!("a copy record's {} ID is not 32 bytes", kind.name()).into(),
                )
            })
    };
    Ok(CopyRecord {
        target: path(&r.target)?,
        target_commit: CommitId::new(id(&r.target_commit, Kind::Commit)?),
        source: path(&r.source)?,
        source_file: FileId::new(id(&r.source_file, Kind::File)?),
        source_commit: CommitId::new(id(&r.source_commit, Kind::Commit)?),
    })
}
