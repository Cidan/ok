//! The `ok` operation store: the caller's own operation log on the SCM.

use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::ObjectId as _;
use jj_lib::object_id::PrefixResolution;
use jj_lib::op_store::OpStore;
use jj_lib::op_store::OpStoreError;
use jj_lib::op_store::OpStoreResult;
use jj_lib::op_store::Operation;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::RootOperationData;
use jj_lib::op_store::View;
use jj_lib::op_store::ViewId;
use ok_encoding::Kind;
use ok_encoding::Object;
use ok_proto::ok::scm::v1 as pb;
use tonic::Code;
use tonic::Status;

use crate::ids::Key;
use crate::ids::ROOT_ID;
use crate::ids::id_from;
use crate::ids::id_of;
use crate::shared;
use crate::shared::Shared;

pub struct OkOpStore {
    shared: Arc<Shared>,
    root_data: RootOperationData,
    root_operation_id: OperationId,
    root_view_id: ViewId,
}

impl fmt::Debug for OkOpStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OkOpStore")
            .field("repository", &self.shared.config.repository)
            .finish_non_exhaustive()
    }
}

impl OkOpStore {
    pub fn load(store_path: &Path, root_data: RootOperationData) -> io::Result<Self> {
        Ok(Self {
            shared: Shared::for_store_path(store_path)?,
            root_data,
            root_operation_id: OperationId::new(ROOT_ID.to_vec()),
            root_view_id: ViewId::new(ROOT_ID.to_vec()),
        })
    }

    async fn read(
        &self,
        kind: Kind,
        id: &impl jj_lib::object_id::ObjectId,
    ) -> OpStoreResult<Object> {
        let hash = id.hex();
        let key = Key::new(
            kind,
            id_of(id).ok_or_else(|| not_found(kind, &hash, "an ID is not 32 bytes".into()))?,
        );
        let shared = self.shared.clone();
        let bytes = shared::run(async move { shared.object(key).await })
            .await
            .map_err(|s| read_error(kind, &hash, s))?
            .ok_or_else(|| not_found(kind, &hash, "not in this operation log".into()))?;
        ok_encoding::decode(&bytes).map_err(|e| OpStoreError::ReadObject {
            object_type: kind.name().into(),
            hash,
            source: e.into(),
        })
    }

    fn write(&self, kind: Kind, canonical: &[u8]) -> OpStoreResult<Vec<u8>> {
        let id = ok_encoding::hash(canonical);
        self.shared
            .outbox
            .stage_object(Key::new(kind, id), canonical)
            .map_err(|e| OpStoreError::WriteObject {
                object_type: kind.name(),
                source: e.into(),
            })?;
        Ok(id.to_vec())
    }
}

fn not_found(
    kind: Kind,
    hash: &str,
    source: Box<dyn std::error::Error + Send + Sync>,
) -> OpStoreError {
    OpStoreError::ObjectNotFound {
        object_type: kind.name().into(),
        hash: hash.into(),
        source,
    }
}

/// Maps a failed read (docs/scm.md, "Protocol").
fn read_error(kind: Kind, hash: &str, s: Status) -> OpStoreError {
    match s.code() {
        Code::NotFound => not_found(kind, hash, s.into()),
        Code::PermissionDenied => OpStoreError::ReadObject {
            object_type: kind.name().into(),
            hash: hash.into(),
            source: s.into(),
        },
        _ => {
            OpStoreError::Other(format!("reading {} {hash} from the SCM: {s}", kind.name()).into())
        }
    }
}

#[async_trait]
impl OpStore for OkOpStore {
    fn name(&self) -> &str {
        crate::NAME
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        if *id == self.root_view_id {
            return Ok(View::make_root(self.root_data.root_commit_id.clone()));
        }
        match self.read(Kind::View, id).await? {
            Object::View(view) => Ok(view),
            _ => Err(not_found(Kind::View, &id.hex(), "not a view".into())),
        }
    }

    async fn write_view(&self, contents: &View) -> OpStoreResult<ViewId> {
        let canonical =
            ok_encoding::encode_view(contents).map_err(|e| OpStoreError::WriteObject {
                object_type: "view",
                source: e.into(),
            })?;
        Ok(ViewId::new(self.write(Kind::View, &canonical)?))
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        if *id == self.root_operation_id {
            return Ok(Operation::make_root(self.root_view_id.clone()));
        }
        match self.read(Kind::Operation, id).await? {
            Object::Operation(op) => Ok(op),
            _ => Err(not_found(
                Kind::Operation,
                &id.hex(),
                "not an operation".into(),
            )),
        }
    }

    async fn write_operation(&self, contents: &Operation) -> OpStoreResult<OperationId> {
        let canonical =
            ok_encoding::encode_operation(contents).map_err(|e| OpStoreError::WriteObject {
                object_type: "operation",
                source: e.into(),
            })?;
        Ok(OperationId::new(self.write(Kind::Operation, &canonical)?))
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        // The server's answer, plus the operations still in the outbox (jj can leave one
        // unpublished, e.g. with --no-integrate-operation).
        let shared = self.shared.clone();
        let bytes = prefix.min_prefix_bytes().to_vec();
        let odd = prefix.as_full_bytes().is_none();
        let resp = shared::run(async move {
            let mut store = shared.client.store().await?;
            store
                .resolve_operation_prefix(pb::ResolveOperationPrefixRequest {
                    repository_id: shared.client.repository_id(),
                    prefix: bytes,
                    odd_nibble: odd,
                    workspace: shared.client.workspace(),
                })
                .await
                .map(|r| r.into_inner())
        })
        .await
        .map_err(|s| {
            OpStoreError::Other(format!("resolving operation prefix {}: {s}", prefix.hex()).into())
        })?;
        use pb::resolve_operation_prefix_response::Resolution;
        let remote = match Resolution::try_from(resp.resolution) {
            Ok(Resolution::Single) => match id_from(&resp.op_id) {
                Some(id) => PrefixResolution::SingleMatch(OperationId::new(id.to_vec())),
                None => {
                    return Err(OpStoreError::Other(
                        "the SCM resolved a prefix to a malformed ID".into(),
                    ));
                }
            },
            Ok(Resolution::Ambiguous) => PrefixResolution::AmbiguousMatch,
            _ => PrefixResolution::NoMatch,
        };
        let mut result = remote;
        for local in self.shared.outbox.operations() {
            let id = OperationId::new(local.to_vec());
            if !prefix.matches(&id) {
                continue;
            }
            result = match result {
                PrefixResolution::SingleMatch(ref existing) if *existing == id => result,
                other => other.plus(&PrefixResolution::SingleMatch(id)),
            };
        }
        Ok(result)
    }

    async fn gc(&self, head_ids: &[OperationId], keep_newer: SystemTime) -> OpStoreResult<()> {
        let heads = head_ids.iter().map(|h| h.to_bytes()).collect();
        let keep_newer = prost_types::Timestamp::from(keep_newer);
        let shared = self.shared.clone();
        shared::run(async move {
            let mut store = shared.client.store().await?;
            store
                .collect_operations(pb::CollectOperationsRequest {
                    repository_id: shared.client.repository_id(),
                    head_ids: heads,
                    keep_newer: Some(keep_newer),
                    workspace: shared.client.workspace(),
                })
                .await
                .map(|_| ())
        })
        .await
        .map_err(|s| OpStoreError::Other(format!("collecting operations: {s}").into()))
    }
}
