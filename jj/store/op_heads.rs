//! The `ok` operation heads store: the caller's operation heads on the SCM.

use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_heads_store::OpHeadsStoreError;
use jj_lib::op_heads_store::OpHeadsStoreLock;
use jj_lib::op_store::OperationId;
use ok_proto::ok::scm::v1 as pb;

use crate::ids::Id;
use crate::ids::id_from;
use crate::ids::id_of;
use crate::publish;
use crate::publish::Heads;
use crate::shared;
use crate::shared::Shared;

pub struct OkOpHeadsStore {
    shared: Arc<Shared>,
}

impl fmt::Debug for OkOpHeadsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OkOpHeadsStore")
            .field("repository", &self.shared.config.repository)
            .finish_non_exhaustive()
    }
}

impl OkOpHeadsStore {
    pub fn load(store_path: &Path) -> io::Result<Self> {
        Ok(Self {
            shared: Shared::for_store_path(store_path)?,
        })
    }
}

/// The lock jj takes around resolving divergent operations. It holds nothing: jj documents
/// the lock as unnecessary for correctness (`op_heads_store.rs`) — it only keeps two
/// processes from resolving the same divergence, and heads that diverge anyway are resolved
/// on the next load — and a real lock would cost a round trip per transaction.
struct NoLock;

impl OpHeadsStoreLock for NoLock {}

#[async_trait]
impl OpHeadsStore for OkOpHeadsStore {
    fn name(&self) -> &str {
        crate::NAME
    }

    /// Publishes the whole outbox — the transaction's objects, its view and operation, and
    /// anything an earlier command staged — and swaps the heads, in one server transaction
    /// (or, for a large outbox, object-only publishes and then that one).
    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let write_error =
            |source: Box<dyn std::error::Error + Send + Sync>| OpHeadsStoreError::Write {
                new_op_id: new_id.clone(),
                source,
            };
        let old: Vec<Id> = old_ids
            .iter()
            .map(|o| id_of(o).ok_or_else(|| write_error("an operation ID is not 32 bytes".into())))
            .collect::<Result<_, _>>()?;
        let new =
            id_of(new_id).ok_or_else(|| write_error("an operation ID is not 32 bytes".into()))?;
        let _lock = self
            .shared
            .outbox
            .lock()
            .map_err(|e| write_error(e.into()))?;
        let shared = self.shared.clone();
        shared::run(async move { publish::publish(shared, Some(Heads { old, new })).await })
            .await
            .map_err(|e| write_error(e.into()))?;
        // The new operation was made from operations this process loaded: its ancestry is local.
        self.shared
            .cache
            .add_op_frontier(&[new])
            .map_err(|e| write_error(e.into()))
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let shared = self.shared.clone();
        let heads = shared::run(async move {
            let mut store = shared.client.store().await?;
            store
                .get_op_heads(pb::GetOpHeadsRequest {
                    repository_id: shared.client.repository_id(),
                    workspace: shared.client.workspace(),
                })
                .await
                .map(|r| r.into_inner().op_ids)
        })
        .await
        .map_err(|s| OpHeadsStoreError::Read(s.into()))?;
        heads
            .iter()
            .map(|h| {
                id_from(h)
                    .map(|id| OperationId::new(id.to_vec()))
                    .ok_or_else(|| {
                        OpHeadsStoreError::Read("the SCM returned a malformed operation ID".into())
                    })
            })
            .collect()
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(NoLock))
    }
}
