//! Locks of paths (docs/scm.md, "Locks"): a person locks paths of a repository so that no one
//! else's CL changing them lands while they work on them, each lock theirs for 18 hours from when
//! they took it, until they unlock it, a landing of theirs that changes its path releases it, or a
//! repository administrator breaks it.

use std::sync::Arc;

use tonic::Status;

use crate::pb;
use crate::shared;
use crate::shared::Shared;

/// Locks paths (repository-relative) for the caller, every one or none.
pub async fn lock(shared: &Arc<Shared>, paths: Vec<String>) -> Result<Vec<pb::FileLock>, Status> {
    let shared = shared.clone();
    shared::run(async move {
        let req = pb::LockRequest {
            repository_id: shared.client.repository_id(),
            paths,
        };
        Ok(shared
            .client
            .locks()
            .await?
            .lock(req)
            .await?
            .into_inner()
            .locks)
    })
    .await
}

/// Ends the caller's locks of paths; returns the paths they held.
pub async fn unlock(shared: &Arc<Shared>, paths: Vec<String>) -> Result<Vec<String>, Status> {
    let shared = shared.clone();
    shared::run(async move {
        let req = pb::UnlockRequest {
            repository_id: shared.client.repository_id(),
            paths,
        };
        Ok(shared
            .client
            .locks()
            .await?
            .unlock(req)
            .await?
            .into_inner()
            .unlocked)
    })
    .await
}

/// Ends whoever's locks of paths — a repository administrator's, in an emergency; returns the
/// live locks it ended.
pub async fn break_locks(
    shared: &Arc<Shared>,
    paths: Vec<String>,
) -> Result<Vec<pb::FileLock>, Status> {
    let shared = shared.clone();
    shared::run(async move {
        let req = pb::BreakLocksRequest {
            repository_id: shared.client.repository_id(),
            paths,
        };
        Ok(shared
            .client
            .locks()
            .await?
            .break_locks(req)
            .await?
            .into_inner()
            .broken)
    })
    .await
}

/// The repository's live locks of a path and everything under it ("": every one), by path.
pub async fn list(shared: &Arc<Shared>, prefix: String) -> Result<Vec<pb::FileLock>, Status> {
    let shared = shared.clone();
    shared::run(async move {
        let req = pb::ListLocksRequest {
            repository_id: shared.client.repository_id(),
            path_prefix: prefix,
        };
        Ok(shared
            .client
            .locks()
            .await?
            .list_locks(req)
            .await?
            .into_inner()
            .locks)
    })
    .await
}

/// Why the SCM refused a lock or an unlock, when it did: the live lock someone else holds.
pub fn rejection(status: &Status) -> Option<pb::LockRejection> {
    crate::changelists::rejection(status, "LockRejection")
}
