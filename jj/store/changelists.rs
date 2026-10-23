//! The SCM calls behind `jj cl`, `jj cl get`, and `jj sync` (docs/scm.md,
//! "Commands"), each run on the store runtime over a repository's shared state.

use std::sync::Arc;

use jj_lib::backend::ChangeId;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use ok_proto::ok::scm::v1 as pb;
use prost::Message as _;
use tonic::Code;
use tonic::Status;

use crate::Repository;
use crate::client;
use crate::ids::Id;
use crate::ids::id_of;
use crate::publish;
use crate::publish::PublishError;
use crate::publish::RpcStatus;
use crate::shared;
use crate::shared::Shared;

fn ids(commits: &[CommitId]) -> Result<Vec<Id>, Status> {
    commits
        .iter()
        .map(|c| id_of(c).ok_or_else(|| Status::invalid_argument("a commit ID is not 32 bytes")))
        .collect()
}

/// Publishes whatever the outbox holds, without moving the operation heads, so every
/// object the view references is on the SCM (an upload names only stored commits).
pub async fn flush(shared: &Arc<Shared>) -> Result<(), PublishError> {
    let _lock = shared.outbox.lock()?;
    if shared.outbox.list()?.is_empty() {
        return Ok(());
    }
    let shared = shared.clone();
    shared::run(async move { publish::publish(shared, None).await.map(|_| ()) }).await
}

/// The repository as the SCM has it now, with main's head.
pub async fn repository(shared: &Arc<Shared>) -> Result<Repository, Status> {
    let shared = shared.clone();
    shared::run(async move {
        let req = pb::GetRepositoryRequest {
            key: Some(pb::get_repository_request::Key::Id(
                shared.client.repository_id(),
            )),
        };
        shared
            .client
            .repositories()
            .await?
            .get_repository(req)
            .await?
            .into_inner()
            .try_into()
    })
    .await
}

/// Brings the repository, a fork, its upstream's newer work on the SCM; true when its main moved
/// to the upstream's.
pub async fn sync_fork(shared: &Arc<Shared>) -> Result<bool, Status> {
    let shared = shared.clone();
    shared::run(async move {
        let req = pb::SyncForkRequest {
            repository_id: shared.client.repository_id(),
            actor_id: String::new(),
        };
        let r = shared
            .client
            .repositories()
            .await?
            .sync_fork(req)
            .await?
            .into_inner();
        Ok(r.moved)
    })
    .await
}

/// Uploads commits, bottom of the stack first, as CLs.
pub async fn upload(
    shared: &Arc<Shared>,
    commits: &[CommitId],
    readiness: pb::upload_request::Readiness,
    reviewers: Vec<String>,
) -> Result<pb::UploadResponse, Status> {
    let commit_ids = ids(commits)?.iter().map(|i| i.to_vec()).collect();
    let shared = shared.clone();
    shared::run(async move {
        let req = pb::UploadRequest {
            repository_id: shared.client.repository_id(),
            commit_ids,
            readiness: readiness as i32,
            reviewers,
        };
        client::while_frozen(|| {
            let (shared, req) = (shared.clone(), req.clone());
            async move {
                Ok(shared
                    .client
                    .change_lists()
                    .await?
                    .upload(req)
                    .await?
                    .into_inner())
            }
        })
        .await
    })
    .await
}

/// One CL.
pub async fn get_change_list(shared: &Arc<Shared>, number: i64) -> Result<pb::ChangeList, Status> {
    let shared = shared.clone();
    shared::run(async move {
        let req = pb::GetChangeListRequest {
            repository_id: shared.client.repository_id(),
            number,
        };
        Ok(shared
            .client
            .change_lists()
            .await?
            .get_change_list(req)
            .await?
            .into_inner())
    })
    .await
}

/// The CLs of the given changes, for those that have one.
pub async fn lookup_changes(
    shared: &Arc<Shared>,
    changes: &[ChangeId],
) -> Result<Vec<pb::ChangeList>, Status> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }
    let change_ids = changes.iter().map(|c| c.as_bytes().to_vec()).collect();
    let shared = shared.clone();
    shared::run(async move {
        let req = pb::LookupChangesRequest {
            repository_id: shared.client.repository_id(),
            change_ids,
        };
        Ok(shared
            .client
            .change_lists()
            .await?
            .lookup_changes(req)
            .await?
            .into_inner()
            .change_lists)
    })
    .await
}

/// Streams the commits reachable from `want` and not from `have` into the cache, so jj
/// reads them locally. Returns how many it streamed.
pub async fn prefetch(
    shared: &Arc<Shared>,
    want: &[CommitId],
    have: &[CommitId],
) -> Result<usize, Status> {
    let (want, have) = (ids(want)?, ids(have)?);
    let shared = shared.clone();
    shared::run(async move {
        crate::prefetch::commits(&shared.client, &shared.cache, &want, &have, false, 0)
            .await
            .map(|c| c.len())
    })
    .await
}

/// The rejection a FAILED_PRECONDITION status carries in its details, if it carries one
/// of type `M` (named `ok.scm.v1.<name>`).
fn rejection<M: prost::Message + Default>(status: &Status, name: &str) -> Option<M> {
    if status.code() != Code::FailedPrecondition {
        return None;
    }
    let any = RpcStatus::decode(status.details())
        .ok()?
        .details
        .into_iter()
        .find(|a| a.type_url.ends_with(&format!("/ok.scm.v1.{name}")))?;
    M::decode(any.value.as_slice()).ok()
}

/// Why the SCM refused an upload, when it did.
pub fn upload_rejection(status: &Status) -> Option<pb::UploadRejection> {
    rejection(status, "UploadRejection")
}

/// Why the SCM refused a landing or candidate, when it did.
pub fn land_rejection(status: &Status) -> Option<pb::LandRejection> {
    rejection(status, "LandRejection")
}
