//! ok's jj stores (docs/scm.md, "The client"): jj's `Backend`, `OpStore`, and
//! `OpHeadsStore` over ok's SCM.
//!
//! A repository's three stores share one [`Shared`] state: the SCM connection, the local
//! cache (`.jj/repo/store/cache`), and the outbox (`.jj/repo/store/outbox`). Reads check the
//! cache, then the outbox, then the server, and concurrent reads coalesce into batched RPCs.
//! Writes encode and hash locally, stage durably in the outbox, and return at once;
//! `update_op_heads` publishes the whole outbox and swaps the operation heads in one server
//! transaction, which makes each jj transaction atomic.

mod backend;
mod batch;
mod cache;
pub mod calls;
pub mod changelists;
mod chunk;
pub mod client;
pub mod config;
mod ids;
mod op_heads;
mod op_store;
mod outbox;
pub mod prefetch;
mod publish;
mod refs;
mod segments;
pub mod shared;

#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::Arc;

use jj_lib::backend::BackendInitError;
use jj_lib::backend::BackendLoadError;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::RootOperationData;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::RepoInitError;
use jj_lib::repo::StoreFactories;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;

pub use backend::OkBackend;
pub use client::Client;
pub use config::RepoConfig;
pub use op_heads::OkOpHeadsStore;
pub use op_store::OkOpStore;
pub use publish::PublishError;
pub use shared::Shared;

/// The SCM protocol's messages, for the CL commands.
pub use ok_proto::ok::scm::v1 as pb;

/// The name every ok store is registered under and recorded as in `.jj/repo/*/type`.
pub const NAME: &str = "ok";

/// A repository on the SCM, as `jj clone` and `jj create` need it.
#[derive(Clone, Debug)]
pub struct Repository {
    pub id: i64,
    pub name: String,
    /// `main`: the root commit until the first landing.
    pub head: jj_lib::backend::CommitId,
}

impl TryFrom<ok_proto::ok::scm::v1::Repository> for Repository {
    type Error = tonic::Status;

    fn try_from(r: ok_proto::ok::scm::v1::Repository) -> Result<Self, tonic::Status> {
        let head = ids::id_from(&r.head)
            .ok_or_else(|| tonic::Status::data_loss("the SCM returned a malformed head"))?;
        Ok(Self {
            id: r.id,
            name: r.name,
            head: jj_lib::backend::CommitId::new(head.to_vec()),
        })
    }
}

/// The repository named `name` on the SCM at `endpoint`.
pub async fn get_repository(endpoint: &str, name: &str) -> Result<Repository, tonic::Status> {
    use ok_proto::ok::scm::v1 as pb;
    let (endpoint, name) = (endpoint.to_owned(), name.to_owned());
    shared::run(async move {
        let req = pb::GetRepositoryRequest {
            key: Some(pb::get_repository_request::Key::Name(name)),
        };
        client::repositories(&endpoint)
            .await?
            .get_repository(req)
            .await?
            .into_inner()
            .try_into()
    })
    .await
}

/// Creates a repository named `name` on the SCM at `endpoint`, the caller becoming its
/// creator.
pub async fn create_repository(endpoint: &str, name: &str) -> Result<Repository, tonic::Status> {
    use ok_proto::ok::scm::v1 as pb;
    let (endpoint, name) = (endpoint.to_owned(), name.to_owned());
    shared::run(async move {
        let req = pb::CreateRepositoryRequest {
            name,
            actor: String::new(),
        };
        client::repositories(&endpoint)
            .await?
            .create_repository(req)
            .await?
            .into_inner()
            .try_into()
    })
    .await
}

/// Makes `workspace` one of the caller's workspaces of repository `repository_id` on the SCM at
/// `endpoint`, with an operation log of its own (docs/scm.md, "Decisions", 3): false when
/// the caller has a workspace of that name already.
pub async fn create_workspace(
    endpoint: &str,
    repository_id: i64,
    workspace: &str,
) -> Result<bool, tonic::Status> {
    use ok_proto::ok::scm::v1 as pb;
    let client = Client::new(
        endpoint,
        repository_id,
        client::dev_principal(endpoint),
        None,
    )
    .map_err(|e| tonic::Status::internal(e.to_string()))?;
    let workspace = workspace.to_owned();
    shared::run(async move {
        let req = pb::CreateWorkspaceRequest {
            repository_id,
            workspace,
        };
        match client.workspaces().await?.create_workspace(req).await {
            Ok(_) => Ok(true),
            Err(s) if s.code() == tonic::Code::AlreadyExists => Ok(false),
            Err(s) => Err(s),
        }
    })
    .await
}

/// The caller's workspaces of repository `repository_id` on the SCM at `endpoint`.
pub async fn list_workspaces(
    endpoint: &str,
    repository_id: i64,
) -> Result<Vec<pb::WorkspaceState>, tonic::Status> {
    use ok_proto::ok::scm::v1 as pb;
    let client = Client::new(
        endpoint,
        repository_id,
        client::dev_principal(endpoint),
        None,
    )
    .map_err(|e| tonic::Status::internal(e.to_string()))?;
    shared::run(async move {
        let req = pb::ListWorkspacesRequest { repository_id };
        Ok(client
            .workspaces()
            .await?
            .list_workspaces(req)
            .await?
            .into_inner()
            .workspaces)
    })
    .await
}

/// The ok store factories, for `CliRunner::add_store_factories`.
pub fn factories() -> StoreFactories {
    let mut factories = StoreFactories::empty();
    factories.add_backend(
        NAME,
        Box::new(|_settings: &UserSettings, store_path: &Path| {
            Ok(Box::new(
                OkBackend::load(store_path).map_err(|e| BackendLoadError(e.into()))?,
            ))
        }),
    );
    factories.add_op_store(
        NAME,
        Box::new(
            |_settings: &UserSettings, store_path: &Path, root: RootOperationData| {
                Ok(Box::new(
                    OkOpStore::load(store_path, root).map_err(|e| BackendLoadError(e.into()))?,
                ))
            },
        ),
    );
    factories.add_op_heads_store(
        NAME,
        Box::new(|_settings: &UserSettings, store_path: &Path| {
            Ok(Box::new(
                OkOpHeadsStore::load(store_path).map_err(|e| BackendLoadError(e.into()))?,
            ))
        }),
    );
    factories
}

/// Lays out a new repository directory (`.jj/repo`, which must exist and be empty) for the
/// SCM repository `config` names: ok's three stores, with `store/ok.toml`, and jj's
/// default index and submodule stores. Nothing is asked of the server; the returned
/// repository is at the root operation, and `load_at_head` loads the SCM's heads.
pub async fn init_repo(
    settings: &UserSettings,
    repo_dir: &Path,
    config: RepoConfig,
    signer: Signer,
) -> Result<Arc<ReadonlyRepo>, RepoInitError> {
    let backend = move |_: &UserSettings,
                        store_path: &Path|
          -> Result<Box<dyn jj_lib::backend::Backend>, BackendInitError> {
        config
            .write(store_path)
            .map_err(|e| BackendInitError(e.into()))?;
        Ok(Box::new(
            OkBackend::load(store_path).map_err(|e| BackendInitError(e.into()))?,
        ))
    };
    let op_store = |_: &UserSettings,
                    store_path: &Path,
                    root: RootOperationData|
     -> Result<Box<dyn jj_lib::op_store::OpStore>, BackendInitError> {
        Ok(Box::new(
            OkOpStore::load(store_path, root).map_err(|e| BackendInitError(e.into()))?,
        ))
    };
    let op_heads = |_: &UserSettings,
                    store_path: &Path,
                    _root: &OperationId|
     -> Result<Box<dyn jj_lib::op_heads_store::OpHeadsStore>, BackendInitError> {
        Ok(Box::new(
            OkOpHeadsStore::load(store_path).map_err(|e| BackendInitError(e.into()))?,
        ))
    };
    ReadonlyRepo::init(
        settings,
        repo_dir,
        &backend,
        signer,
        &op_store,
        &op_heads,
        ReadonlyRepo::default_index_store_initializer(),
        ReadonlyRepo::default_submodule_store_initializer(),
    )
    .await
}
