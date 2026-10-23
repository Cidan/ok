//! The connection to ok's SCM.
//!
//! The endpoint is ok's public URL, `https://ok.example.com`: every request carries this machine's
//! credential and a proof signed by its key (`ok_credential`), which ok resolves to the user — or,
//! with `OK_TOKEN` set, a personal token, which narrows the user to what it allows. For
//! an ok SCM run locally with `-dev`, a plaintext `http://` endpoint on a loopback address is
//! allowed too, where the client names its principal itself from `OK_DEV_PRINCIPAL` (an ok not
//! serving development principals ignores that header, so it grants nothing anywhere else).
//!
//! On a network the installation trusts to carry it — a service mesh — the endpoint may be a
//! role's plaintext address on any host, with `OK_PLAINTEXT=1`; its requests still carry the
//! machine's credential.

use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use ok_credential::{Auth, Authed};
use ok_proto::ok::scm::v1 as pb;
use tonic::Request;
use tonic::Status;
use tonic::metadata::Ascii;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::calls::Measured;
use tonic::transport::Uri;

/// The variable naming the principal for a loopback development SCM.
pub const DEV_PRINCIPAL: &str = "OK_DEV_PRINCIPAL";

/// The metadata a development principal travels in (internal/serve/authn).
pub const DEV_PRINCIPAL_KEY: &str = "ok-dev-principal";

/// Set (to `1`) where the network carries plaintext safely: a plaintext endpoint on any address.
pub const PLAINTEXT: &str = "OK_PLAINTEXT";

fn plaintext_allowed() -> bool {
    std::env::var(PLAINTEXT).is_ok_and(|v| v == "1")
}

/// The metadata naming the repository a request is about — its ID, or its full name — by which the
/// edge in front of ok routes it to the repository's cell (docs/hosted.md, "Cells, as built").
pub const REPOSITORY_KEY: &str = "ok-repository";

/// Adds a request's headers: the development principal on loopback connections, and the repository
/// the request is about, when there is one.
#[derive(Clone, Default)]
pub struct Headers {
    dev: Option<MetadataValue<Ascii>>,
    repository: Option<MetadataValue<Ascii>>,
}

impl Headers {
    /// The principal a loopback development server is told the caller is, if any.
    pub fn dev_principal(&self) -> Option<&MetadataValue<Ascii>> {
        self.dev.as_ref()
    }

    /// The repository the requests name, if any.
    pub fn repository(&self) -> Option<&MetadataValue<Ascii>> {
        self.repository.as_ref()
    }

    /// These headers naming `repository` — its ID, or its full name — on every request; none for
    /// an empty one.
    pub fn for_repository(mut self, repository: &str) -> Self {
        self.repository = if repository.is_empty() {
            None
        } else {
            repository.parse().ok()
        };
        self
    }
}

impl Interceptor for Headers {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(p) = &self.dev {
            req.metadata_mut().insert(DEV_PRINCIPAL_KEY, p.clone());
        }
        if let Some(r) = &self.repository {
            req.metadata_mut().insert(REPOSITORY_KEY, r.clone());
        }
        Ok(req)
    }
}

/// A connection: measured (`calls::Measured`), carrying its headers (`Headers`).
pub type Svc = InterceptedService<Conn, Headers>;

/// A connection to one of ok's services, its requests proving the caller (`ok_credential`) and
/// every RPC over it recorded (`crate::calls`).
pub type Conn = Measured<Authed<Channel>>;
pub type StoreClient = pb::store_client::StoreClient<Svc>;
pub type RepositoriesClient = pb::repositories_client::RepositoriesClient<Svc>;
pub type ChangeListsClient = pb::change_lists_client::ChangeListsClient<Svc>;
pub type WorkspacesClient = pb::workspaces_client::WorkspacesClient<Svc>;
pub type SecretsClient = ok_proto::ok::secrets::v1::secrets_client::SecretsClient<Svc>;

/// Whether a refusal is a frozen network's (ok.v1.Frozen): its repository is moving between cells
/// (docs/hosted.md, "Cells, as built"), and nothing was done — what asked it asks again.
pub fn is_frozen(status: &Status) -> bool {
    use prost::Message;
    status.code() == tonic::Code::Unavailable
        && crate::publish::RpcStatus::decode(status.details()).is_ok_and(|s| {
            s.details
                .iter()
                .any(|d| d.type_url.ends_with("/ok.v1.Frozen"))
        })
}

/// How long a call asks again while its repository's network is frozen: a move freezes it for as
/// long as copying its rows takes, seconds for most.
const FROZEN_FOR: std::time::Duration = std::time::Duration::from_secs(300);

/// Runs a call, again while its repository's network is frozen (is_frozen), backing off, up to
/// FROZEN_FOR.
pub async fn while_frozen<T, F, Fut>(mut call: F) -> Result<T, Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Status>>,
{
    let deadline = std::time::Instant::now() + FROZEN_FOR;
    let mut wait = std::time::Duration::from_millis(250);
    loop {
        match call().await {
            Err(s) if is_frozen(&s) && std::time::Instant::now() + wait < deadline => {
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(std::time::Duration::from_secs(5));
            }
            result => return result,
        }
    }
}

/// How long a connection is used before the next call opens a new one.
const CONNECTION_AGE: std::time::Duration = std::time::Duration::from_secs(3600);

fn is_loopback(uri: &Uri) -> bool {
    let host = uri.host().unwrap_or_default().trim_matches(['[', ']']);
    host == "localhost" || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Whether `endpoint` is a plaintext loopback development SCM.
fn is_dev(endpoint: &str) -> bool {
    endpoint
        .parse::<Uri>()
        .is_ok_and(|uri| uri.scheme_str() == Some("http") && is_loopback(&uri))
}

/// The principal a new clone of a loopback development SCM acts as: `OK_DEV_PRINCIPAL` when it is
/// set. None for any other endpoint.
pub fn dev_principal(endpoint: &str) -> Option<String> {
    if !is_dev(endpoint) {
        return None;
    }
    std::env::var(DEV_PRINCIPAL).ok().filter(|p| !p.is_empty())
}

/// The machine's credential, as a connection's requests prove it — or a personal token, when the
/// environment holds one (`OK_TOKEN`).
fn machine() -> Result<Auth, Status> {
    if let Some(t) = ok_credential::env_token() {
        return Ok(Auth::Token(t));
    }
    let dir = ok_credential::dir().map_err(|e| Status::unauthenticated(e.to_string()))?;
    let src =
        ok_credential::Source::open(dir).map_err(|e| Status::unauthenticated(e.to_string()))?;
    Ok(Auth::Machine(Arc::new(src)))
}

async fn plain(uri: Uri, endpoint: &str) -> Result<Channel, Status> {
    Channel::builder(uri)
        .connect()
        .await
        .map_err(|e| Status::unavailable(format!("connect to {endpoint}: {e}")))
}

/// Who a loopback plaintext endpoint is called as: a development principal named — the clone's
/// (`dev`), or else `OK_DEV_PRINCIPAL` — for a development server; else this machine's credential,
/// for an installation served on localhost without TLS (docker compose).
#[derive(Debug, PartialEq, Eq)]
enum LoopbackCaller {
    Dev(String),
    Machine,
}

fn loopback_caller(dev: Option<&str>, env: Option<String>) -> LoopbackCaller {
    match dev.map(str::to_owned).or(env.filter(|p| !p.is_empty())) {
        Some(p) => LoopbackCaller::Dev(p),
        None => LoopbackCaller::Machine,
    }
}

/// Connects to `endpoint` — the SCM, or another of ok's services — as this machine's user. On a
/// loopback plaintext endpoint the caller is a development principal when one is named
/// (`loopback_caller`), and this machine's credential otherwise. Runs on the store runtime.
pub async fn connect(endpoint: &str, dev: Option<&str>) -> Result<(Conn, Headers), Status> {
    let uri: Uri = endpoint
        .parse()
        .map_err(|e| Status::invalid_argument(format!("endpoint {endpoint}: {e}")))?;
    match uri.scheme_str() {
        Some("https") => {
            let channel = ok_credential::channel(endpoint)
                .await
                .map_err(|e| Status::unauthenticated(e.to_string()))?;
            Ok((Measured::new(channel), Headers::default()))
        }
        Some("http") => {
            if !is_loopback(&uri) {
                if !plaintext_allowed() {
                    return Err(Status::invalid_argument(format!(
                        "endpoint {endpoint}: plaintext is only for a loopback development server, or a network the installation trusts ({PLAINTEXT}=1)"
                    )));
                }
                let auth = machine()?;
                let channel = plain(uri, endpoint).await?;
                return Ok((
                    Measured::new(Authed::new(channel, auth)),
                    Headers::default(),
                ));
            }
            let principal = match loopback_caller(dev, std::env::var(DEV_PRINCIPAL).ok()) {
                LoopbackCaller::Dev(p) => p,
                LoopbackCaller::Machine => {
                    let auth = machine().map_err(|e| {
                        Status::unauthenticated(format!(
                            "{}: sign in with jj auth login {endpoint} (a development server takes {DEV_PRINCIPAL} instead)",
                            e.message()
                        ))
                    })?;
                    let channel = plain(uri, endpoint).await?;
                    return Ok((
                        Measured::new(Authed::new(channel, auth)),
                        Headers::default(),
                    ));
                }
            };
            let value = principal.parse().map_err(|_| {
                Status::invalid_argument(format!("{DEV_PRINCIPAL} is not a header value"))
            })?;
            let channel = plain(uri, endpoint).await?;
            Ok((
                Measured::new(Authed::new(channel, Auth::None)),
                Headers {
                    dev: Some(value),
                    repository: None,
                },
            ))
        }
        _ => Err(Status::invalid_argument(format!(
            "endpoint {endpoint}: use https://"
        ))),
    }
}

/// A Repositories client for `endpoint`, for commands that run before a repository exists
/// locally (`jj clone`, `jj create`), its requests naming `repository` (its full name; empty for
/// none). Call from the store runtime.
pub async fn repositories(endpoint: &str, repository: &str) -> Result<RepositoriesClient, Status> {
    let (channel, headers) = connect(endpoint, None).await?;
    Ok(
        pb::repositories_client::RepositoriesClient::with_interceptor(
            channel,
            headers.for_repository(repository),
        )
        .max_decoding_message_size(MAX_MESSAGE),
    )
}

/// The largest message either side sends: a publish's Begin and Missing carry every staged
/// object's reference, and the client splits publishes to stay under this.
pub const MAX_MESSAGE: usize = 16 << 20;

/// One repository's connection, opened on first use.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    endpoint: String,
    repository_id: i64,
    dev_principal: Option<String>,
    workspace: String,
    conn: tokio::sync::Mutex<Option<(std::time::Instant, Conn, Headers)>>,
}

impl Client {
    /// A connection to `repository_id` at `endpoint`, as the workspace whose operation log its
    /// operation-log requests name (none: a presentation's bare store, which reads no log).
    pub fn new(
        endpoint: &str,
        repository_id: i64,
        dev_principal: Option<String>,
        workspace: Option<String>,
    ) -> io::Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner {
                endpoint: endpoint.to_owned(),
                repository_id,
                dev_principal,
                workspace: workspace.unwrap_or_default(),
                conn: tokio::sync::Mutex::new(None),
            }),
        })
    }

    /// The connection, its requests naming the repository by ID.
    async fn conn(&self) -> Result<(Conn, Headers), Status> {
        let mut conn = self.inner.conn.lock().await;
        if let Some((opened, channel, headers)) = conn.as_ref()
            && opened.elapsed() < CONNECTION_AGE
        {
            return Ok((channel.clone(), headers.clone()));
        }
        let (channel, headers) =
            connect(&self.inner.endpoint, self.inner.dev_principal.as_deref()).await?;
        let headers = headers.for_repository(&self.inner.repository_id.to_string());
        *conn = Some((std::time::Instant::now(), channel.clone(), headers.clone()));
        Ok((channel, headers))
    }

    pub fn repository_id(&self) -> i64 {
        self.inner.repository_id
    }

    /// The workspace whose operation log this repository's are (empty: none).
    pub fn workspace(&self) -> String {
        self.inner.workspace.clone()
    }

    pub fn endpoint(&self) -> &str {
        &self.inner.endpoint
    }

    /// The Store client. Call from the store runtime.
    pub async fn store(&self) -> Result<StoreClient, Status> {
        let (channel, dev) = self.conn().await?;
        Ok(
            pb::store_client::StoreClient::with_interceptor(channel, dev)
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE),
        )
    }

    /// The Repositories client. Call from the store runtime.
    pub async fn repositories(&self) -> Result<RepositoriesClient, Status> {
        let (channel, dev) = self.conn().await?;
        Ok(
            pb::repositories_client::RepositoriesClient::with_interceptor(channel, dev)
                .max_decoding_message_size(MAX_MESSAGE),
        )
    }

    /// The Workspaces client (okfs's leases and checkpoints). Call from the store runtime.
    pub async fn workspaces(&self) -> Result<WorkspacesClient, Status> {
        let (channel, dev) = self.conn().await?;
        Ok(
            pb::workspaces_client::WorkspacesClient::with_interceptor(channel, dev)
                .max_decoding_message_size(MAX_MESSAGE),
        )
    }

    /// ok secrets' Secrets client: the same address, which routes `ok.secrets.*` to ok secrets
    /// (docs/secrets.md). Call from the store runtime.
    pub async fn secrets(&self) -> Result<SecretsClient, Status> {
        let (channel, dev) = self.conn().await?;
        Ok(
            ok_proto::ok::secrets::v1::secrets_client::SecretsClient::with_interceptor(
                channel, dev,
            )
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE),
        )
    }

    /// The ChangeLists client. Call from the store runtime.
    pub async fn change_lists(&self) -> Result<ChangeListsClient, Status> {
        let (channel, dev) = self.conn().await?;
        Ok(
            pb::change_lists_client::ChangeListsClient::with_interceptor(channel, dev)
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE),
        )
    }
}

#[cfg(test)]
mod loopback_tests {
    use super::LoopbackCaller;
    use super::loopback_caller;

    // A development principal named — the clone's first, then the environment's — makes a
    // development call; with none, the machine's credential is presented: an installation on
    // localhost without TLS is no development server.
    #[test]
    fn a_loopback_endpoint_is_called_as_named_or_as_the_machine() {
        assert_eq!(
            loopback_caller(Some("ada"), Some("bea".into())),
            LoopbackCaller::Dev("ada".into())
        );
        assert_eq!(
            loopback_caller(None, Some("bea".into())),
            LoopbackCaller::Dev("bea".into())
        );
        assert_eq!(
            loopback_caller(None, Some(String::new())),
            LoopbackCaller::Machine
        );
        assert_eq!(loopback_caller(None, None), LoopbackCaller::Machine);
    }
}

#[cfg(test)]
mod header_tests {
    use super::DEV_PRINCIPAL_KEY;
    use super::Headers;
    use super::REPOSITORY_KEY;
    use tonic::Request;
    use tonic::service::Interceptor;

    // Every request names the repository its connection is for, for the edge to route it by, beside
    // a development principal when there is one; an empty name names none.
    #[test]
    fn requests_name_their_repository() {
        let mut h = Headers {
            dev: Some("ada.human.example.com".parse().unwrap()),
            repository: None,
        }
        .for_repository("42");
        let req = h.call(Request::new(())).unwrap();
        assert_eq!(req.metadata().get(REPOSITORY_KEY).unwrap(), "42");
        assert_eq!(
            req.metadata().get(DEV_PRINCIPAL_KEY).unwrap(),
            "ada.human.example.com"
        );
        let mut by_name = Headers::default().for_repository("acme/app");
        let req = by_name.call(Request::new(())).unwrap();
        assert_eq!(req.metadata().get(REPOSITORY_KEY).unwrap(), "acme/app");
        assert!(req.metadata().get(DEV_PRINCIPAL_KEY).is_none());
        let mut none = Headers::default().for_repository("");
        assert!(
            none.call(Request::new(()))
                .unwrap()
                .metadata()
                .get(REPOSITORY_KEY)
                .is_none()
        );
    }
}

#[cfg(test)]
mod frozen_tests {
    use super::is_frozen;
    use super::while_frozen;
    use crate::publish::RpcStatus;
    use prost::Message;
    use tonic::Code;
    use tonic::Status;

    fn refusal(code: Code, type_url: &str) -> Status {
        let details = RpcStatus {
            code: code as i32,
            message: "moving".into(),
            details: vec![prost_types::Any {
                type_url: type_url.into(),
                value: vec![8, 7],
            }],
        }
        .encode_to_vec();
        Status::with_details(code, "moving", details.into())
    }

    // A frozen network's refusal is asked again until it is not; any other refusal is not.
    #[tokio::test]
    async fn frozen_refusals_are_asked_again() {
        assert!(is_frozen(&refusal(
            Code::Unavailable,
            "type.googleapis.com/ok.v1.Frozen"
        )));
        assert!(!is_frozen(&refusal(
            Code::FailedPrecondition,
            "type.googleapis.com/ok.v1.Frozen"
        )));
        assert!(!is_frozen(&refusal(
            Code::Unavailable,
            "type.googleapis.com/ok.v1.Moved"
        )));
        assert!(!is_frozen(&Status::unavailable("down")));
        let mut calls = 0;
        let got = while_frozen(|| {
            calls += 1;
            let n = calls;
            async move {
                if n < 4 {
                    Err(refusal(
                        Code::Unavailable,
                        "type.googleapis.com/ok.v1.Frozen",
                    ))
                } else {
                    Ok(n)
                }
            }
        })
        .await;
        assert_eq!(got.unwrap(), 4);
        let mut down = 0;
        let got: Result<(), Status> = while_frozen(|| {
            down += 1;
            async { Err(Status::unavailable("down")) }
        })
        .await;
        assert!(got.is_err());
        assert_eq!(down, 1);
    }
}
