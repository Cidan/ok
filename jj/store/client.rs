//! The connection to ok's SCM.
//!
//! The endpoint is ok's public URL, `https://ok.example.com`: every request carries this machine's
//! credential and a proof signed by its key (`ok_credential`), which ok resolves to the user. For
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

/// Adds the development principal header on loopback connections.
#[derive(Clone)]
pub struct DevPrincipal(Option<MetadataValue<Ascii>>);

impl DevPrincipal {
    /// The principal a loopback development server is told the caller is, if any.
    pub fn value(&self) -> Option<&MetadataValue<Ascii>> {
        self.0.as_ref()
    }
}

impl Interceptor for DevPrincipal {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(p) = &self.0 {
            req.metadata_mut().insert(DEV_PRINCIPAL_KEY, p.clone());
        }
        Ok(req)
    }
}

/// A connection: measured (`calls::Measured`), carrying the development principal when there is one.
pub type Svc = InterceptedService<Conn, DevPrincipal>;

/// A connection to one of ok's services, its requests proving the caller (`ok_credential`) and
/// every RPC over it recorded (`crate::calls`).
pub type Conn = Measured<Authed<Channel>>;
pub type StoreClient = pb::store_client::StoreClient<Svc>;
pub type RepositoriesClient = pb::repositories_client::RepositoriesClient<Svc>;
pub type ChangeListsClient = pb::change_lists_client::ChangeListsClient<Svc>;
pub type WorkspacesClient = pb::workspaces_client::WorkspacesClient<Svc>;

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

/// The machine's credential, as a connection's requests prove it.
fn machine() -> Result<Auth, Status> {
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

/// Connects to `endpoint` — the SCM, or another of ok's services — as this machine's user. On a
/// loopback development server the caller is `dev`, or else `OK_DEV_PRINCIPAL`. Runs on the store
/// runtime.
pub async fn connect(endpoint: &str, dev: Option<&str>) -> Result<(Conn, DevPrincipal), Status> {
    let uri: Uri = endpoint
        .parse()
        .map_err(|e| Status::invalid_argument(format!("endpoint {endpoint}: {e}")))?;
    match uri.scheme_str() {
        Some("https") => {
            let channel = ok_credential::channel(endpoint)
                .await
                .map_err(|e| Status::unauthenticated(e.to_string()))?;
            Ok((Measured::new(channel), DevPrincipal(None)))
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
                    DevPrincipal(None),
                ));
            }
            let principal = dev
                .map(str::to_owned)
                .or_else(|| std::env::var(DEV_PRINCIPAL).ok().filter(|p| !p.is_empty()))
                .ok_or_else(|| {
                    Status::unauthenticated(format!(
                        "a loopback development SCM needs {DEV_PRINCIPAL}"
                    ))
                })?;
            let value = principal.parse().map_err(|_| {
                Status::invalid_argument(format!("{DEV_PRINCIPAL} is not a header value"))
            })?;
            let channel = plain(uri, endpoint).await?;
            Ok((
                Measured::new(Authed::new(channel, Auth::None)),
                DevPrincipal(Some(value)),
            ))
        }
        _ => Err(Status::invalid_argument(format!(
            "endpoint {endpoint}: use https://"
        ))),
    }
}

/// A Repositories client for `endpoint`, for commands that run before a repository exists
/// locally (`jj clone`, `jj create`).
/// Call from the store runtime.
pub async fn repositories(endpoint: &str) -> Result<RepositoriesClient, Status> {
    let (channel, dev) = connect(endpoint, None).await?;
    Ok(
        pb::repositories_client::RepositoriesClient::with_interceptor(channel, dev)
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
    conn: tokio::sync::Mutex<Option<(std::time::Instant, Conn, DevPrincipal)>>,
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

    async fn conn(&self) -> Result<(Conn, DevPrincipal), Status> {
        let mut conn = self.inner.conn.lock().await;
        if let Some((opened, channel, dev)) = conn.as_ref()
            && opened.elapsed() < CONNECTION_AGE
        {
            return Ok((channel.clone(), dev.clone()));
        }
        let (channel, dev) =
            connect(&self.inner.endpoint, self.inner.dev_principal.as_deref()).await?;
        *conn = Some((std::time::Instant::now(), channel.clone(), dev.clone()));
        Ok((channel, dev))
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
