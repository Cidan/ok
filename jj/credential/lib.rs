//! This machine's ok credential, for ok's jj (docs/identity.md, "Signing in").
//!
//! `jj auth login` (or `ok auth login`) signs the machine in with ok identity's device login and
//! writes the credential; this crate reads the same files `ok auth login` writes:
//!
//! ```text
//! <dir>/key.pem        PKCS#8 "PRIVATE KEY", ECDSA P-256, mode 0600
//! <dir>/cert.pem       the certificate, naming ok:principal:<name> and ok:credential:<id>
//! <dir>/server.json    where ok is: {"url": "https://ok.example.com", "addr": "host:443", "tls": true}
//! <dir>/person.json    who signed in: {"name": …, "email": …, "display_name": …}, when known
//! ```
//!
//! where `<dir>` is `$OK_CONFIG_DIR/credential`, else `ok/credential` in the user's configuration
//! directory (`$XDG_CONFIG_HOME` or `~/.config`; `~/Library/Application Support` on macOS). Every request carries the certificate and a proof signed by the key
//! over the request's method, the time, and a nonce ([`Authed`]); the certificate is renewed
//! before it expires.

use std::io;
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use p256::ecdsa::signature::Signer as _;
use p256::ecdsa::{Signature, SigningKey};
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey, LineEnding};
use rand_core::{OsRng, RngCore as _};
use serde::{Deserialize, Serialize};
use tonic::transport::{Channel, ClientTlsConfig, Uri};
use x509_cert::der::{Decode as _, DecodePem as _, Encode as _, EncodePem as _};
use x509_cert::ext::pkix::SubjectAltName;
use x509_cert::ext::pkix::name::GeneralName;

mod login;
pub use login::{Approval, login};

#[cfg(test)]
mod tests;

/// A certificate is renewed once less than this remains of it (certificates live 24 hours).
pub const RENEW_WITHIN: Duration = Duration::from_secs(6 * 3600);

/// The files in the credential directory.
pub const KEY_FILE: &str = "key.pem";
pub const CERT_FILE: &str = "cert.pem";
pub const SERVER_FILE: &str = "server.json";
pub const PERSON_FILE: &str = "person.json";

/// The metadata a request carries its credential and proof under (internal/credential).
pub const CERTIFICATE_KEY: &str = "ok-credential-bin";
pub const PROOF_KEY: &str = "ok-proof";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("this machine is not signed in to ok; run jj auth login")]
    NotLoggedIn,
    #[error("{0} is readable by others (mode {1:04o}); it must be 0600")]
    Exposed(PathBuf, u32),
    #[error("{0}: {1}")]
    Io(PathBuf, io::Error),
    #[error("{0}: {1}")]
    Malformed(PathBuf, String),
    #[error(
        "could not locate the credential directory: set HOME, XDG_CONFIG_HOME, or OK_CONFIG_DIR"
    )]
    NoDirectory,
    #[error("the credential expired; run jj auth login")]
    Expired,
    #[error("renewing the certificate: {0}")]
    Renew(tonic::Status),
    #[error("{0} is not ok's address: {1}")]
    Endpoint(String, String),
    #[error("connecting to {0}: {1}")]
    Transport(String, tonic::transport::Error),
    #[error("signing in: {0}")]
    Login(String),
}

/// Where ok is (`server.json`), as `ok auth login` writes it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Server {
    /// ok's public URL: https://ok.example.com.
    pub url: String,
    /// The address its services answer on: host:port.
    pub addr: String,
    pub tls: bool,
}

impl Server {
    /// The server at ok's public URL: https://… on 443, http://… (loopback, for development) on
    /// 80, unless the URL names a port.
    pub fn parse(url: &str) -> Result<Self, Error> {
        let uri: Uri = url
            .parse()
            .map_err(|e: http::uri::InvalidUri| Error::Endpoint(url.into(), e.to_string()))?;
        let tls = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => return Err(Error::Endpoint(url.into(), "use https://".into())),
        };
        let authority = uri
            .authority()
            .ok_or_else(|| Error::Endpoint(url.into(), "no host".into()))?;
        let port = authority.port_u16().unwrap_or(if tls { 443 } else { 80 });
        let scheme = if tls { "https" } else { "http" };
        Ok(Self {
            url: format!("{scheme}://{authority}"),
            addr: format!("{}:{port}", authority.host()),
            tls,
        })
    }

    /// The URI channels dial.
    fn uri(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://{}", self.addr)
    }

    /// A channel to the server, calling as no one.
    pub async fn connect(&self) -> Result<Channel, Error> {
        dial(&self.uri()).await
    }
}

/// Who signed in (`person.json`), as identity's directory says.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Person {
    pub name: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub display_name: String,
}

/// A loaded credential.
#[derive(Clone)]
pub struct Credential {
    pub server: Server,
    /// The principal the certificate names: ada.human.example.com.
    pub principal: String,
    /// The credential the certificate is of.
    pub id: String,
    /// When the certificate expires.
    pub not_after: SystemTime,
    /// Who signed in, when the login recorded it.
    pub person: Option<Person>,
    cert_der: Vec<u8>,
    key: SigningKey,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("principal", &self.principal)
            .field("id", &self.id)
            .field("server", &self.server)
            .finish_non_exhaustive()
    }
}

impl Credential {
    /// A credential from its parts: a certificate (DER) for `key`, at `server`.
    pub fn from_parts(server: Server, cert_der: Vec<u8>, key: SigningKey) -> Result<Self, Error> {
        let (principal, id, not_after) =
            parse_certificate(&cert_der, Path::new("the certificate"))?;
        Ok(Self {
            server,
            principal,
            id,
            not_after,
            person: None,
            cert_der,
            key,
        })
    }

    /// Whether the certificate is due for renewal at `now`.
    pub fn needs_renewal(&self, now: SystemTime) -> bool {
        self.not_after.duration_since(now).unwrap_or_default() < RENEW_WITHIN
    }

    /// The certificate, DER.
    pub fn certificate(&self) -> &[u8] {
        &self.cert_der
    }

    /// The proof for one request to `method` (`/ok.scm.v1.Store/ReadObjects`) at `now`:
    /// `v1.<seconds>.<nonce>.<signature>`, the signature ECDSA P-256, ASN.1, over SHA-256 of
    /// `"ok-proof v1\n" + method + "\n" + seconds + "\n" + nonce`.
    pub fn proof(&self, method: &str, now: SystemTime) -> String {
        let mut n = [0u8; 16];
        OsRng.fill_bytes(&mut n);
        let nonce = URL_SAFE_NO_PAD.encode(n);
        let seconds = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let signed = format!("ok-proof v1\n{method}\n{seconds}\n{nonce}");
        let sig: Signature = self.key.sign(signed.as_bytes());
        format!(
            "v1.{seconds}.{nonce}.{}",
            URL_SAFE_NO_PAD.encode(sig.to_der().as_bytes())
        )
    }

    /// The handle of a person's principal: ada of ada.human.example.com.
    pub fn handle(&self) -> Option<&str> {
        self.principal.split_once(".human.").map(|(h, _)| h)
    }
}

/// The credential directory: `$OK_CONFIG_DIR/credential`, else `ok/credential` in the user's
/// configuration directory — where `ok auth login` keeps it (Go's `os.UserConfigDir`).
pub fn dir() -> Result<PathBuf, Error> {
    let nonempty = |name: &str| std::env::var_os(name).filter(|v| !v.is_empty());
    if let Some(d) = nonempty("OK_CONFIG_DIR") {
        return Ok(PathBuf::from(d).join("credential"));
    }
    Ok(user_config_dir()?.join("ok").join("credential"))
}

/// The user's configuration directory, as Go's `os.UserConfigDir` finds it.
fn user_config_dir() -> Result<PathBuf, Error> {
    let nonempty = |name: &str| std::env::var_os(name).filter(|v| !v.is_empty());
    let home = || {
        nonempty("HOME")
            .map(PathBuf::from)
            .ok_or(Error::NoDirectory)
    };
    if cfg!(target_os = "macos") {
        return Ok(home()?.join("Library").join("Application Support"));
    }
    if let Some(d) = nonempty("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|d| d.is_absolute())
    {
        return Ok(d);
    }
    Ok(home()?.join(".config"))
}

/// Signs this machine out: revokes the credential in `dir`, when ok answers, and removes it.
pub async fn logout(dir: &Path) -> Result<(), Error> {
    use ok_proto::ok::identity::v1 as pb;
    let c = match load(dir) {
        Err(Error::NotLoggedIn) => return Ok(()),
        c => c?,
    };
    if let Ok(channel) = c.server.connect().await {
        let svc = Authed::new(channel, Auth::Static(Arc::new(c.clone())));
        let revoked = pb::credentials_client::CredentialsClient::new(svc)
            .revoke(pb::RevokeRequest { id: c.id.clone() })
            .await;
        if let Err(e) = revoked {
            eprintln!(
                "revoking the credential: {} (it is removed from this machine anyway)",
                e.message()
            );
        }
    }
    for f in [KEY_FILE, CERT_FILE, SERVER_FILE, PERSON_FILE] {
        let p = dir.join(f);
        if let Err(e) = std::fs::remove_file(&p)
            && e.kind() != io::ErrorKind::NotFound
        {
            return Err(Error::Io(p, e));
        }
    }
    Ok(())
}

fn read(path: &Path) -> Result<Vec<u8>, Error> {
    std::fs::read(path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            Error::NotLoggedIn
        } else {
            Error::Io(path.to_owned(), e)
        }
    })
}

/// The URI SAN value after `prefix`: the certificate's one such name.
fn uri_value(sans: &[GeneralName], prefix: &str) -> Option<String> {
    let mut found = None;
    for n in sans {
        if let GeneralName::UniformResourceIdentifier(u) = n
            && let Some(v) = u.as_str().strip_prefix(prefix)
        {
            if found.is_some() || v.is_empty() {
                return None;
            }
            found = Some(v.to_owned());
        }
    }
    found
}

/// What a certificate names: its principal, its credential, and when it expires.
fn parse_certificate(der: &[u8], path: &Path) -> Result<(String, String, SystemTime), Error> {
    let bad = |m: &str| Error::Malformed(path.to_owned(), m.to_owned());
    let cert = x509_cert::Certificate::from_der(der).map_err(|e| bad(&e.to_string()))?;
    let not_after = cert.tbs_certificate.validity.not_after.to_system_time();
    let mut sans = Vec::new();
    for ext in cert.tbs_certificate.extensions.iter().flatten() {
        if ext.extn_id == x509_cert::der::oid::db::rfc5280::ID_CE_SUBJECT_ALT_NAME {
            let san = SubjectAltName::from_der(ext.extn_value.as_bytes())
                .map_err(|e| bad(&e.to_string()))?;
            sans.extend(san.0);
        }
    }
    let principal = uri_value(&sans, "ok:principal:").ok_or_else(|| bad("names no principal"))?;
    let id = uri_value(&sans, "ok:credential:").ok_or_else(|| bad("names no credential"))?;
    Ok((principal, id, not_after))
}

/// Reads the credential in `dir`, refusing a key that anyone but the owner can read.
pub fn load(dir: &Path) -> Result<Credential, Error> {
    let key_path = dir.join(KEY_FILE);
    let mode = match std::fs::metadata(&key_path) {
        Ok(m) => m.permissions().mode() & 0o777,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Error::NotLoggedIn),
        Err(e) => return Err(Error::Io(key_path, e)),
    };
    if mode & 0o077 != 0 {
        return Err(Error::Exposed(key_path, mode));
    }
    let key_pem = String::from_utf8(read(&key_path)?)
        .map_err(|_| Error::Malformed(key_path.clone(), "not PEM".into()))?;
    let key = SigningKey::from_pkcs8_pem(&key_pem).map_err(|e| {
        Error::Malformed(
            key_path.clone(),
            format!("not a PKCS#8 ECDSA P-256 key: {e}"),
        )
    })?;
    let cert_path = dir.join(CERT_FILE);
    let cert = x509_cert::Certificate::from_pem(read(&cert_path)?)
        .map_err(|e| Error::Malformed(cert_path.clone(), e.to_string()))?;
    let cert_der = cert
        .to_der()
        .map_err(|e| Error::Malformed(cert_path.clone(), e.to_string()))?;
    let (principal, id, not_after) = parse_certificate(&cert_der, &cert_path)?;
    let server_path = dir.join(SERVER_FILE);
    let server: Server = serde_json::from_slice(&read(&server_path)?)
        .map_err(|e| Error::Malformed(server_path, e.to_string()))?;
    let person = std::fs::read(dir.join(PERSON_FILE))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok());
    Ok(Credential {
        server,
        principal,
        id,
        not_after,
        person,
        cert_der,
        key,
    })
}

/// Replaces a file whole: a reader sees the old one or the new one.
fn write_file(path: &Path, data: &[u8], mode: u32) -> Result<(), Error> {
    let io_err = |e| Error::Io(path.to_owned(), e);
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir).map_err(io_err)?;
    tmp.write_all(data).map_err(io_err)?;
    std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(mode)).map_err(io_err)?;
    tmp.persist(path).map_err(|e| io_err(e.error))?;
    Ok(())
}

/// Writes a credential to `dir`, as `ok auth login` does: the key only its owner reads.
pub fn save(
    dir: &Path,
    server: &Server,
    cert_der: &[u8],
    key: &SigningKey,
    person: Option<&Person>,
) -> Result<(), Error> {
    std::fs::create_dir_all(dir).map_err(|e| Error::Io(dir.to_owned(), e))?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| Error::Io(dir.to_owned(), e))?;
    let key_pem = key
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(|e| Error::Malformed(dir.join(KEY_FILE), e.to_string()))?;
    write_file(&dir.join(KEY_FILE), key_pem.as_bytes(), 0o600)?;
    write_certificate(dir, cert_der)?;
    let sj = serde_json::to_vec_pretty(server)
        .map_err(|e| Error::Malformed(dir.join(SERVER_FILE), e.to_string()))?;
    write_file(&dir.join(SERVER_FILE), &sj, 0o644)?;
    if let Some(p) = person {
        let pj = serde_json::to_vec_pretty(p)
            .map_err(|e| Error::Malformed(dir.join(PERSON_FILE), e.to_string()))?;
        write_file(&dir.join(PERSON_FILE), &pj, 0o644)?;
    }
    Ok(())
}

fn write_certificate(dir: &Path, cert_der: &[u8]) -> Result<(), Error> {
    let path = dir.join(CERT_FILE);
    let cert = x509_cert::Certificate::from_der(cert_der)
        .map_err(|e| Error::Malformed(path.clone(), e.to_string()))?;
    let pem = cert
        .to_pem(LineEnding::LF)
        .map_err(|e| Error::Malformed(path.clone(), e.to_string()))?;
    write_file(&path, pem.as_bytes(), 0o644)
}

/// The SubjectPublicKeyInfo of a key, DER: what the device login certifies.
pub fn public_key_der(key: &SigningKey) -> Vec<u8> {
    key.verifying_key()
        .to_public_key_der()
        .map(|d| d.into_vec())
        .unwrap_or_default()
}

/// A new key for a credential.
pub fn new_key() -> SigningKey {
    SigningKey::random(&mut OsRng)
}

/// The variable naming a CA certificate (PEM) a TLS server's certificate is checked against, in
/// place of the system's roots: an installation's own, between its roles.
pub const CA_FILE: &str = "OK_CA_FILE";

/// Dials a URI: TLS for https, verified against the system's roots, or `$OK_CA_FILE`'s.
async fn dial(uri: &str) -> Result<Channel, Error> {
    let parsed: Uri = uri
        .parse()
        .map_err(|e: http::uri::InvalidUri| Error::Endpoint(uri.into(), e.to_string()))?;
    let mut ep = Channel::builder(parsed.clone());
    if parsed.scheme_str() == Some("https") {
        let host = parsed.host().unwrap_or_default().to_owned();
        let mut tls = ClientTlsConfig::new().domain_name(host);
        tls = match std::env::var_os(CA_FILE).filter(|v| !v.is_empty()) {
            Some(path) => {
                let pem = std::fs::read(&path).map_err(|e| Error::Io(PathBuf::from(&path), e))?;
                tls.ca_certificate(tonic::transport::Certificate::from_pem(pem))
            }
            None => tls.with_native_roots(),
        };
        ep = ep
            .tls_config(tls)
            .map_err(|e| Error::Transport(uri.into(), e))?;
    }
    ep.connect()
        .await
        .map_err(|e| Error::Transport(uri.into(), e))
}

/// The machine's credential as calls use it: renewed before it expires, and written back.
pub struct Source {
    dir: PathBuf,
    current: tokio::sync::Mutex<Credential>,
}

impl Source {
    /// The credential in `dir`.
    pub fn open(dir: PathBuf) -> Result<Self, Error> {
        let c = load(&dir)?;
        Ok(Self {
            dir,
            current: tokio::sync::Mutex::new(c),
        })
    }

    /// The credential to call with, renewed first when it is due.
    pub async fn current(&self) -> Result<Credential, Error> {
        self.current_at(SystemTime::now()).await
    }

    async fn current_at(&self, now: SystemTime) -> Result<Credential, Error> {
        let mut c = self.current.lock().await;
        if c.needs_renewal(now) {
            // Another process may have renewed it — `ok auth`, or the service that keeps a
            // workload's credential here.
            if let Ok(on_disk) = load(&self.dir)
                && on_disk.id == c.id
                && on_disk.not_after > c.not_after
            {
                *c = on_disk;
            }
        }
        if now >= c.not_after {
            return Err(Error::Expired);
        }
        if c.needs_renewal(now) {
            match renew(&c).await {
                Ok(der) => {
                    write_certificate(&self.dir, &der)?;
                    *c = load(&self.dir)?;
                }
                // Within a minute of expiring it is of no use; until then, it is tried again.
                Err(e)
                    if c.not_after.duration_since(now).unwrap_or_default()
                        < Duration::from_secs(60) =>
                {
                    return Err(e);
                }
                Err(_) => {}
            }
        }
        Ok(c.clone())
    }
}

/// Renews a credential's certificate with itself: the new certificate, DER.
async fn renew(c: &Credential) -> Result<Vec<u8>, Error> {
    use ok_proto::ok::identity::v1 as pb;
    let channel = c.server.connect().await?;
    let svc = Authed::new(channel, Auth::Static(Arc::new(c.clone())));
    let mut client = pb::credentials_client::CredentialsClient::new(svc);
    let resp = client
        .renew(pb::RenewRequest {})
        .await
        .map_err(Error::Renew)?;
    resp.into_inner()
        .certificate
        .and_then(|c| c.chain.into_iter().next())
        .ok_or_else(|| {
            Error::Renew(tonic::Status::internal(
                "the renewal returned no certificate",
            ))
        })
}

/// How a channel's calls prove their caller.
#[derive(Clone)]
pub enum Auth {
    /// As no one: a service open to anyone, or one a network the installation trusts carries.
    None,
    /// With the machine's credential, renewed as it is due.
    Machine(Arc<Source>),
    /// With a credential as it is.
    Static(Arc<Credential>),
}

/// A channel whose every request carries its caller's credential and a fresh proof.
#[derive(Clone)]
pub struct Authed<S> {
    inner: S,
    auth: Auth,
}

impl<S> Authed<S> {
    pub fn new(inner: S, auth: Auth) -> Self {
        Self { inner, auth }
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl<S, B> tower_service::Service<http::Request<B>> for Authed<S>
where
    S: tower_service::Service<http::Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<S::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: http::Request<B>) -> Self::Future {
        // The service polled ready makes the call; a clone takes its place.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let auth = self.auth.clone();
        Box::pin(async move {
            let cred = match &auth {
                Auth::None => None,
                Auth::Machine(src) => Some(src.current().await?),
                Auth::Static(c) => Some((**c).clone()),
            };
            if let Some(c) = cred {
                let proof = c.proof(req.uri().path(), SystemTime::now());
                let h = req.headers_mut();
                h.insert(
                    CERTIFICATE_KEY,
                    http::HeaderValue::from_str(&STANDARD_NO_PAD.encode(c.certificate()))?,
                );
                h.insert(PROOF_KEY, http::HeaderValue::from_str(&proof)?);
            }
            inner.call(req).await.map_err(Into::into)
        })
    }
}

/// Dials ok at `endpoint` (its public URL) as this machine's credential.
pub async fn channel(endpoint: &str) -> Result<Authed<Channel>, Error> {
    let src = Source::open(dir()?)?;
    let server = Server::parse(endpoint)?;
    let channel = server.connect().await?;
    Ok(Authed::new(channel, Auth::Machine(Arc::new(src))))
}
