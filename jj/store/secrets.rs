//! A repository's secrets from ok secrets (docs/secrets.md, "Clients"): whether the repository
//! keeps them, a value read by the pointer at its path, a value kept as a new version, what the
//! caller may do in a folder, and unseals. Every value read is checked against the pointer it was
//! read by, and every version written against the value sent, as every byte ok serves is checked
//! against its ID.

use std::time::Duration;
use std::time::Instant;

use ok_proto::ok::secrets::v1 as pb;
use ok_secret::Digest;
use ok_secret::SecretBuf;
use tonic::Status;

use crate::shared::Shared;

pub use pb::UnsealResponse;
pub use pb::unseal_response::State as UnsealState;

/// What the caller may do with a folder's secrets, and why not.
#[derive(Clone, Debug)]
pub struct Access {
    /// The repository keeps secrets; when false, `secret/` is an ordinary folder.
    pub keeps_secrets: bool,
    pub read: bool,
    pub write: bool,
    /// The folder asks for a passkey to unseal its secrets.
    pub passkey: bool,
    pub why_not_read: String,
    pub why_not_write: String,
}

/// How long it is believed a repository keeps no secrets, or that ok secrets could not say: one
/// that starts to keep them is guarded within it (the SCM refuses a value at publish meanwhile).
const RECHECK: Duration = Duration::from_secs(60);

/// What ok secrets last said of whether the repository keeps secrets.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Keeps {
    keeps: bool,
    /// ok secrets said so (and did not merely fail to answer).
    said: bool,
    at: Instant,
}

/// A digest or opening ok sent, which must be 32 bytes.
fn bytes32(b: &[u8], what: &str) -> Result<[u8; 32], Status> {
    b.try_into()
        .map_err(|_| Status::data_loss(format!("ok secrets sent {what} that is not 32 bytes")))
}

impl Shared {
    /// Whether the repository keeps secrets — whether `secret/` is special in it at all — as ok
    /// secrets' `CheckAccess` says: once it does, for good; that it does not, for a minute. When it
    /// does not say (it is unreachable, or the caller is no person), it is taken to: the guard
    /// holds, and the question is asked again in a minute. Call from the store runtime.
    pub async fn keeps_secrets(&self) -> bool {
        if let Some(k) = *self.keeps_secrets.lock().unwrap()
            && ((k.keeps && k.said) || k.at.elapsed() < RECHECK)
        {
            return k.keeps;
        }
        let (keeps, said) = match self.secret_access(ok_secret::DIR).await {
            Ok(a) => (a.keeps_secrets, true),
            Err(_) => (true, false),
        };
        *self.keeps_secrets.lock().unwrap() = Some(Keeps {
            keeps,
            said,
            at: Instant::now(),
        });
        keeps
    }

    /// The value of the version at `path` its pointer's digest names, using one of the caller's
    /// unseals of `path`.
    pub async fn read_secret(&self, path: &str, digest: &Digest) -> Result<SecretBuf, Status> {
        let mut client = self.client.secrets().await?;
        let mut resp = client
            .read_secret(pb::ReadSecretRequest {
                repository_id: self.client.repository_id(),
                path: path.to_owned(),
                digest: digest.0.to_vec(),
            })
            .await?
            .into_inner();
        let value = SecretBuf::new(std::mem::take(&mut resp.value));
        let opening = bytes32(&resp.opening, "an opening")?;
        if !ok_secret::verify(
            digest,
            self.client.repository_id(),
            path,
            &opening,
            value.as_slice(),
        ) {
            return Err(Status::data_loss(format!(
                "ok secrets sent a value for {path} that its pointer does not name"
            )));
        }
        Ok(value)
    }

    /// Keeps `value` as a new version of the secret at `path`, returning its digest: the pointer
    /// the path is to hold is `ok_secret::pointer` of it.
    pub async fn write_secret(&self, path: &str, value: &[u8]) -> Result<Digest, Status> {
        let mut client = self.client.secrets().await?;
        let resp = client
            .write_secret(pb::WriteSecretRequest {
                repository_id: self.client.repository_id(),
                path: path.to_owned(),
                value: value.to_vec(),
            })
            .await?
            .into_inner();
        let digest = Digest(bytes32(&resp.digest, "a digest")?);
        let opening = bytes32(&resp.opening, "an opening")?;
        if !ok_secret::verify(&digest, self.client.repository_id(), path, &opening, value)
            || resp.pointer != ok_secret::pointer(&digest)
        {
            return Err(Status::data_loss(format!(
                "ok secrets' pointer for {path} does not name the value written"
            )));
        }
        Ok(digest)
    }

    /// Whether the caller may read and write the secrets of `path`'s folder (`secret`: the tree's
    /// root).
    pub async fn secret_access(&self, path: &str) -> Result<Access, Status> {
        let mut client = self.client.secrets().await?;
        let resp = client
            .check_access(pb::CheckAccessRequest {
                repository_id: self.client.repository_id(),
                path: path.to_owned(),
            })
            .await?
            .into_inner();
        Ok(Access {
            keeps_secrets: resp.keeps_secrets,
            read: resp.read,
            write: resp.write,
            passkey: resp.passkey,
            why_not_read: resp.why_not_read,
            why_not_write: resp.why_not_write,
        })
    }

    /// Asks to unseal each path once: granted, or an approval to make with a passkey, or one still
    /// waiting (`approval` names one an earlier call started).
    pub async fn unseal(&self, paths: &[String], approval: &str) -> Result<UnsealResponse, Status> {
        let mut client = self.client.secrets().await?;
        Ok(client
            .unseal(pb::UnsealRequest {
                repository_id: self.client.repository_id(),
                paths: paths.to_vec(),
                approval_id: approval.to_owned(),
            })
            .await?
            .into_inner())
    }

    /// The caller's unused, unexpired unseals in the repository: each path, and when it expires.
    pub async fn unseals(&self) -> Result<Vec<pb::Unseal>, Status> {
        let mut client = self.client.secrets().await?;
        Ok(client
            .list_unseals(pb::ListUnsealsRequest {
                repository_id: self.client.repository_id(),
            })
            .await?
            .into_inner()
            .unseals)
    }
}
