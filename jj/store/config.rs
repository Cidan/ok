//! `.jj/repo/store/ok.toml`: which SCM repository a jj repository is.

use std::io;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

/// The file under the backend's store directory.
pub const FILE: &str = "ok.toml";

/// The SCM repository a jj repository mirrors.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RepoConfig {
    /// The SCM endpoint, e.g. `https://ok.example.com`.
    pub endpoint: String,
    /// The repository's stable id.
    pub repository_id: i64,
    /// Its name when the repository was cloned (names can change; the id cannot).
    pub repository: String,
    /// For a loopback development SCM only: the principal this clone acts as, recorded
    /// from `OK_DEV_PRINCIPAL` when it was cloned, so one machine can hold clones
    /// as different people. A real SCM takes the principal from the credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dev_principal: Option<String>,
    /// Where this repository's cache is, when not the machine's: tests give each machine they
    /// stand in for its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<PathBuf>,
    /// The workspace this repository is — one jj repository per workspace — whose operation log,
    /// on the SCM, is its own (docs/scm.md, "Decisions", 3). None for a presentation's
    /// bare store, which has no log, and for a repository cloned before each workspace had its own
    /// log, which ok no longer serves (`jj clone` replaces it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

impl RepoConfig {
    /// Reads the config from a store directory.
    pub fn read(store_path: &Path) -> io::Result<Self> {
        let text = std::fs::read_to_string(store_path.join(FILE))?;
        toml::from_str(&text)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {e}", FILE)))
    }

    /// The repository's cache: the machine's for its SCM and id, shared by every clone and
    /// virtual workspace of it — under `$OK_CACHE_DIR`, `$XDG_CACHE_HOME/ok`, or
    /// `~/.cache/ok` — unless the config names one.
    pub fn cache_dir(&self) -> io::Result<PathBuf> {
        if let Some(dir) = &self.cache {
            return Ok(dir.clone());
        }
        let base = match (
            std::env::var_os("OK_CACHE_DIR"),
            std::env::var_os("XDG_CACHE_HOME"),
            std::env::var_os("HOME"),
        ) {
            (Some(d), _, _) if !d.is_empty() => PathBuf::from(d),
            (_, Some(d), _) if !d.is_empty() => PathBuf::from(d).join("ok"),
            (_, _, Some(h)) if !h.is_empty() => PathBuf::from(h).join(".cache/ok"),
            _ => return Err(io::Error::other("no HOME for the repository's cache")),
        };
        if !base.exists() {
            std::fs::create_dir_all(&base)?;
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(base
            .join(endpoint_key(&self.endpoint))
            .join(self.repository_id.to_string()))
    }

    /// Writes the config into a store directory.
    pub fn write(&self, store_path: &Path) -> io::Result<()> {
        let text =
            toml::to_string(self).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        std::fs::write(store_path.join(FILE), text)
    }
}

/// An SCM endpoint as a directory name: its host and port (`ok.example.com`, `127.0.0.1-41234`).
fn endpoint_key(endpoint: &str) -> String {
    let rest = endpoint.split_once("://").map_or(endpoint, |(_, r)| r);
    let authority = rest.split('/').next().unwrap_or_default();
    let key: String = authority
        .chars()
        .map(|c| match c {
            ':' => '-',
            c if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' => c,
            _ => '_',
        })
        .collect();
    if key.is_empty() || key.starts_with('.') {
        format!("_{key}")
    } else {
        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_name_their_caches() {
        assert_eq!(endpoint_key("https://ok.example.com"), "ok.example.com");
        assert_eq!(endpoint_key("http://127.0.0.1:41234/"), "127.0.0.1-41234");
        assert_eq!(endpoint_key("https://[::1]:9"), "_--1_-9");
        assert_eq!(endpoint_key("../../etc"), "_..");
        assert_eq!(endpoint_key("http://a b/c"), "a_b");
    }
}
