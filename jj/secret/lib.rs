//! Secrets in ok's jj (docs/secrets.md): where a repository's secrets are (`//secret`), the
//! pointer a secret's path holds, the commitment it names, and a buffer for a value that is
//! cleared when dropped. The Go model (`internal/secrets`) implements the same, held to the same
//! golden vectors.

use std::fmt;

use subtle::ConstantTimeEq as _;
use zeroize::Zeroize as _;

/// The folder, at a repository's root, that holds its secrets.
pub const DIR: &str = "secret";

/// The file in each of the tree's folders naming the sets that may use its secrets.
pub const ACL_FILE: &str = "acl.yaml";

/// The largest value a secret holds.
pub const MAX_VALUE: usize = 1 << 20;

/// The longest name a folder or a secret may have.
pub const MAX_NAME_LEN: usize = 255;

/// BLAKE3's derive-key context for commitments.
pub const COMMITMENT_CONTEXT: &str = "ok 2026-10-22 secret commitment v1";

const POINTER_PREFIX: &str = "ok-secret v1 blake3:";

/// The length of every pointer, in bytes.
pub const POINTER_LEN: usize = POINTER_PREFIX.len() + 64 + 1;

/// A secret's path from the repository's root, as a person or a stanza writes it (`//secret/…`),
/// or the repository path itself, as the path; `None` for anything that is no secret's valid path.
pub fn parse_ref(s: &str) -> Option<&str> {
    let p = s.strip_prefix("//").unwrap_or(s);
    valid_path(p).then_some(p)
}

/// Whether `s` may name a folder or a secret: `[A-Za-z0-9][A-Za-z0-9._-]*`, at most 255 bytes.
pub fn valid_name(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= MAX_NAME_LEN
        && b[0].is_ascii_alphanumeric()
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
}

/// Whether a slash-separated repository path is in the `//secret` tree: the folder, or beneath it.
pub fn in_tree(p: &str) -> bool {
    p == DIR || p.strip_prefix(DIR).is_some_and(|r| r.starts_with('/'))
}

/// Whether a repository path is a secret's: a file in the tree that is neither an `acl.yaml` nor
/// an OWNERS-config file.
pub fn is_secret(p: &str) -> bool {
    p.strip_prefix(DIR)
        .is_some_and(|r| r.starts_with('/') && secret_name(base(p)))
}

/// Whether a file of this name in the tree is a secret: it is neither `acl.yaml` nor an
/// OWNERS-config file's name.
pub fn secret_name(name: &str) -> bool {
    name != ACL_FILE && !owners_name(name)
}

/// Whether a repository path is an OWNERS-config file in the tree: its approval list, as anywhere
/// else in the repository, and no secret.
pub fn is_owners(p: &str) -> bool {
    p.strip_prefix(DIR)
        .is_some_and(|r| r.starts_with('/') && owners_name(base(p)))
}

/// An OWNERS-config file's name, as `internal/owners` matches it: `OWNERS`,
/// `<name>_OWNERS`, or `OWNERS_<name>`, each `<name>` of letters, digits, `_`, and `-`.
fn owners_name(name: &str) -> bool {
    let word = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    };
    name == "OWNERS"
        || name.strip_suffix("_OWNERS").is_some_and(word)
        || name.strip_prefix("OWNERS_").is_some_and(word)
}

/// Whether a repository path is one of the tree's `acl.yaml` files.
pub fn is_acl(p: &str) -> bool {
    p.strip_prefix(DIR)
        .is_some_and(|r| r.starts_with('/') && base(p) == ACL_FILE)
}

/// Whether `p` may be a secret's path: in the tree, every part below `secret/` a valid name, and
/// the last neither `acl.yaml` nor an OWNERS-config file's name.
pub fn valid_path(p: &str) -> bool {
    match p.strip_prefix("secret/") {
        Some(rest) => is_secret(p) && rest.split('/').all(valid_name),
        None => false,
    }
}

fn base(p: &str) -> &str {
    p.rsplit('/').next().unwrap_or(p)
}

/// A pointer's commitment to a secret's value.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Digest(pub [u8; 32]);

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest({})", hex(&self.0))
    }
}

impl Digest {
    /// The digest in lowercase hex.
    pub fn hex(&self) -> String {
        hex(&self.0)
    }
}

fn hex(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push(HEX[(x >> 4) as usize] as char);
        s.push(HEX[(x & 15) as usize] as char);
    }
    s
}

/// The line a secret's path holds: `ok-secret v1 blake3:<64 lowercase hex digits>\n`.
pub fn pointer(d: &Digest) -> Vec<u8> {
    let mut out = Vec::with_capacity(POINTER_LEN);
    out.extend_from_slice(POINTER_PREFIX.as_bytes());
    out.extend_from_slice(d.hex().as_bytes());
    out.push(b'\n');
    out
}

/// Reads a pointer: exactly the line [`pointer`] writes.
pub fn parse_pointer(b: &[u8]) -> Option<Digest> {
    if b.len() != POINTER_LEN
        || !b.starts_with(POINTER_PREFIX.as_bytes())
        || b[b.len() - 1] != b'\n'
    {
        return None;
    }
    let h = &b[POINTER_PREFIX.len()..b.len() - 1];
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut d = [0u8; 32];
    for (i, pair) in h.chunks(2).enumerate() {
        d[i] = nibble(pair[0])? << 4 | nibble(pair[1])?;
    }
    Some(Digest(d))
}

/// The digest committing to a value at a path of a repository, with an opening (docs/secrets.md,
/// "Pointers and commitments").
pub fn commit(repository: i64, path: &str, opening: &[u8; 32], value: &[u8]) -> Digest {
    let mut h = blake3::Hasher::new_derive_key(COMMITMENT_CONTEXT);
    h.update(&(repository as u64).to_le_bytes());
    h.update(&(path.len() as u64).to_le_bytes());
    h.update(path.as_bytes());
    h.update(opening);
    h.update(value);
    Digest(*h.finalize().as_bytes())
}

/// Whether a value and opening are the ones a digest commits to, at a path of a repository.
pub fn verify(d: &Digest, repository: i64, path: &str, opening: &[u8; 32], value: &[u8]) -> bool {
    commit(repository, path, opening, value)
        .0
        .ct_eq(&d.0)
        .into()
}

/// A value's BLAKE3-256, kept in memory to tell a value rewritten as it was from a new one.
pub fn value_hash(value: &[u8]) -> [u8; 32] {
    *blake3::hash(value).as_bytes()
}

/// A secret's value in memory: cleared when dropped, and never printed.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretBuf(Vec<u8>);

impl SecretBuf {
    pub fn new(v: Vec<u8>) -> Self {
        Self(v)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Writes `data` at `offset`, growing the value (with zeros between) as a file does.
    pub fn write_at(&mut self, offset: usize, data: &[u8]) {
        let end = offset + data.len();
        if end > self.0.len() {
            self.grow(end);
        }
        self.0[offset..end].copy_from_slice(data);
    }

    /// Cuts or grows the value to `len` bytes.
    pub fn set_len(&mut self, len: usize) {
        if len < self.0.len() {
            self.0[len..].zeroize();
            self.0.truncate(len);
        } else {
            self.grow(len);
        }
    }

    /// Grows to `len`, moving the value to a new buffer without leaving a copy behind.
    fn grow(&mut self, len: usize) {
        if len > self.0.capacity() {
            let mut next = Vec::with_capacity(len.max(self.0.capacity() * 2));
            next.extend_from_slice(&self.0);
            self.0.zeroize();
            self.0 = next;
        }
        self.0.resize(len, 0);
    }
}

impl Drop for SecretBuf {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for SecretBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretBuf({} bytes)", self.0.len())
    }
}

#[cfg(test)]
mod tests;
