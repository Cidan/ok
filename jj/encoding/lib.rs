//! ok SCM's canonical object encoding for the jj client: the single byte form of every
//! object the SCM stores, and the BLAKE3-256 IDs computed over it (docs/scm.md,
//! "Objects and identity"). The SCM implements the same encoding in Go
//! (internal/scm/encoding); the golden vectors there hold the two to identical bytes,
//! IDs, and rejections.
//!
//! An object's canonical bytes are its kind byte followed by its body, and its ID is the
//! BLAKE3-256 digest of those bytes. Objects are jj-lib's own types, so the stores hand jj
//! back exactly what it wrote. [`decode`] accepts only canonical bytes: re-encoding a
//! decoded object reproduces its input.

use std::fmt;

use jj_lib::backend::{Commit, CopyHistory, Tree};
use jj_lib::op_store::{Operation, View};

mod decode;
mod delta;
mod encode;
mod layer;
#[cfg(test)]
mod tests;
mod tree_sizes;

pub use decode::decode;
pub use delta::{DELTA_VERSION, apply_delta};
pub use encode::{
    encode, encode_chunk, encode_commit, encode_copy_history, encode_file, encode_operation,
    encode_symlink, encode_tree, encode_view, signed_data,
};
pub use layer::{LayerRecord, decode_layer_records};
pub use tree_sizes::{
    KIND_TREE_SIZES, TREE_SIZES_VERSION, TreeSizes, decode_tree_sizes, encode_tree_sizes,
};

/// The length of every object ID: a BLAKE3-256 digest.
pub const ID_LEN: usize = 32;

/// The length of a change ID, which the client generates at random.
pub const CHANGE_ID_LEN: usize = 16;

/// An object's kind byte. It prefixes the body both in the canonical bytes and in the
/// hashed input, so objects of different kinds never share an ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Kind {
    File = 0x01,
    Symlink = 0x02,
    Tree = 0x03,
    CopyHistory = 0x04,
    Commit = 0x05,
    Operation = 0x06,
    View = 0x07,
    /// A stored piece of a file's contents. It is not a jj object, and a file's ID never
    /// depends on how it was chunked.
    Chunk = 0x10,
}

impl Kind {
    /// The kind a byte names, if any.
    pub fn from_byte(b: u8) -> Option<Self> {
        Some(match b {
            0x01 => Self::File,
            0x02 => Self::Symlink,
            0x03 => Self::Tree,
            0x04 => Self::CopyHistory,
            0x05 => Self::Commit,
            0x06 => Self::Operation,
            0x07 => Self::View,
            0x10 => Self::Chunk,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Symlink => "symlink",
            Self::Tree => "tree",
            Self::CopyHistory => "copy_history",
            Self::Commit => "commit",
            Self::Operation => "operation",
            Self::View => "view",
            Self::Chunk => "chunk",
        }
    }
}

/// One decoded object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Object {
    File(Vec<u8>),
    Symlink(String),
    Tree(Tree),
    CopyHistory(CopyHistory),
    Commit(Commit),
    Operation(Operation),
    View(View),
    Chunk(Vec<u8>),
}

impl Object {
    pub fn kind(&self) -> Kind {
        match self {
            Self::File(_) => Kind::File,
            Self::Symlink(_) => Kind::Symlink,
            Self::Tree(_) => Kind::Tree,
            Self::CopyHistory(_) => Kind::CopyHistory,
            Self::Commit(_) => Kind::Commit,
            Self::Operation(_) => Kind::Operation,
            Self::View(_) => Kind::View,
            Self::Chunk(_) => Kind::Chunk,
        }
    }
}

/// Why bytes do not decode, or a value does not encode. The codes are shared with
/// internal/scm/encoding, and the golden vectors name them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Code {
    /// The input ends inside a value, or a length or count exceeds the bytes remaining
    /// when it is read.
    Truncated,
    /// Bytes follow a complete object.
    Trailing,
    /// The kind byte names no object.
    Kind,
    /// A bool byte above 1.
    Bool,
    /// An opt tag above 1.
    Option,
    /// A str, or a symlink target, that is not valid UTF-8.
    Utf8,
    /// Set elements, map keys, or tree entry names out of increasing order.
    Unsorted,
    /// A set element, map key, or tree entry name that repeats.
    Duplicate,
    /// A merge with an even number of terms, or none.
    Merge,
    /// A tree value other than a file, symlink, or tree.
    Tag,
    /// A tree entry name that is empty, ".", or "..", or contains '/' or NUL.
    Name,
    /// A copy history path that is empty or has a component that is not a valid tree
    /// entry name.
    Path,
    /// A file's copy ID that is neither empty nor 32 bytes.
    CopyId,
    /// A remote ref state above 1.
    State,
    /// A tree sizes record of a version this does not read.
    Version,
    /// A varint that is not minimal, or exceeds 64 bits.
    Varint,
    /// Encoding only: an object ID that is not 32 bytes, or a change ID that is not 16.
    IdLength,
    /// Encoding only: a commit's `secure_sig.data` that is not the commit's signed data.
    SignedData,
}

impl Code {
    /// The code's name, as the golden vectors spell it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Truncated => "truncated",
            Self::Trailing => "trailing",
            Self::Kind => "kind",
            Self::Bool => "bool",
            Self::Option => "option",
            Self::Utf8 => "utf8",
            Self::Unsorted => "unsorted",
            Self::Duplicate => "duplicate",
            Self::Merge => "merge",
            Self::Tag => "tag",
            Self::Name => "name",
            Self::Path => "path",
            Self::CopyId => "copy-id",
            Self::State => "state",
            Self::Version => "version",
            Self::Varint => "varint",
            Self::IdLength => "id-length",
            Self::SignedData => "signed-data",
        }
    }
}

/// A value that does not decode or encode canonically.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub code: Code,
    /// The byte offset in the input at which decoding failed; `None` for an encoding
    /// error.
    pub offset: Option<usize>,
    pub detail: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.offset {
            Some(offset) => write!(
                f,
                "decoding at byte {offset}: {}: {}",
                self.code.as_str(),
                self.detail
            ),
            None => write!(f, "encoding: {}: {}", self.code.as_str(), self.detail),
        }
    }
}

impl std::error::Error for Error {}

/// The ID of an object, from its canonical bytes.
pub fn hash(canonical: &[u8]) -> [u8; ID_LEN] {
    *blake3::hash(canonical).as_bytes()
}

/// The empty tree's ID: `BLAKE3(0x03 ‖ u64 0)`. It is virtual on both ends, never stored
/// and always present.
pub fn empty_tree_id() -> [u8; ID_LEN] {
    hash(&[Kind::Tree as u8, 0, 0, 0, 0, 0, 0, 0, 0])
}

/// Computes an object's ID from a body written in pieces, for file contents too large to
/// hold in memory. `finalize` equals [`hash`] of the kind byte followed by everything
/// written.
#[derive(Clone, Debug)]
pub struct Hasher(blake3::Hasher);

impl Hasher {
    pub fn new(kind: Kind) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(&[kind as u8]);
        Self(h)
    }

    pub fn update(&mut self, body: &[u8]) {
        self.0.update(body);
    }

    pub fn finalize(&self) -> [u8; ID_LEN] {
        *self.0.finalize().as_bytes()
    }
}

/// Whether `name` can name a tree entry: non-empty, neither "." nor "..", and free of '/'
/// and NUL. jj itself forbids only empty names and '/'.
fn valid_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\0'])
}

/// Whether `path` is non-empty and every '/'-separated component is a valid tree entry
/// name.
fn valid_path(path: &str) -> bool {
    !path.is_empty() && path.split('/').all(valid_name)
}
