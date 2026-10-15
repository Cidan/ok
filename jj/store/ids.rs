//! Object keys and the SCM's fixed IDs.

use ok_encoding::ID_LEN;
use ok_encoding::Kind;

/// An object's 32-byte ID.
pub type Id = [u8; ID_LEN];

/// The root commit, root operation, and root view: all zero, virtual on both sides.
pub const ROOT_ID: Id = [0; ID_LEN];

/// An object by kind and ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    pub kind: Kind,
    pub id: Id,
}

impl Key {
    pub fn new(kind: Kind, id: Id) -> Self {
        Self { kind, id }
    }

    pub fn hex(&self) -> String {
        hex::encode(self.id)
    }
}

/// The 32-byte ID of a jj object ID, or None for any other length.
pub fn id_of(id: &impl jj_lib::object_id::ObjectId) -> Option<Id> {
    id.as_bytes().try_into().ok()
}

/// Converts wire bytes to an ID.
pub fn id_from(bytes: &[u8]) -> Option<Id> {
    bytes.try_into().ok()
}

/// The canonical kind byte of a wire `ObjectKind`.
pub fn kind_from_wire(k: i32) -> Option<Kind> {
    u8::try_from(k).ok().and_then(Kind::from_byte)
}

/// The wire `ObjectKind` of a kind.
pub fn kind_to_wire(k: Kind) -> i32 {
    k as i32
}
