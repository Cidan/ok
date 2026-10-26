//! A checkout layer's records (docs/scm.md, "Checkouts"): objects of a commit's checkout — trees,
//! symlinks, copy histories, the files planning reads — and trees' sizes records, which a client
//! takes into its cache as if it had read each. A layer stored or sent is these records
//! compressed; this reads what that decompresses to. The Go half is
//! internal/scm/encoding/layer.go, and testdata/layer.json there holds the two to the same records.

use crate::Kind;
use crate::delta::read_uvarint;
use crate::tree_sizes::KIND_TREE_SIZES;
use crate::tree_sizes::decode_tree_sizes;

/// One record of a layer: its kind byte (an object's, or `KIND_TREE_SIZES`), its ID (the
/// object's, or a sizes record's tree's), and its body (the object's canonical bytes after its
/// kind byte, or the sizes record).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerRecord {
    pub kind: u8,
    pub id: [u8; 32],
    pub body: Vec<u8>,
}

fn held_kind(k: u8) -> bool {
    k == Kind::File as u8
        || k == Kind::Symlink as u8
        || k == Kind::Tree as u8
        || k == Kind::CopyHistory as u8
        || k == KIND_TREE_SIZES
}

/// Reads a layer's records, refusing one out of order or repeated, of a kind a layer does not
/// hold, an object whose body does not hash to its ID, and a sizes record that does not decode.
pub fn decode_layer_records(b: &[u8]) -> Result<Vec<LayerRecord>, String> {
    let mut off = 0;
    let count = read_uvarint(b, &mut off).ok_or("layer: truncated")?;
    if count > ((b.len() - off) / (1 + 32 + 1)) as u64 {
        return Err(format!("layer: {count} records in {} bytes", b.len() - off));
    }
    let mut out: Vec<LayerRecord> = Vec::with_capacity(count as usize);
    for i in 0..count {
        let Some(head) = b.get(off..off + 33) else {
            return Err("layer: truncated".into());
        };
        let kind = head[0];
        let id: [u8; 32] = head[1..].try_into().expect("32 bytes");
        off += 33;
        let length = read_uvarint(b, &mut off).ok_or("layer: truncated")?;
        let Some(body) = usize::try_from(length)
            .ok()
            .and_then(|n| b.get(off..off.checked_add(n)?))
        else {
            return Err("layer: truncated".into());
        };
        off += body.len();
        if !held_kind(kind) {
            return Err(format!("layer: record {i} is of kind {kind:#04x}"));
        }
        if let Some(prev) = out.last()
            && (prev.kind, prev.id) >= (kind, id)
        {
            return Err(format!("layer: record {i} is out of order"));
        }
        if kind == KIND_TREE_SIZES {
            decode_tree_sizes(body)
                .map_err(|e| format!("layer: tree {}'s sizes: {e}", hex(&id)))?;
        } else {
            let mut canonical = Vec::with_capacity(body.len() + 1);
            canonical.push(kind);
            canonical.extend_from_slice(body);
            if crate::hash(&canonical) != id {
                return Err(format!(
                    "layer: an object of kind {kind:#04x}, {}, does not hash to its ID",
                    hex(&id)
                ));
            }
        }
        out.push(LayerRecord {
            kind,
            id,
            body: body.to_vec(),
        });
    }
    if off != b.len() {
        return Err(format!("layer: {} bytes after its records", b.len() - off));
    }
    Ok(out)
}

fn hex(id: &[u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
