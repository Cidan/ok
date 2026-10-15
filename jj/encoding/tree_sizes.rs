//! Tree sizes records (docs/scm.md, "Tree sizes"): what a tree's entries weigh, one size
//! per entry in the tree's order — a file's length, a symlink target's, 0 for a subtree. Derived
//! by the SCM and kept under the tree's ID; not a jj object, and never hashed. The Go half is
//! internal/scm/encoding/treesizes.go, and testdata/tree_sizes.json there holds the two to
//! identical bytes.

use crate::Code;
use crate::Error;

/// A tree sizes record's first byte. It names no object: `decode` refuses it.
pub const KIND_TREE_SIZES: u8 = 0x20;

/// The record format `encode_tree_sizes` writes and `decode_tree_sizes` reads.
pub const TREE_SIZES_VERSION: u8 = 1;

/// A tree's entries' sizes, in its order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TreeSizes {
    pub sizes: Vec<u64>,
}

/// The record: the kind byte, the version, the entry count, and each size, the count and sizes
/// as minimal unsigned LEB128 varints.
pub fn encode_tree_sizes(s: &TreeSizes) -> Vec<u8> {
    let mut b = Vec::with_capacity(3 + s.sizes.len() * 2);
    b.push(KIND_TREE_SIZES);
    b.push(TREE_SIZES_VERSION);
    uvarint(&mut b, s.sizes.len() as u64);
    for &n in &s.sizes {
        uvarint(&mut b, n);
    }
    b
}

fn uvarint(b: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        b.push((n as u8) | 0x80);
        n >>= 7;
    }
    b.push(n as u8);
}

/// Reads a record `encode_tree_sizes` wrote, refusing any other bytes, with the first violation
/// in encoding order, as the Go decoder does.
pub fn decode_tree_sizes(b: &[u8]) -> Result<TreeSizes, Error> {
    let fail = |offset: usize, code: Code, detail: String| Error {
        code,
        offset: Some(offset),
        detail,
    };
    let Some(&kind) = b.first() else {
        return Err(fail(0, Code::Truncated, "no kind byte".into()));
    };
    if kind != KIND_TREE_SIZES {
        return Err(fail(
            0,
            Code::Kind,
            format!("kind {kind:#04x}, not tree sizes"),
        ));
    }
    let Some(&version) = b.get(1) else {
        return Err(fail(1, Code::Truncated, "no version".into()));
    };
    if version != TREE_SIZES_VERSION {
        return Err(fail(1, Code::Version, format!("version {version}")));
    }
    let mut off = 2;
    let read = |off: &mut usize| -> Result<u64, Error> {
        let start = *off;
        let mut value: u64 = 0;
        // Go's binary.Uvarint, step for step: the input ending first is truncated; an eleventh
        // byte, or a tenth that ends the varint above 1, exceeds 64 bits.
        for i in 0.. {
            let Some(&byte) = b.get(start + i) else {
                return Err(fail(
                    start,
                    Code::Truncated,
                    "a varint ends at the end of the input".into(),
                ));
            };
            if i == 10 {
                return Err(fail(start, Code::Varint, "a varint exceeds 64 bits".into()));
            }
            if byte < 0x80 {
                if i == 9 && byte > 1 {
                    return Err(fail(start, Code::Varint, "a varint exceeds 64 bits".into()));
                }
                if i > 0 && byte == 0 {
                    return Err(fail(start, Code::Varint, "a varint is not minimal".into()));
                }
                value |= u64::from(byte) << (7 * i);
                *off = start + i + 1;
                return Ok(value);
            }
            value |= u64::from(byte & 0x7f) << (7 * i);
        }
        unreachable!()
    };
    let count = read(&mut off)?;
    let left = (b.len() - off) as u64;
    if count > left {
        return Err(fail(
            off,
            Code::Truncated,
            format!("{count} entries, but {left} bytes left"),
        ));
    }
    let mut sizes = Vec::with_capacity(count as usize);
    for _ in 0..count {
        sizes.push(read(&mut off)?);
    }
    if off != b.len() {
        return Err(fail(
            off,
            Code::Trailing,
            format!("{} bytes after the last size", b.len() - off),
        ));
    }
    Ok(TreeSizes { sizes })
}
