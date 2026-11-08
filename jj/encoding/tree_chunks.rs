//! Large trees (docs/scm.md, "Large trees"): a tree whose canonical body is over
//! `TREE_CHUNK_THRESHOLD` is published as chunks of its body, cut where its entries say — its entry
//! count, then runs of whole entries ended by their names' BLAKE3 — so an edit of one entry sends a
//! chunk or two. The Go half is internal/scm/encoding/treechunks.go, which the SCM holds a publish's
//! chunks to, and testdata/tree_chunks.json there holds the two to the same chunks.

/// The largest tree body published whole.
pub const TREE_CHUNK_THRESHOLD: usize = 128 << 10;
const LEAF_MIN: usize = 16 << 10;
const LEAF_AVG: usize = 64 << 10;
const LEAF_MAX: usize = 256 << 10;
const COUNT_LEN: usize = 8;

/// Cuts a tree's canonical body (after its kind byte), over `TREE_CHUNK_THRESHOLD`, into its chunks:
/// the entry count, then runs of whole entries. They join to the body. A body that does not decode
/// as a tree's, or has an entry longer than a chunk may be, is not cut.
pub fn chunk_tree(body: &[u8]) -> Result<Vec<&[u8]>, String> {
    let mut off = 0;
    let n = u64_at(body, &mut off)?;
    let mut out = vec![&body[..COUNT_LEN]];
    let mut start = off;
    for i in 0..n {
        let entry = off;
        let name_len = u64_at(body, &mut off)?;
        let name = take(body, &mut off, name_len)?;
        match *take(body, &mut off, 1)?.first().expect("1 byte") {
            0 => {
                take(body, &mut off, 32 + 1)?;
                let copy_len = u64_at(body, &mut off)?;
                take(body, &mut off, copy_len)?;
            }
            1 | 2 => {
                take(body, &mut off, 32)?;
            }
            tag => return Err(format!("tree: entry {i} has tag {tag}")),
        }
        if off - entry > LEAF_MAX {
            return Err(format!("tree: entry {i} is {} bytes", off - entry));
        }
        if off - start > LEAF_MAX {
            out.push(&body[start..entry]);
            start = entry;
        }
        if (off - start >= LEAF_MIN && cut_after(name, off - entry)) || i + 1 == n {
            out.push(&body[start..off]);
            start = off;
        }
    }
    if off != body.len() {
        return Err(format!(
            "tree: {} bytes after its entries",
            body.len() - off
        ));
    }
    Ok(out)
}

fn cut_after(name: &[u8], entry: usize) -> bool {
    let h = crate::hash(name);
    let x = u64::from(u32::from_le_bytes(h[0..4].try_into().expect("4 bytes")));
    x < ((entry as u64) << 32) / (LEAF_AVG - LEAF_MIN) as u64
}

fn u64_at(b: &[u8], off: &mut usize) -> Result<u64, String> {
    let v = take(b, off, 8)?;
    Ok(u64::from_le_bytes(v.try_into().expect("8 bytes")))
}

fn take<'a>(b: &'a [u8], off: &mut usize, n: u64) -> Result<&'a [u8], String> {
    let n = usize::try_from(n).map_err(|_| "tree: truncated".to_owned())?;
    let v = b
        .get(*off..off.checked_add(n).ok_or("tree: truncated")?)
        .ok_or("tree: truncated")?;
    *off += n;
    Ok(v)
}
