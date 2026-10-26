//! Deltas (docs/scm.md, "Deltas"): a file written as copies from a base the client holds and the
//! bytes the base does not have. What the SCM sends is a delta compressed
//! (`CHUNK_CODEC_DELTA`); this applies what that decompresses to. The Go half is
//! internal/scm/encoding/delta.go, and testdata/delta.json there holds the two to the same
//! results.

/// The delta format `apply_delta` reads.
pub const DELTA_VERSION: u8 = 1;

/// Reads a uvarint as Go's `binary.Uvarint` does: at most ten bytes, the tenth at most 1.
pub(crate) fn read_uvarint(b: &[u8], off: &mut usize) -> Option<u64> {
    let mut v: u64 = 0;
    for i in 0..10 {
        let c = *b.get(*off + i)?;
        if i == 9 && c > 1 {
            return None;
        }
        v |= u64::from(c & 0x7f) << (7 * i);
        if c < 0x80 {
            *off += i + 1;
            return Some(v);
        }
    }
    None
}

/// The file a delta makes of `base`: refused for a base of another length, one making more than
/// `limit` bytes, and one whose copies reach past the base or whose inserts past its end.
pub fn apply_delta(base: &[u8], delta: &[u8], limit: u64) -> Result<Vec<u8>, String> {
    if delta.first() != Some(&DELTA_VERSION) {
        return Err("delta: not a version 1 delta".into());
    }
    let mut off = 1;
    let next = |off: &mut usize| read_uvarint(delta, off).ok_or("delta: truncated");
    let base_len = next(&mut off)?;
    if base_len != base.len() as u64 {
        return Err(format!(
            "delta: made against a base of {base_len} bytes, given {}",
            base.len()
        ));
    }
    let length = next(&mut off)?;
    if length > limit {
        return Err(format!("delta: makes {length} bytes, more than {limit}"));
    }
    let mut out = Vec::with_capacity(length as usize);
    while off < delta.len() {
        let tag = next(&mut off)?;
        let n = tag >> 1;
        if n > length - out.len() as u64 {
            return Err("delta: makes more than its length".into());
        }
        let n = n as usize;
        if tag & 1 == 1 {
            let Some(bytes) = delta.get(off..off + n) else {
                return Err("delta: an insert past its end".into());
            };
            out.extend_from_slice(bytes);
            off += n;
            continue;
        }
        let at = next(&mut off)?;
        let Some(bytes) = usize::try_from(at)
            .ok()
            .and_then(|at| base.get(at..at.checked_add(n)?))
        else {
            return Err("delta: a copy past the base's end".into());
        };
        out.extend_from_slice(bytes);
    }
    if out.len() as u64 != length {
        return Err(format!("delta: made {} bytes of {length}", out.len()));
    }
    Ok(out)
}
