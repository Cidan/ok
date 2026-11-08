//! The commit graph's records (docs/scm.md, "The commit graph"): what a client's index of a
//! repository's commits needs of each — its ID, change ID, generation, committer's time, tree, and
//! parents — and, where the SCM derived it, a filter of the paths it changed, without the commit's
//! body. A main graph layer stored or sent is these records compressed; this reads what that
//! decompresses to, and the records ReadGraph sends as they are. The Go half is
//! internal/scm/encoding/graph.go, and testdata/graph.json there holds the two to the same records.

use std::collections::HashMap;

use crate::delta::read_uvarint;

/// One commit's record in the commit graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphRecord {
    pub id: [u8; 32],
    pub change_id: Vec<u8>,
    /// 1 + the highest parent's; the root's 0.
    pub generation: u64,
    /// The committer's time: milliseconds since the Unix epoch, and minutes east of UTC.
    pub committed_millis: i64,
    pub tz_offset: i32,
    /// The commit's tree, as a merge's terms: one when it has no conflict.
    pub tree: Vec<[u8; 32]>,
    pub parents: Vec<[u8; 32]>,
    /// The commit's changed-path filter, where the SCM derived one.
    pub filter: Option<Vec<u8>>,
}

const PARENT_BEFORE: u8 = 1;
const FILTER: u8 = 2;
const MAX_TERMS: u64 = 255;

fn read_varint(b: &[u8], off: &mut usize) -> Option<i64> {
    let u = read_uvarint(b, off)?;
    Some(((u >> 1) as i64) ^ -((u & 1) as i64))
}

/// Reads records, refusing truncated ones, unknown flags, a tree of an even number of terms or
/// none, a record twice, and one whose generation is not above a parent's the records hold. The
/// parent a record shares with the one before it is named.
pub fn decode_graph_records(b: &[u8]) -> Result<Vec<GraphRecord>, String> {
    const TRUNCATED: &str = "graph: truncated";
    let mut off = 0;
    let count = read_uvarint(b, &mut off).ok_or(TRUNCATED)?;
    if count > ((b.len() - off) / (1 + 32 + 1 + 1 + 1 + 1 + 1 + 32)) as u64 {
        return Err(format!("graph: {count} records in {} bytes", b.len() - off));
    }
    let id = |off: &mut usize| -> Result<[u8; 32], String> {
        let v: [u8; 32] = b
            .get(*off..*off + 32)
            .ok_or(TRUNCATED)?
            .try_into()
            .expect("32 bytes");
        *off += 32;
        Ok(v)
    };
    let bytes = |off: &mut usize| -> Result<Vec<u8>, String> {
        let n = read_uvarint(b, off).ok_or(TRUNCATED)?;
        let v = usize::try_from(n)
            .ok()
            .and_then(|n| b.get(*off..off.checked_add(n)?))
            .ok_or(TRUNCATED)?;
        *off += v.len();
        Ok(v.to_vec())
    };
    let mut out: Vec<GraphRecord> = Vec::with_capacity(count as usize);
    let mut gens: HashMap<[u8; 32], u64> = HashMap::with_capacity(count as usize);
    for i in 0..count {
        let flags = *b.get(off).ok_or(TRUNCATED)?;
        off += 1;
        if flags & !(PARENT_BEFORE | FILTER) != 0 {
            return Err(format!("graph: record {i} has flags {flags:#x}"));
        }
        let rid = id(&mut off)?;
        let change_id = bytes(&mut off)?;
        let generation = read_uvarint(b, &mut off).ok_or(TRUNCATED)?;
        let committed_millis = read_varint(b, &mut off).ok_or(TRUNCATED)?;
        let zone = read_varint(b, &mut off).ok_or(TRUNCATED)?;
        if !(-24 * 60..=24 * 60).contains(&zone) {
            return Err(format!("graph: record {i}'s time zone is {zone} minutes"));
        }
        let terms = read_uvarint(b, &mut off).ok_or(TRUNCATED)?;
        if terms % 2 == 0 || terms > MAX_TERMS {
            return Err(format!("graph: record {i}'s tree has {terms} terms"));
        }
        let mut tree = Vec::with_capacity(terms as usize);
        for _ in 0..terms {
            tree.push(id(&mut off)?);
        }
        let parents = if flags & PARENT_BEFORE != 0 {
            let Some(prev) = out.last() else {
                return Err("graph: the first record's parent is a record before it".into());
            };
            vec![prev.id]
        } else {
            let k = read_uvarint(b, &mut off).ok_or(TRUNCATED)?;
            if k > ((b.len() - off) / 32) as u64 {
                return Err(TRUNCATED.into());
            }
            let mut ps = Vec::with_capacity(k as usize);
            for _ in 0..k {
                ps.push(id(&mut off)?);
            }
            ps
        };
        let filter = if flags & FILTER != 0 {
            Some(bytes(&mut off)?)
        } else {
            None
        };
        if gens.contains_key(&rid) {
            return Err(format!("graph: commit {} twice", hex(&rid)));
        }
        for p in &parents {
            if let Some(&g) = gens.get(p)
                && g >= generation
            {
                return Err(format!(
                    "graph: commit {}'s generation {generation} is not above its parent's, {g}",
                    hex(&rid)
                ));
            }
        }
        gens.insert(rid, generation);
        out.push(GraphRecord {
            id: rid,
            change_id,
            generation,
            committed_millis,
            tz_offset: zone as i32,
            tree,
            parents,
            filter,
        });
    }
    if off != b.len() {
        return Err(format!("graph: {} bytes after its records", b.len() - off));
    }
    Ok(out)
}

fn hex(id: &[u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
