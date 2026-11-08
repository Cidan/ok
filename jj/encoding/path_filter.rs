//! A commit's changed-path filter (docs/scm.md, "The commit graph"): a Bloom filter of the paths it
//! changed from its one parent — each changed file and every directory above one — which a graph
//! record carries. The Go half, which makes them, is internal/scm/encoding/pathfilter.go, and
//! testdata/path_filter.json there holds the two to the same answers.

/// How many bits a path sets.
pub const PATH_FILTER_PROBES: u64 = 7;

/// Whether `filter` may hold `path`: false is certain. The empty filter holds no path; the one byte
/// 0xff, a commit's that changed more paths than a filter holds, every path.
pub fn path_filter_holds(filter: &[u8], path: &[u8]) -> bool {
    if filter.is_empty() {
        return false;
    }
    let h = crate::hash(path);
    let h1 = u64::from(u32::from_le_bytes(h[0..4].try_into().expect("4 bytes")));
    let h2 = u64::from(u32::from_le_bytes(h[4..8].try_into().expect("4 bytes")));
    let bits = filter.len() as u64 * 8;
    (0..PATH_FILTER_PROBES).all(|i| {
        let b = (h1 + i * h2) % bits;
        filter[(b / 8) as usize] & (1 << (b % 8)) != 0
    })
}
