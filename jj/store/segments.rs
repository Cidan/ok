//! Segments: where the cache keeps its entries (docs/scm.md, "Cache, outbox, and
//! prefetch"; docs/scm.md). A file per entry costs a block and an
//! inode however small the entry is — 4 KiB and more for an 82-byte manifest — so entries are
//! appended to a few large segment files instead, each with a few bytes of header, and found
//! through immutable sorted index runs of 12 bytes an entry.
//!
//! ```text
//! meta          the segments and runs that are live (replaced whole: written aside, renamed)
//! LOCK          held by a process appending, sealing, evicting, or merging
//! s<n>          a segment: a header, then entries
//! x<n>          the segment being written's index: a record per entry, in the order appended
//! r<n>          a run: the sorted index of one or more sealed segments
//! ```
//!
//! An entry is a kind byte, a flags byte (its codec, whether a key and a trailer follow, and a
//! 2-bit use counter), its stored length, then whatever its codec needs, then the bytes. An entry
//! whose ID is the hash of its bytes carries no key at all: the reader checks the bytes against
//! the ID it asked for, so an index that points at the wrong bytes — a torn write, a crash before
//! the kernel wrote a page — only ever costs a miss. An entry whose key is not its hash (a derived
//! record) carries its key and a checksum.
//!
//! Readers take no lock: segments only grow, runs never change, and a reader that holds a segment
//! or run open keeps reading it after a writer deletes it. Writers append under `LOCK`. Nothing is
//! fsynced: anything a crash loses is fetched again.
//!
//! The cache is kept under its budget a segment at a time, oldest first, without ever reading the
//! whole cache: each entry of the segment retired is dropped unless it was read since it was
//! written (its use counter, S3-FIFO's rule) or something pins it, and those are copied forward —
//! so an entry read once by a clone's scan leaves with its segment, and one read every day stays.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io;
use std::io::Write as _;
use std::os::unix::fs::FileExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use jj_lib::lock::FileLock;
use memmap2::Mmap;

use crate::ids::Id;

/// How the bytes of an entry are stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Raw = 0,
    Zstd = 1,
    /// zstd with a repository's dictionary, named by `dict`.
    ZstdDict = 2,
}

impl Codec {
    fn from_bits(b: u8) -> Option<Self> {
        match b & 3 {
            0 => Some(Self::Raw),
            1 => Some(Self::Zstd),
            2 => Some(Self::ZstdDict),
            _ => None,
        }
    }
}

/// An entry to append.
pub struct NewEntry<'a> {
    pub kind: u8,
    pub codec: Codec,
    /// The bytes' length once decoded; for `Codec::Raw`, `data.len()`.
    pub raw_len: u64,
    /// For `Codec::ZstdDict`, the dictionary.
    pub dict: u32,
    /// For an entry whose key is not the hash of its bytes, the key, kept and checked.
    pub key: Option<Id>,
    /// The bytes, as `codec` stores them.
    pub data: &'a [u8],
    /// Bytes kept with the entry, not covered by its key's hash (a tree's sizes record).
    pub trailer: &'a [u8],
    /// The IDs it is found by: at least one.
    pub index: &'a [Id],
}

/// An entry read.
#[derive(Clone, Debug)]
pub struct Entry {
    pub kind: u8,
    pub codec: Codec,
    pub raw_len: u64,
    pub dict: u32,
    pub key: Option<Id>,
    pub data: Vec<u8>,
    pub trailer: Vec<u8>,
    pub loc: Loc,
    uses: u8,
}

impl Entry {
    /// An entry that is in no segment: bytes to decode as an entry's would be.
    pub fn detached(codec: Codec, dict: u32, raw_len: u64, data: Vec<u8>) -> Self {
        Self {
            kind: 0,
            codec,
            raw_len,
            dict,
            key: None,
            data,
            trailer: Vec::new(),
            loc: Loc { seg: 0, off: 0 },
            uses: 0,
        }
    }
}

/// Where an entry is: its segment and its offset in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Loc {
    pub seg: u32,
    pub off: u32,
}

/// Sizes and the budget.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// A segment is sealed once it holds this much.
    pub segment_bytes: u64,
    /// What the segments may hold in all; the oldest are retired past it.
    pub budget: u64,
    /// How often a reader whose lookups all hit checks for another process's changes:
    /// `REFRESH_EVERY`, but in tests.
    pub refresh_every: Duration,
}

/// What retirement needs from the layer above: what pins an entry, and what an entry is found
/// by (so a copy can be indexed again).
pub trait Retire {
    /// IDs whose entries must survive retirement.
    fn pinned(&self) -> HashSet<Id>;
    /// The IDs an entry is found by, or none if it is not one (its bytes do not verify).
    fn ids(&self, entry: &Entry) -> Vec<Id>;
}

/// A retirement policy that pins nothing and keeps no entry's identity: for tests.
#[cfg(test)]
pub struct KeepNothing;

#[cfg(test)]
impl Retire for KeepNothing {
    fn pinned(&self) -> HashSet<Id> {
        HashSet::new()
    }
    fn ids(&self, _entry: &Entry) -> Vec<Id> {
        Vec::new()
    }
}

const SEGMENT_MAGIC: &[u8; 8] = b"qseg\x00\x01\x00\x00";
const RUN_MAGIC: &[u8; 8] = b"qrun\x00\x01\x00\x00";
const META_MAGIC: &[u8; 8] = b"qmeta\x00\x01\x00";
/// A run: its magic, its entry count, its segment count, its segments, its fan-out, its entries.
const FANOUT: usize = 256;
/// A run entry: a 48-bit ID prefix, the segment's index in the run's list, the offset.
const RUN_ENTRY: usize = 12;
/// A side record: a 64-bit ID prefix and the offset.
const SIDE_RECORD: usize = 12;
/// The largest entry: a segment holds at least two.
const MAX_ENTRY_SHARE: u64 = 2;
/// The most runs kept before the two newest are merged regardless of size.
const MAX_RUNS: usize = 12;
/// How often a reader whose lookups all hit checks for another process's changes.
pub const REFRESH_EVERY: Duration = Duration::from_secs(1);

const FLAG_KEY: u8 = 1 << 2;
const FLAG_TRAILER: u8 = 1 << 3;
const USES_SHIFT: u8 = 6;
const USES_MAX: u8 = 3;

fn prefix64(id: &Id) -> u64 {
    u64::from_be_bytes(id[..8].try_into().unwrap())
}

fn prefix48(p64: u64) -> u64 {
    p64 >> 16
}

fn put_uvarint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn get_uvarint(b: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for i in 0..10 {
        let byte = *b.get(*at)?;
        *at += 1;
        v |= u64::from(byte & 0x7f) << (7 * i);
        if byte < 0x80 {
            return Some(v);
        }
    }
    None
}

fn checksum(key: &Id, data: &[u8], trailer: &[u8]) -> [u8; 8] {
    let mut h = blake3::Hasher::new();
    h.update(b"ok-segment-entry\n");
    h.update(key);
    h.update(&(data.len() as u64).to_le_bytes());
    h.update(data);
    h.update(trailer);
    h.finalize().as_bytes()[..8].try_into().unwrap()
}

/// The encoded entry, its uses counter at zero.
fn encode(e: &NewEntry, uses: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + 40 + e.data.len() + e.trailer.len());
    out.push(e.kind);
    let mut flags = e.codec as u8 | (uses.min(USES_MAX) << USES_SHIFT);
    if e.key.is_some() {
        flags |= FLAG_KEY;
    }
    if !e.trailer.is_empty() {
        flags |= FLAG_TRAILER;
    }
    out.push(flags);
    put_uvarint(&mut out, e.data.len() as u64);
    if e.codec != Codec::Raw {
        put_uvarint(&mut out, e.raw_len);
    }
    if e.codec == Codec::ZstdDict {
        out.extend_from_slice(&e.dict.to_le_bytes());
    }
    if !e.trailer.is_empty() {
        put_uvarint(&mut out, e.trailer.len() as u64);
    }
    if let Some(key) = &e.key {
        out.extend_from_slice(key);
        out.extend_from_slice(&checksum(key, e.data, e.trailer));
    }
    out.extend_from_slice(e.data);
    out.extend_from_slice(e.trailer);
    out
}

/// A decoded header: everything but the bytes, and where they start and end.
struct Header {
    kind: u8,
    codec: Codec,
    uses: u8,
    raw_len: u64,
    dict: u32,
    key: Option<(Id, [u8; 8])>,
    data_len: usize,
    trailer_len: usize,
    /// From the entry's start.
    body: usize,
}

impl Header {
    fn total(&self) -> usize {
        self.body + self.data_len + self.trailer_len
    }
}

/// The longest header: kind, flags, three varints, a dictionary, a key and its checksum.
const MAX_HEADER: usize = 2 + 3 * 10 + 4 + 32 + 8;

fn decode_header(b: &[u8]) -> Option<Header> {
    let kind = *b.first()?;
    let flags = *b.get(1)?;
    let codec = Codec::from_bits(flags)?;
    let mut at = 2;
    let data_len = usize::try_from(get_uvarint(b, &mut at)?).ok()?;
    let raw_len = if codec == Codec::Raw {
        data_len as u64
    } else {
        get_uvarint(b, &mut at)?
    };
    let dict = if codec == Codec::ZstdDict {
        let d = u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?);
        at += 4;
        d
    } else {
        0
    };
    let trailer_len = if flags & FLAG_TRAILER != 0 {
        usize::try_from(get_uvarint(b, &mut at)?).ok()?
    } else {
        0
    };
    let key = if flags & FLAG_KEY != 0 {
        let key: Id = b.get(at..at + 32)?.try_into().ok()?;
        let sum: [u8; 8] = b.get(at + 32..at + 40)?.try_into().ok()?;
        at += 40;
        Some((key, sum))
    } else {
        None
    };
    Some(Header {
        kind,
        codec,
        uses: flags >> USES_SHIFT,
        raw_len,
        dict,
        key,
        data_len,
        trailer_len,
        body: at,
    })
}

/// The live state, as `meta` records it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Meta {
    next_id: u32,
    /// Oldest first; at most the last is unsealed.
    segments: Vec<SegmentMeta>,
    /// Newest first.
    runs: Vec<RunMeta>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SegmentMeta {
    id: u32,
    sealed: bool,
    /// For a sealed segment, its length.
    len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RunMeta {
    id: u32,
    entries: u64,
}

impl Meta {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(META_MAGIC);
        out.extend_from_slice(&self.next_id.to_le_bytes());
        out.extend_from_slice(&(self.segments.len() as u32).to_le_bytes());
        for s in &self.segments {
            out.extend_from_slice(&s.id.to_le_bytes());
            out.push(u8::from(s.sealed));
            out.extend_from_slice(&s.len.to_le_bytes());
        }
        out.extend_from_slice(&(self.runs.len() as u32).to_le_bytes());
        for r in &self.runs {
            out.extend_from_slice(&r.id.to_le_bytes());
            out.extend_from_slice(&r.entries.to_le_bytes());
        }
        let sum = blake3::hash(&out);
        out.extend_from_slice(&sum.as_bytes()[..8]);
        out
    }

    fn decode(b: &[u8]) -> Option<Self> {
        let (body, sum) = b.split_at(b.len().checked_sub(8)?);
        if blake3::hash(body).as_bytes()[..8] != *sum || !body.starts_with(META_MAGIC) {
            return None;
        }
        let mut at = META_MAGIC.len();
        let u32_at = |at: &mut usize| -> Option<u32> {
            let v = u32::from_le_bytes(body.get(*at..*at + 4)?.try_into().ok()?);
            *at += 4;
            Some(v)
        };
        let next_id = u32_at(&mut at)?;
        let n = u32_at(&mut at)? as usize;
        let mut segments = Vec::with_capacity(n.min(1 << 16));
        for _ in 0..n {
            let id = u32_at(&mut at)?;
            let sealed = *body.get(at)? != 0;
            at += 1;
            let len = u64::from_le_bytes(body.get(at..at + 8)?.try_into().ok()?);
            at += 8;
            segments.push(SegmentMeta { id, sealed, len });
        }
        let n = u32_at(&mut at)? as usize;
        let mut runs = Vec::with_capacity(n.min(1 << 16));
        for _ in 0..n {
            let id = u32_at(&mut at)?;
            let entries = u64::from_le_bytes(body.get(at..at + 8)?.try_into().ok()?);
            at += 8;
            runs.push(RunMeta { id, entries });
        }
        (at == body.len()).then_some(Self {
            next_id,
            segments,
            runs,
        })
    }

    fn live(&self, seg: u32) -> bool {
        self.segments.iter().any(|s| s.id == seg)
    }

    fn active(&self) -> Option<u32> {
        self.segments.last().filter(|s| !s.sealed).map(|s| s.id)
    }
}

/// A run, mapped: its segments, its fan-out, its entries.
struct Run {
    map: Mmap,
    segments: Vec<u32>,
    entries_at: usize,
    count: usize,
    fanout_at: usize,
}

impl Run {
    fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: runs are written aside and renamed into place, never changed afterwards.
        let map = unsafe { Mmap::map(&file)? };
        let bad = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("run {}", path.display()),
            )
        };
        if map.len() < 24 || &map[..8] != RUN_MAGIC {
            return Err(bad());
        }
        let count = u64::from_le_bytes(map[8..16].try_into().unwrap()) as usize;
        let nseg = u32::from_le_bytes(map[16..20].try_into().unwrap()) as usize;
        let segs_at = 20;
        let fanout_at = segs_at + nseg * 4;
        let entries_at = fanout_at + FANOUT * 4;
        if map.len() != entries_at + count * RUN_ENTRY {
            return Err(bad());
        }
        let segments = (0..nseg)
            .map(|i| {
                u32::from_le_bytes(
                    map[segs_at + i * 4..segs_at + i * 4 + 4]
                        .try_into()
                        .unwrap(),
                )
            })
            .collect();
        Ok(Self {
            map,
            segments,
            entries_at,
            count,
            fanout_at,
        })
    }

    fn fanout(&self, b: usize) -> usize {
        let at = self.fanout_at + b * 4;
        u32::from_le_bytes(self.map[at..at + 4].try_into().unwrap()) as usize
    }

    fn entry(&self, i: usize) -> (u64, u16, u32) {
        let at = self.entries_at + i * RUN_ENTRY;
        let e = &self.map[at..at + RUN_ENTRY];
        let mut p = [0u8; 8];
        p[2..].copy_from_slice(&e[..6]);
        (
            u64::from_be_bytes(p),
            u16::from_le_bytes(e[6..8].try_into().unwrap()),
            u32::from_le_bytes(e[8..12].try_into().unwrap()),
        )
    }

    /// The locations of entries whose ID begins with `p48`.
    fn find(&self, p48: u64, out: &mut Vec<Loc>) {
        let top = (p48 >> 40) as usize;
        let mut lo = if top == 0 { 0 } else { self.fanout(top - 1) };
        let mut hi = self.fanout(top);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.entry(mid).0 < p48 {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        while lo < self.count {
            let (p, seg, off) = self.entry(lo);
            if p != p48 {
                break;
            }
            if let Some(seg) = self.segments.get(seg as usize) {
                out.push(Loc { seg: *seg, off });
            }
            lo += 1;
        }
    }

    /// Every entry, sorted.
    fn all(&self) -> impl Iterator<Item = (u64, Loc)> + '_ {
        (0..self.count).filter_map(|i| {
            let (p, seg, off) = self.entry(i);
            self.segments
                .get(seg as usize)
                .map(|s| (p, Loc { seg: *s, off }))
        })
    }
}

/// Writes a run of `entries` — (48-bit prefix, location), any order — at `path`.
fn write_run(path: &Path, entries: &mut [(u64, Loc)]) -> io::Result<u64> {
    entries.sort_unstable();
    let mut segments: Vec<u32> = entries.iter().map(|e| e.1.seg).collect();
    segments.sort_unstable();
    segments.dedup();
    if segments.len() > usize::from(u16::MAX) {
        return Err(io::Error::other("a run of more than 65,535 segments"));
    }
    let index: HashMap<u32, u16> = segments
        .iter()
        .enumerate()
        .map(|(i, s)| (*s, i as u16))
        .collect();
    let mut out =
        Vec::with_capacity(24 + segments.len() * 4 + FANOUT * 4 + entries.len() * RUN_ENTRY);
    out.extend_from_slice(RUN_MAGIC);
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    out.extend_from_slice(&(segments.len() as u32).to_le_bytes());
    for s in &segments {
        out.extend_from_slice(&s.to_le_bytes());
    }
    let mut fanout = [0u32; FANOUT];
    for (p, _) in entries.iter() {
        fanout[(p >> 40) as usize] += 1;
    }
    let mut sum = 0u32;
    for f in fanout.iter_mut() {
        sum += *f;
        *f = sum;
    }
    for f in fanout {
        out.extend_from_slice(&f.to_le_bytes());
    }
    for (p, loc) in entries.iter() {
        out.extend_from_slice(&p.to_be_bytes()[2..]);
        out.extend_from_slice(&index[&loc.seg].to_le_bytes());
        out.extend_from_slice(&loc.off.to_le_bytes());
    }
    write_aside(path, &out)?;
    Ok(entries.len() as u64)
}

/// Writes `data` at `path` through a file beside it, renamed into place.
fn write_aside(path: &Path, data: &[u8]) -> io::Result<()> {
    let mut tmp = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
    tmp.write_all(data)?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

/// The identity of `meta` when read: a change in any part is another process's write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Stamp {
    ino: u64,
    len: u64,
    mtime_ns: i128,
}

fn stamp(path: &Path) -> Option<Stamp> {
    use std::os::unix::fs::MetadataExt as _;
    let m = fs::metadata(path).ok()?;
    Some(Stamp {
        ino: m.ino(),
        len: m.len(),
        mtime_ns: i128::from(m.mtime()) * 1_000_000_000 + i128::from(m.mtime_nsec()),
    })
}

/// What a reader sees: the live segments and runs, as of one `meta`.
struct View {
    meta: Meta,
    stamp: Option<Stamp>,
    runs: Vec<Run>,
    files: Mutex<HashMap<u32, Arc<Segment>>>,
    active: Mutex<Side>,
    checked: Mutex<Instant>,
}

/// The index of the segment being written, as far as this process has read it.
#[derive(Default)]
struct Side {
    seg: Option<u32>,
    read: u64,
    map: HashMap<u64, Vec<u32>>,
    /// The side file, open while it is the active segment's: a lookup reads what was appended
    /// since, without opening it again.
    file: Option<File>,
}

/// An open segment: mapped once sealed; read with `pread` while it grows.
struct Segment {
    file: File,
    map: Option<Mmap>,
}

impl Segment {
    fn read(&self, off: u64, len: usize) -> io::Result<Vec<u8>> {
        if let Some(map) = &self.map {
            let start = usize::try_from(off).map_err(io::Error::other)?;
            let end = start.saturating_add(len).min(map.len());
            return Ok(map.get(start..end).unwrap_or_default().to_vec());
        }
        let mut buf = vec![0; len];
        let mut got = 0;
        while got < len {
            match self.file.read_at(&mut buf[got..], off + got as u64)? {
                0 => break,
                n => got += n,
            }
        }
        buf.truncate(got);
        Ok(buf)
    }
}

/// A directory of segments.
pub struct Segments {
    dir: PathBuf,
    opts: Options,
    view: Mutex<Arc<View>>,
    /// Raised each time the view is read again: entries may have moved.
    epoch: std::sync::atomic::AtomicU64,
    /// Set when this process appends: its own entries are seen by peek, which reads the view
    /// again on a miss only then.
    wrote: std::sync::atomic::AtomicBool,
}

/// What a segment's entries are found by, kept while the segment is written: its side records.
fn side_path(dir: &Path, seg: u32) -> PathBuf {
    dir.join(format!("x{seg}"))
}

fn segment_path(dir: &Path, seg: u32) -> PathBuf {
    dir.join(format!("s{seg}"))
}

fn run_path(dir: &Path, run: u32) -> PathBuf {
    dir.join(format!("r{run}"))
}

impl Segments {
    /// Opens the segments at `dir`, creating it. A `meta` that does not decode — a crash before
    /// the kernel wrote it — empties the directory: the cache starts over.
    pub fn open(dir: PathBuf, opts: Options) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;
        let this = Self {
            view: Mutex::new(Arc::new(View::empty())),
            epoch: std::sync::atomic::AtomicU64::new(0),
            wrote: std::sync::atomic::AtomicBool::new(false),
            dir,
            opts,
        };
        {
            let _lock = this.lock()?;
            if this.read_meta().is_none() {
                this.reset()?;
            }
        }
        this.refresh(true)?;
        Ok(this)
    }

    fn lock(&self) -> io::Result<FileLock> {
        FileLock::lock(self.dir.join("LOCK")).map_err(io::Error::other)
    }

    fn read_meta(&self) -> Option<Meta> {
        Meta::decode(&fs::read(self.dir.join("meta")).ok()?)
    }

    fn write_meta(&self, meta: &Meta) -> io::Result<()> {
        write_aside(&self.dir.join("meta"), &meta.encode())
    }

    /// Removes every segment, side, and run, and writes an empty `meta`. Under the lock.
    fn reset(&self) -> io::Result<()> {
        for e in fs::read_dir(&self.dir)? {
            let e = e?;
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name != "LOCK" {
                let _ = fs::remove_file(e.path());
            }
        }
        self.write_meta(&Meta {
            next_id: 1,
            ..Meta::default()
        })
    }

    /// Reads `meta` again if it changed (or `force`), mapping the runs it names.
    fn refresh(&self, force: bool) -> io::Result<()> {
        let now = stamp(&self.dir.join("meta"));
        {
            let view = self.view.lock().unwrap();
            if !force && view.stamp == now {
                return Ok(());
            }
        }
        let Some(meta) = self.read_meta() else {
            return Ok(());
        };
        let mut runs = Vec::with_capacity(meta.runs.len());
        for r in &meta.runs {
            if let Ok(run) = Run::open(&run_path(&self.dir, r.id)) {
                runs.push(run);
            }
        }
        let view = View {
            meta,
            stamp: now,
            runs,
            files: Mutex::new(HashMap::new()),
            active: Mutex::new(Side::default()),
            checked: Mutex::new(Instant::now()),
        };
        *self.view.lock().unwrap() = Arc::new(view);
        self.epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Raised each time the segments this process sees change: a caller holding an entry's
    /// bytes elsewhere marks it used again after one, since retirement may have moved it.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Marks the newest entry found by `id` whose kind is one of `kinds` used, reading only its
    /// header: for a read answered from a copy held elsewhere, so retirement still sees it.
    pub fn touch(&self, id: &Id, kinds: &[u8]) {
        let view = self.current();
        for loc in self.candidates(&view, id) {
            let Some(seg) = self.segment(&view, loc.seg) else {
                continue;
            };
            let Ok(head) = seg.read(u64::from(loc.off), 2) else {
                continue;
            };
            if head.len() == 2 && kinds.contains(&head[0]) {
                let uses = head[1] >> USES_SHIFT;
                if uses < USES_MAX {
                    let f = (head[1] & !(USES_MAX << USES_SHIFT)) | ((uses + 1) << USES_SHIFT);
                    let _ = seg.file.write_at(&[f], u64::from(loc.off) + 1);
                }
                return;
            }
        }
    }

    /// Every live location an ID's 64-bit prefix is found at, newest first.
    fn candidates(&self, view: &View, id: &Id) -> Vec<Loc> {
        let p64 = prefix64(id);
        let mut locs = Vec::new();
        if let Some(active) = view.meta.active() {
            let mut side = view.active.lock().unwrap();
            self.read_side(&mut side, active);
            if let Some(offs) = side.map.get(&p64) {
                locs.extend(offs.iter().map(|off| Loc {
                    seg: active,
                    off: *off,
                }));
            }
        }
        for run in &view.runs {
            run.find(prefix48(p64), &mut locs);
        }
        locs.retain(|l| view.meta.live(l.seg));
        locs.sort_unstable_by(|a, b| b.cmp(a));
        locs.dedup();
        locs
    }

    fn current(&self) -> Arc<View> {
        self.view.lock().unwrap().clone()
    }

    /// The bytes the live segments hold.
    pub fn bytes(&self) -> u64 {
        let view = self.current();
        view.meta
            .segments
            .iter()
            .map(|s| {
                if s.sealed {
                    s.len
                } else {
                    fs::metadata(segment_path(&self.dir, s.id)).map_or(0, |m| m.len())
                }
            })
            .sum()
    }

    /// The newest entry found by `id` that `accept` takes, its use counter raised. A lookup that
    /// finds nothing reads `meta` and the segment being written again before it answers, so it
    /// sees what other processes wrote.
    pub fn get(&self, id: &Id, accept: &mut dyn FnMut(&Entry) -> bool) -> Option<Entry> {
        let view = self.current();
        let stale = {
            let mut checked = view.checked.lock().unwrap();
            let stale = checked.elapsed() > self.opts.refresh_every;
            if stale {
                *checked = Instant::now();
            }
            stale
        };
        if stale {
            let _ = self.refresh(false);
        }
        let view = self.current();
        if let Some(e) = self.find(&view, id, accept) {
            return Some(e);
        }
        let _ = self.refresh(false);
        let view = self.current();
        self.find(&view, id, accept)
    }

    /// Finds an entry as get does, but on a miss does not look again at what other processes
    /// wrote since: for a check whose miss costs only doing something again (staging what is
    /// cached), where looking again would cost every miss a refresh.
    pub fn peek(&self, id: &Id, accept: &mut dyn FnMut(&Entry) -> bool) -> Option<Entry> {
        let view = self.current();
        let stale = {
            let mut checked = view.checked.lock().unwrap();
            let stale = checked.elapsed() > self.opts.refresh_every;
            if stale {
                *checked = Instant::now();
            }
            stale
        };
        if stale {
            let _ = self.refresh(false);
        }
        if let Some(e) = self.find(&self.current(), id, accept) {
            return Some(e);
        }
        if self.wrote.swap(false, std::sync::atomic::Ordering::AcqRel) {
            let _ = self.refresh(false);
            return self.find(&self.current(), id, accept);
        }
        None
    }

    fn find(&self, view: &View, id: &Id, accept: &mut dyn FnMut(&Entry) -> bool) -> Option<Entry> {
        for loc in self.candidates(view, id) {
            if let Some(e) = self.read_entry(view, loc)
                && accept(&e)
            {
                self.used(view, &e);
                return Some(e);
            }
        }
        None
    }

    /// Reads the side records appended since last read.
    fn read_side(&self, side: &mut Side, active: u32) {
        if side.seg != Some(active) {
            *side = Side {
                seg: Some(active),
                ..Side::default()
            };
        }
        if side.file.is_none() {
            let Ok(f) = File::open(side_path(&self.dir, active)) else {
                return;
            };
            side.file = Some(f);
        }
        let file = side.file.as_ref().unwrap();
        let Ok(len) = file.metadata().map(|m| m.len()) else {
            return;
        };
        let len = len - len % SIDE_RECORD as u64;
        if len <= side.read {
            return;
        }
        let mut buf = vec![0; (len - side.read) as usize];
        if file.read_exact_at(&mut buf, side.read).is_err() {
            return;
        }
        for r in buf.as_chunks::<SIDE_RECORD>().0 {
            let p = u64::from_le_bytes(r[..8].try_into().unwrap());
            let off = u32::from_le_bytes(r[8..].try_into().unwrap());
            side.map.entry(p).or_default().push(off);
        }
        side.read = len;
    }

    fn segment(&self, view: &View, seg: u32) -> Option<Arc<Segment>> {
        let mut files = view.files.lock().unwrap();
        if let Some(s) = files.get(&seg) {
            return Some(s.clone());
        }
        let meta = view.meta.segments.iter().find(|s| s.id == seg)?;
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_path(&self.dir, seg))
            .ok()?;
        let map = if meta.sealed {
            // SAFETY: a sealed segment's bytes never change but for its entries' use counters,
            // which readers tolerate in any state.
            unsafe { Mmap::map(&file).ok() }
        } else {
            None
        };
        let s = Arc::new(Segment { file, map });
        files.insert(seg, s.clone());
        Some(s)
    }

    fn read_entry(&self, view: &View, loc: Loc) -> Option<Entry> {
        let seg = self.segment(view, loc.seg)?;
        let head = seg.read(u64::from(loc.off), MAX_HEADER).ok()?;
        let h = decode_header(&head)?;
        let body = if h.body + h.data_len + h.trailer_len <= head.len() {
            head[h.body..h.total()].to_vec()
        } else {
            let b = seg
                .read(
                    u64::from(loc.off) + h.body as u64,
                    h.data_len + h.trailer_len,
                )
                .ok()?;
            if b.len() != h.data_len + h.trailer_len {
                return None;
            }
            b
        };
        let (data, trailer) = body.split_at(h.data_len);
        if let Some((key, sum)) = h.key
            && checksum(&key, data, trailer) != sum
        {
            return None;
        }
        Some(Entry {
            kind: h.kind,
            codec: h.codec,
            raw_len: h.raw_len,
            dict: h.dict,
            key: h.key.map(|k| k.0),
            data: data.to_vec(),
            trailer: trailer.to_vec(),
            loc,
            uses: h.uses,
        })
    }

    /// Raises an entry's use counter, in place.
    fn used(&self, view: &View, e: &Entry) {
        if e.uses >= USES_MAX {
            return;
        }
        let Some(seg) = self.segment(view, e.loc.seg) else {
            return;
        };
        let mut flags = [0u8];
        if seg
            .file
            .read_exact_at(&mut flags, u64::from(e.loc.off) + 1)
            .is_ok()
        {
            let uses = (flags[0] >> USES_SHIFT).saturating_add(1).min(USES_MAX);
            let f = (flags[0] & !(USES_MAX << USES_SHIFT)) | (uses << USES_SHIFT);
            let _ = seg.file.write_at(&[f], u64::from(e.loc.off) + 1);
        }
    }

    /// Appends entries: one write to the segment being written, one to its index. Seals it once it
    /// is full, retiring the oldest segments past the budget (`retire` says what survives).
    pub fn append(&self, entries: &[NewEntry], retire: &dyn Retire) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.wrote.store(true, std::sync::atomic::Ordering::Release);
        let max_entry = self.opts.segment_bytes / MAX_ENTRY_SHARE;
        let _lock = self.lock()?;
        let mut meta = self
            .read_meta()
            .ok_or_else(|| io::Error::other("the segments' meta is unreadable"))?;
        let mut pending: Vec<(Vec<u8>, Vec<u64>)> = Vec::with_capacity(entries.len());
        for e in entries {
            let bytes = encode(e, 0);
            if bytes.len() as u64 > max_entry {
                continue;
            }
            pending.push((bytes, e.index.iter().map(prefix64).collect()));
        }
        self.write_pending(&mut meta, pending, Some(retire))?;
        Ok(())
    }

    /// Writes encoded entries under the lock, sealing as segments fill — and, given a policy,
    /// retiring past the budget.
    fn write_pending(
        &self,
        meta: &mut Meta,
        pending: Vec<(Vec<u8>, Vec<u64>)>,
        retire: Option<&dyn Retire>,
    ) -> io::Result<()> {
        let mut i = 0;
        while i < pending.len() {
            let active = self.ensure_active(meta)?;
            let seg = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(segment_path(&self.dir, active))?;
            let mut len = seg.metadata()?.len();
            let mut data = Vec::new();
            let mut side = Vec::new();
            while i < pending.len() {
                let (bytes, prefixes) = &pending[i];
                if len + data.len() as u64 + bytes.len() as u64 > self.opts.segment_bytes
                    && len + data.len() as u64 > SEGMENT_MAGIC.len() as u64
                {
                    break;
                }
                let off = u32::try_from(len + data.len() as u64).map_err(io::Error::other)?;
                for p in prefixes {
                    side.extend_from_slice(&p.to_le_bytes());
                    side.extend_from_slice(&off.to_le_bytes());
                }
                data.extend_from_slice(bytes);
                i += 1;
            }
            seg.write_all_at(&data, len)?;
            len += data.len() as u64;
            let mut sf = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(side_path(&self.dir, active))?;
            sf.write_all(&side)?;
            if len >= self.opts.segment_bytes || i < pending.len() {
                self.seal(meta, active, len, retire)?;
            }
        }
        Ok(())
    }

    /// The segment being written, starting one if there is none. Under the lock.
    fn ensure_active(&self, meta: &mut Meta) -> io::Result<u32> {
        if let Some(a) = meta.active() {
            return Ok(a);
        }
        let id = meta.next_id;
        meta.next_id += 1;
        let path = segment_path(&self.dir, id);
        let mut f = File::create(&path)?;
        f.write_all(SEGMENT_MAGIC)?;
        let _ = fs::remove_file(side_path(&self.dir, id));
        meta.segments.push(SegmentMeta {
            id,
            sealed: false,
            len: 0,
        });
        self.write_meta(meta)?;
        Ok(id)
    }

    /// Seals the segment being written: its index becomes a run, runs merge, and — given a
    /// policy — the oldest segments retire past the budget. Under the lock.
    fn seal(
        &self,
        meta: &mut Meta,
        seg: u32,
        len: u64,
        retire: Option<&dyn Retire>,
    ) -> io::Result<()> {
        let side = fs::read(side_path(&self.dir, seg)).unwrap_or_default();
        let mut entries: Vec<(u64, Loc)> = side
            .as_chunks::<SIDE_RECORD>()
            .0
            .iter()
            .filter_map(|r| {
                let p = u64::from_le_bytes(r[..8].try_into().unwrap());
                let off = u32::from_le_bytes(r[8..].try_into().unwrap());
                (u64::from(off) < len).then_some((prefix48(p), Loc { seg, off }))
            })
            .collect();
        let run = meta.next_id;
        meta.next_id += 1;
        let count = write_run(&run_path(&self.dir, run), &mut entries)?;
        if let Some(s) = meta.segments.iter_mut().find(|s| s.id == seg) {
            s.sealed = true;
            s.len = len;
        }
        meta.runs.insert(
            0,
            RunMeta {
                id: run,
                entries: count,
            },
        );
        self.merge(meta)?;
        self.write_meta(meta)?;
        let _ = fs::remove_file(side_path(&self.dir, seg));
        if let Some(retire) = retire {
            self.retire(meta, retire)?;
        }
        Ok(())
    }

    /// Merges the newest runs while the older is no more than twice the newer, or there are too
    /// many, dropping entries of retired segments. Under the lock; the caller writes `meta`.
    fn merge(&self, meta: &mut Meta) -> io::Result<()> {
        while meta.runs.len() >= 2
            && (meta.runs[1].entries <= meta.runs[0].entries * 2 || meta.runs.len() > MAX_RUNS)
        {
            let (newer, older) = (meta.runs[0].clone(), meta.runs[1].clone());
            let a = Run::open(&run_path(&self.dir, newer.id))?;
            let b = Run::open(&run_path(&self.dir, older.id))?;
            let mut entries: Vec<(u64, Loc)> = a
                .all()
                .chain(b.all())
                .filter(|(_, l)| meta.live(l.seg))
                .collect();
            let id = meta.next_id;
            meta.next_id += 1;
            let count = write_run(&run_path(&self.dir, id), &mut entries)?;
            meta.runs.splice(0..2, [RunMeta { id, entries: count }]);
            drop((a, b));
            let _ = fs::remove_file(run_path(&self.dir, newer.id));
            let _ = fs::remove_file(run_path(&self.dir, older.id));
        }
        Ok(())
    }

    /// Retires the oldest sealed segments while the live ones hold more than the budget: each
    /// entry read since it was written, or pinned, is copied forward (a read one with its counter
    /// lowered); the rest leave with the segment. A pass retires each segment it began with at
    /// most once. Under the lock.
    fn retire(&self, meta: &mut Meta, retire: &dyn Retire) -> io::Result<()> {
        let mut total: u64 = self.live_bytes(meta);
        if total <= self.opts.budget {
            return Ok(());
        }
        let candidates: Vec<u32> = meta
            .segments
            .iter()
            .filter(|s| s.sealed)
            .map(|s| s.id)
            .collect();
        let pinned = retire.pinned();
        for seg in candidates {
            if total <= self.opts.budget {
                break;
            }
            // Where the pinned IDs' newest entries are, as this process sees them now.
            let _ = self.refresh(true);
            let view = self.current();
            let mut pinned_locs = HashSet::new();
            for id in &pinned {
                if let Some(loc) = self.newest(&view, id)
                    && loc.seg == seg
                {
                    pinned_locs.insert(loc);
                }
            }
            let mut copies: Vec<(Vec<u8>, Vec<u64>)> = Vec::new();
            for e in self.scan(&view, seg) {
                let keep = e.uses > 0 || pinned_locs.contains(&e.loc);
                if !keep {
                    continue;
                }
                let ids = match e.key {
                    Some(k) => vec![k],
                    None => retire.ids(&e),
                };
                // Only an entry that is still what its IDs find: a later one supersedes it.
                let ids: Vec<Id> = ids
                    .into_iter()
                    .filter(|id| self.newest(&view, id) == Some(e.loc))
                    .collect();
                if ids.is_empty() {
                    continue;
                }
                let uses = if pinned_locs.contains(&e.loc) {
                    e.uses
                } else {
                    e.uses - 1
                };
                let new = NewEntry {
                    kind: e.kind,
                    codec: e.codec,
                    raw_len: e.raw_len,
                    dict: e.dict,
                    key: e.key,
                    data: &e.data,
                    trailer: &e.trailer,
                    index: &ids,
                };
                copies.push((encode(&new, uses), ids.iter().map(prefix64).collect()));
            }
            let len = meta
                .segments
                .iter()
                .find(|s| s.id == seg)
                .map_or(0, |s| s.len);
            meta.segments.retain(|s| s.id != seg);
            // Runs whose every segment is gone go with them.
            let mut dead_runs = Vec::new();
            meta.runs.retain(|r| {
                let alive = Run::open(&run_path(&self.dir, r.id))
                    .map(|run| {
                        run.segments
                            .iter()
                            .any(|s| meta.segments.iter().any(|m| m.id == *s))
                    })
                    .unwrap_or(false);
                if !alive {
                    dead_runs.push(r.id);
                }
                alive
            });
            self.write_meta(meta)?;
            let _ = fs::remove_file(segment_path(&self.dir, seg));
            for r in dead_runs {
                let _ = fs::remove_file(run_path(&self.dir, r));
            }
            total = total.saturating_sub(len);
            if !copies.is_empty() {
                // Copies go into the segment being written; one that fills it seals it, without
                // retiring again in this pass.
                let copied: u64 = copies.iter().map(|c| c.0.len() as u64).sum();
                self.write_pending(meta, copies, None)?;
                total += copied;
            }
        }
        Ok(())
    }

    fn live_bytes(&self, meta: &Meta) -> u64 {
        meta.segments
            .iter()
            .map(|s| {
                if s.sealed {
                    s.len
                } else {
                    fs::metadata(segment_path(&self.dir, s.id)).map_or(0, |m| m.len())
                }
            })
            .sum()
    }

    /// The newest location an ID's prefix is found at whose entry the ID owns — for a keyed entry,
    /// its key; otherwise the first candidate, newest first, since only the layer above can check
    /// bytes against IDs, and two IDs sharing 64 bits is a collision no store will see.
    fn newest(&self, view: &View, id: &Id) -> Option<Loc> {
        self.candidates(view, id).into_iter().find(|l| {
            self.read_entry(view, *l)
                .is_some_and(|e| e.key.is_none_or(|k| &k == id))
        })
    }

    /// How many live entries of each kind the segments hold, and their bytes: every segment read
    /// through, so for diagnostics and tests, never a command's path.
    pub fn census(&self) -> HashMap<u8, (usize, u64)> {
        let _ = self.refresh(true);
        let view = self.current();
        let mut out: HashMap<u8, (usize, u64)> = HashMap::new();
        for s in &view.meta.segments {
            let len = if s.sealed {
                s.len
            } else {
                fs::metadata(segment_path(&self.dir, s.id)).map_or(0, |m| m.len())
            };
            for e in self.scan_to(&view, s.id, len) {
                let c = out.entry(e.kind).or_default();
                c.0 += 1;
                c.1 += e.data.len() as u64 + e.trailer.len() as u64;
            }
        }
        out
    }

    /// Every entry of a sealed segment, in order; an entry that does not decode ends it.
    fn scan(&self, view: &View, seg: u32) -> Vec<Entry> {
        let Some(meta) = view.meta.segments.iter().find(|s| s.id == seg) else {
            return Vec::new();
        };
        self.scan_to(view, seg, meta.len)
    }

    /// Every entry of a segment up to `len`, in order; an entry that does not decode ends it.
    fn scan_to(&self, view: &View, seg: u32, len: u64) -> Vec<Entry> {
        let mut out = Vec::new();
        let mut off = SEGMENT_MAGIC.len() as u64;
        while off < len {
            let Ok(off32) = u32::try_from(off) else {
                break;
            };
            let Some(e) = self.read_entry(view, Loc { seg, off: off32 }) else {
                break;
            };
            let Some(seg_file) = self.segment(view, seg) else {
                break;
            };
            let Ok(head) = seg_file.read(off, MAX_HEADER) else {
                break;
            };
            let Some(h) = decode_header(&head) else {
                break;
            };
            off += h.total() as u64;
            out.push(e);
        }
        out
    }
}

impl View {
    fn empty() -> Self {
        Self {
            meta: Meta::default(),
            stamp: None,
            runs: Vec::new(),
            files: Mutex::new(HashMap::new()),
            active: Mutex::new(Side::default()),
            checked: Mutex::new(Instant::now()),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn id(n: u64) -> Id {
        *blake3::hash(&n.to_le_bytes()).as_bytes()
    }

    /// An ID whose first 8 bytes are another's: the collision the index must survive.
    fn twin(of: &Id, n: u8) -> Id {
        let mut t = *of;
        t[31] ^= n;
        t
    }

    fn opts(segment: u64, budget: u64) -> Options {
        Options {
            segment_bytes: segment,
            budget,
            refresh_every: REFRESH_EVERY,
        }
    }

    fn raw<'a>(kind: u8, data: &'a [u8], index: &'a [Id]) -> NewEntry<'a> {
        NewEntry {
            kind,
            codec: Codec::Raw,
            raw_len: data.len() as u64,
            dict: 0,
            key: None,
            data,
            trailer: &[],
            index,
        }
    }

    /// The entry found by `id` whose bytes are `want`'s: what the layer above's check does.
    fn get_data(s: &Segments, id: &Id) -> Option<Vec<u8>> {
        s.get(id, &mut |e| e.data.starts_with(&id[..4]))
            .map(|e| e.data)
    }

    fn body(id: &Id, n: usize) -> Vec<u8> {
        let mut b = id[..4].to_vec();
        b.extend(std::iter::repeat_n(id[5], n));
        b
    }

    #[test]
    fn entries_round_trip_and_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let s = Segments::open(dir.path().join("seg"), opts(1 << 20, 1 << 30)).unwrap();
        let (a, b) = (id(1), id(2));
        let (da, db) = (body(&a, 100), body(&b, 5000));
        s.append(&[raw(3, &da, &[a]), raw(1, &db, &[b, id(3)])], &KeepNothing)
            .unwrap();
        assert_eq!(get_data(&s, &a), Some(da.clone()));
        assert_eq!(get_data(&s, &b), Some(db.clone()));
        // Found by its second ID too.
        let e = s.get(&id(3), &mut |_| true).unwrap();
        assert_eq!(e.data, db);
        assert_eq!(get_data(&s, &id(4)), None);
        // A keyed entry carries its key and a trailer, compressed metadata survives.
        let k = id(9);
        s.append(
            &[NewEntry {
                kind: 0x81,
                codec: Codec::ZstdDict,
                raw_len: 12345,
                dict: 32769,
                key: Some(k),
                data: b"stored",
                trailer: b"trailer",
                index: &[k],
            }],
            &KeepNothing,
        )
        .unwrap();
        drop(s);
        let s = Segments::open(dir.path().join("seg"), opts(1 << 20, 1 << 30)).unwrap();
        assert_eq!(get_data(&s, &a), Some(da));
        let e = s.get(&k, &mut |_| true).unwrap();
        assert_eq!(
            (
                e.kind,
                e.codec,
                e.raw_len,
                e.dict,
                e.key,
                e.data.as_slice(),
                e.trailer.as_slice()
            ),
            (
                0x81,
                Codec::ZstdDict,
                12345,
                32769,
                Some(k),
                &b"stored"[..],
                &b"trailer"[..]
            )
        );
    }

    #[test]
    fn sealed_segments_are_found_through_runs_and_runs_stay_few() {
        let dir = tempfile::tempdir().unwrap();
        let s = Segments::open(dir.path().join("seg"), opts(64 << 10, 1 << 40)).unwrap();
        let ids: Vec<Id> = (0..4000).map(id).collect();
        let bodies: Vec<Vec<u8>> = ids.iter().map(|i| body(i, 200)).collect();
        for chunk in ids.chunks(50).zip(bodies.chunks(50)) {
            let entries: Vec<NewEntry> = chunk
                .0
                .iter()
                .zip(chunk.1)
                .map(|(i, b)| raw(3, b, std::slice::from_ref(i)))
                .collect();
            s.append(&entries, &KeepNothing).unwrap();
        }
        let meta = s.read_meta().unwrap();
        assert!(meta.segments.len() > 10, "{} segments", meta.segments.len());
        assert!(
            meta.runs.len() <= 8,
            "{} runs for {} segments",
            meta.runs.len(),
            meta.segments.len()
        );
        for (i, b) in ids.iter().zip(&bodies) {
            assert_eq!(get_data(&s, i).as_ref(), Some(b));
        }
        // Another process's view finds them too.
        let other = Segments::open(dir.path().join("seg"), opts(64 << 10, 1 << 40)).unwrap();
        assert_eq!(get_data(&other, &ids[17]).as_ref(), Some(&bodies[17]));
    }

    #[test]
    fn entries_sharing_a_prefix_are_all_candidates_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let s = Segments::open(dir.path().join("seg"), opts(32 << 10, 1 << 40)).unwrap();
        let a = id(5);
        let a2 = twin(&a, 1);
        let mut ba = body(&a, 10);
        ba.extend_from_slice(b"A");
        let mut ba2 = body(&a2, 10);
        ba2.extend_from_slice(b"B");
        s.append(&[raw(3, &ba, &[a])], &KeepNothing).unwrap();
        // Fill so the first lands in a run.
        let filler: Vec<Vec<u8>> = (100..400).map(|n| body(&id(n), 300)).collect();
        let fids: Vec<Id> = (100..400).map(id).collect();
        let entries: Vec<NewEntry> = fids
            .iter()
            .zip(&filler)
            .map(|(i, b)| raw(3, b, std::slice::from_ref(i)))
            .collect();
        s.append(&entries, &KeepNothing).unwrap();
        s.append(&[raw(3, &ba2, &[a2])], &KeepNothing).unwrap();
        let got_a = s.get(&a, &mut |e| e.data.ends_with(b"A")).unwrap();
        assert_eq!(got_a.data, ba);
        let got_a2 = s.get(&a2, &mut |e| e.data.ends_with(b"B")).unwrap();
        assert_eq!(got_a2.data, ba2);
        // Newest first: without a check, the twin written later is found first.
        assert_eq!(s.get(&a, &mut |_| true).unwrap().data, ba2);
    }

    #[test]
    fn retirement_keeps_what_was_read_and_what_is_pinned() {
        struct Policy {
            pinned: HashSet<Id>,
        }
        impl Retire for Policy {
            fn pinned(&self) -> HashSet<Id> {
                self.pinned.clone()
            }
            fn ids(&self, e: &Entry) -> Vec<Id> {
                // The test's bodies begin with their ID's first 4 bytes; find it among 0..2000.
                (0..2000u64)
                    .map(id)
                    .filter(|i| e.data.starts_with(&i[..4]))
                    .collect()
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let seg = 16 << 10;
        let s = Segments::open(dir.path().join("seg"), opts(seg, 4 * seg)).unwrap();
        let (hot, pinned, cold) = (id(1), id(2), id(3));
        let bodies: HashMap<Id, Vec<u8>> = [hot, pinned, cold]
            .iter()
            .map(|i| (*i, body(i, 500)))
            .collect();
        let policy = Policy {
            pinned: [pinned].into(),
        };
        for i in [hot, pinned, cold] {
            s.append(&[raw(3, &bodies[&i], &[i])], &policy).unwrap();
        }
        // Write well past the budget, reading the hot entry now and then, as a build would.
        for n in 1000..1400u64 {
            let i = id(n);
            let b = body(&i, 400);
            s.append(&[raw(3, &b, &[i])], &policy).unwrap();
            if n % 10 == 0 {
                assert!(get_data(&s, &hot).is_some(), "the hot entry left at {n}");
            }
        }
        assert!(
            s.bytes() <= 6 * seg,
            "{} bytes held, budget {}",
            s.bytes(),
            4 * seg
        );
        assert_eq!(
            get_data(&s, &hot).as_ref(),
            Some(&bodies[&hot]),
            "a read entry survives"
        );
        assert_eq!(
            get_data(&s, &pinned).as_ref(),
            Some(&bodies[&pinned]),
            "a pinned entry survives"
        );
        assert_eq!(get_data(&s, &cold), None, "an entry never read leaves");
        // Unread filler from the start is gone too.
        assert_eq!(get_data(&s, &id(1000)), None);
    }

    #[test]
    fn a_superseded_entry_is_not_copied_forward() {
        struct Ids;
        impl Retire for Ids {
            fn pinned(&self) -> HashSet<Id> {
                HashSet::new()
            }
            fn ids(&self, e: &Entry) -> Vec<Id> {
                (0..2000u64)
                    .map(id)
                    .filter(|i| e.data.starts_with(&i[..4]))
                    .collect()
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let seg = 16 << 10;
        let s = Segments::open(dir.path().join("seg"), opts(seg, 3 * seg)).unwrap();
        let t = id(7);
        let mut first = body(&t, 300);
        first.push(1);
        let mut second = body(&t, 300);
        second.push(2);
        s.append(&[raw(3, &first, &[t])], &Ids).unwrap();
        s.get(&t, &mut |_| true).unwrap();
        s.append(&[raw(3, &second, &[t])], &Ids).unwrap();
        for n in 1000..1300u64 {
            let i = id(n);
            let b = body(&i, 400);
            s.append(&[raw(3, &b, &[i])], &Ids).unwrap();
        }
        // The read, superseded first copy did not come forward: only the second answers.
        let mut seen = Vec::new();
        s.get(&t, &mut |e| {
            seen.push(*e.data.last().unwrap());
            false
        });
        assert!(
            !seen.contains(&1),
            "the superseded entry survived: {seen:?}"
        );
    }

    #[test]
    fn damage_is_a_miss_never_wrong_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg");
        let s = Segments::open(path.clone(), opts(1 << 20, 1 << 30)).unwrap();
        let k = id(1);
        s.append(
            &[NewEntry {
                key: Some(k),
                ..raw(0x81, b"manifest bytes", &[k])
            }],
            &KeepNothing,
        )
        .unwrap();
        // A torn side record and garbage at the segment's end are ignored.
        let active = s.read_meta().unwrap().active().unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(side_path(&path, active))
            .unwrap()
            .write_all(&[1, 2, 3])
            .unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(segment_path(&path, active))
            .unwrap()
            .write_all(&[0xff; 64])
            .unwrap();
        assert_eq!(s.get(&k, &mut |_| true).unwrap().data, b"manifest bytes");
        // A flipped byte in a keyed entry's bytes fails its checksum.
        let seg = segment_path(&path, active);
        let mut bytes = fs::read(&seg).unwrap();
        let at = bytes.windows(8).position(|w| w == b"manifest").unwrap();
        bytes[at] ^= 1;
        fs::write(&seg, &bytes).unwrap();
        let fresh = Segments::open(path.clone(), opts(1 << 20, 1 << 30)).unwrap();
        assert!(fresh.get(&k, &mut |_| true).is_none());
        // An unreadable meta starts the directory over.
        fs::write(path.join("meta"), b"garbage").unwrap();
        let reset = Segments::open(path, opts(1 << 20, 1 << 30)).unwrap();
        assert!(reset.get(&k, &mut |_| true).is_none());
        reset
            .append(&[raw(3, &body(&id(2), 10), &[id(2)])], &KeepNothing)
            .unwrap();
        assert!(get_data(&reset, &id(2)).is_some());
    }

    #[test]
    fn a_reader_keeps_reading_a_segment_another_process_retired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg");
        let seg = 16 << 10;
        // A reader that does not look again on its own however long the writes take: a loaded
        // machine's (macOS, its syncs full) take longer than REFRESH_EVERY.
        let reader = Segments::open(
            path.clone(),
            Options {
                refresh_every: Duration::from_secs(3600),
                ..opts(seg, 1 << 30)
            },
        )
        .unwrap();
        let writer = Segments::open(path.clone(), opts(seg, 2 * seg)).unwrap();
        let a = id(1);
        let da = body(&a, 1000);
        writer.append(&[raw(3, &da, &[a])], &KeepNothing).unwrap();
        for n in 10..40u64 {
            let i = id(n);
            writer
                .append(&[raw(3, &body(&i, 1000), &[i])], &KeepNothing)
                .unwrap();
        }
        // The reader maps a's segment, sealed by now.
        assert_eq!(get_data(&reader, &a), Some(da.clone()));
        let before = reader.read_meta().unwrap();
        for n in 40..400u64 {
            let i = id(n);
            writer
                .append(&[raw(3, &body(&i, 1000), &[i])], &KeepNothing)
                .unwrap();
        }
        let after = reader.read_meta().unwrap();
        let first = before.segments[0].id;
        assert!(!after.live(first), "the writer retired a's segment");
        assert!(!segment_path(&path, first).exists());
        // The reader's view still holds it: the bytes read are still the entry's.
        assert_eq!(get_data(&reader, &a), Some(da));
        // Once it looks again, a is gone, and what is live is found.
        reader.refresh(true).unwrap();
        assert_eq!(get_data(&reader, &a), None);
        assert!(get_data(&reader, &id(399)).is_some());
    }

    #[test]
    fn concurrent_writers_lose_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg");
        let threads: Vec<_> = (0..4u64)
            .map(|t| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let s = Segments::open(path, opts(64 << 10, 1 << 40)).unwrap();
                    for n in 0..300u64 {
                        let i = id(t * 1000 + n);
                        s.append(&[raw(3, &body(&i, 100), &[i])], &KeepNothing)
                            .unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let s = Segments::open(path, opts(64 << 10, 1 << 40)).unwrap();
        for t in 0..4u64 {
            for n in 0..300u64 {
                let i = id(t * 1000 + n);
                assert_eq!(
                    get_data(&s, &i),
                    Some(body(&i, 100)),
                    "writer {t} entry {n}"
                );
            }
        }
    }
}
