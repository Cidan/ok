//! The local object cache: content-addressed, immutable, verified on arrival and again on read,
//! shared by every workspace of the repository on the machine, and kept under its budget. Its
//! entries live in segments (`crate::segments`) — a few large files — not a file each.
//!
//! What an entry is:
//!
//! - an object (a tree, commit, operation, view, symlink, or copy history): its canonical bytes; a
//!   tree may carry its sizes record (docs/scm.md, "Tree sizes") as a trailer;
//! - a file of one chunk (most files): its contents, whole, found by its file ID and by its
//!   chunk's ID, with no manifest;
//! - a chunk of a larger file, and that file's manifest;
//! - a tree's sizes record, alone, for a tree cached without it.
//!
//! Contents are kept as ok sends them — zstd, as packs store them — and decompressed on read;
//! what is written here is compressed before it is kept. Every read is checked against the ID it
//! is for, so nothing in the segments is trusted, and what was read recently is kept decoded in
//! memory.
//!
//! ```text
//! segments/        the entries (crate::segments)
//! outboxes/<h>     the outboxes whose entries pin cache entries: each a file naming one
//! op_frontier      operations whose whole ancestry was received (32-byte IDs)
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs;
use std::io;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::Weak;

use ok_encoding::Hasher;
use ok_encoding::Kind;
use ok_encoding::TreeSizes;

use crate::ids::Id;
use crate::ids::Key;
use crate::segments::Codec;
use crate::segments::Entry;
use crate::segments::NewEntry;
use crate::segments::Options;
use crate::segments::Retire;
use crate::segments::Segments;

/// The cache's size budget: what its segments may hold, compressed.
pub const BUDGET: u64 = 8 << 30;

/// A segment is sealed at this size: the unit the cache is retired by.
pub const SEGMENT_BYTES: u64 = 256 << 20;

/// How many operations the operation frontier keeps (`Cache::op_frontier`).
pub const FRONTIER_MAX: usize = 64;

/// What the cache keeps decoded in memory, by bytes.
const DECODED_BYTES: usize = 64 << 20;

/// The entry kinds that are not objects': a file of one chunk, whole; a chunk; a manifest; a
/// tree's sizes alone. Objects are stored under their own kind bytes.
const WHOLE: u8 = Kind::File as u8;
const CHUNK: u8 = Kind::Chunk as u8;
const MANIFEST: u8 = 0x81;
const SIZES: u8 = 0xa0;
/// A repository's zstd dictionary, as ok serves it (docs/scm.md, "Dictionaries"), found
/// by `dictionary_key` of its ID.
const DICTIONARY: u8 = 0xd0;

/// A file: its ID, size, and chunks in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub id: Id,
    pub size: u64,
    pub chunks: Vec<(Id, u32)>,
    /// The path the file was written at: the server orders chunks in packs by it.
    pub path_hint: String,
}

impl Manifest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(46 + self.path_hint.len() + self.chunks.len() * 36);
        out.extend_from_slice(&self.id);
        out.extend_from_slice(&self.size.to_be_bytes());
        out.extend_from_slice(&(self.path_hint.len() as u16).to_be_bytes());
        out.extend_from_slice(self.path_hint.as_bytes());
        out.extend_from_slice(&(self.chunks.len() as u32).to_be_bytes());
        for (id, len) in &self.chunks {
            out.extend_from_slice(id);
            out.extend_from_slice(&len.to_be_bytes());
        }
        out
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        let mut r = b;
        let mut take = |n: usize| -> Option<&[u8]> {
            if r.len() < n {
                return None;
            }
            let (head, tail) = r.split_at(n);
            r = tail;
            Some(head)
        };
        let id: Id = take(32)?.try_into().ok()?;
        let size = u64::from_be_bytes(take(8)?.try_into().ok()?);
        let hint_len = u16::from_be_bytes(take(2)?.try_into().ok()?) as usize;
        let path_hint = String::from_utf8(take(hint_len)?.to_vec()).ok()?;
        let n = u32::from_be_bytes(take(4)?.try_into().ok()?) as usize;
        let mut chunks = Vec::with_capacity(n.min(1 << 16));
        for _ in 0..n {
            let cid: Id = take(32)?.try_into().ok()?;
            let len = u32::from_be_bytes(take(4)?.try_into().ok()?);
            chunks.push((cid, len));
        }
        if !r.is_empty() || chunks.iter().map(|c| u64::from(c.1)).sum::<u64>() != size {
            return None;
        }
        Some(Self {
            id,
            size,
            chunks,
            path_hint,
        })
    }
}

/// A chunk's ID: BLAKE3 over the chunk kind byte and its bytes.
pub fn chunk_id(raw: &[u8]) -> Id {
    let mut h = Hasher::new(Kind::Chunk);
    h.update(raw);
    h.finalize()
}

/// An object's ID from its canonical bytes, if they carry the expected kind.
pub fn canonical_id(kind: Kind, canonical: &[u8]) -> Option<Id> {
    (canonical.first() == Some(&(kind as u8))).then(|| ok_encoding::hash(canonical))
}

pub struct Cache {
    dir: PathBuf,
    segments: Segments,
    decoded: Mutex<Decoded>,
    /// The dictionaries read, ready to decode with, by ID.
    dictionaries: Mutex<HashMap<u32, Arc<DictionaryDecoder>>>,
}

/// A decoder holding one dictionary, and when its entry was last marked used.
struct DictionaryDecoder {
    decoder: Mutex<ruzstd::decoding::FrameDecoder>,
    marked: Mutex<u64>,
}

/// Bytes as the cache keeps them: raw, or compressed — with a dictionary, which `dict` names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stored {
    pub codec: Codec,
    pub dict: u32,
    pub data: Vec<u8>,
}

impl Stored {
    pub fn raw(data: Vec<u8>) -> Self {
        Self {
            codec: Codec::Raw,
            dict: 0,
            data,
        }
    }
}

/// The ID a dictionary is found by in the cache.
fn dictionary_key(id: u32) -> Id {
    let mut h = blake3::Hasher::new();
    h.update(b"ok-dictionary\n");
    h.update(&id.to_le_bytes());
    *h.finalize().as_bytes()
}

/// Every cache this process has open, by directory: two repositories sharing one cache share one
/// `Cache`.
static OPEN: LazyLock<Mutex<HashMap<PathBuf, Weak<Cache>>>> = LazyLock::new(Default::default);

impl Cache {
    /// Opens the cache at `dir`, sized by the cache's budget.
    pub fn open(dir: PathBuf) -> io::Result<Arc<Self>> {
        Self::open_with(
            dir,
            Options {
                segment_bytes: SEGMENT_BYTES,
                refresh_every: crate::segments::REFRESH_EVERY,
                budget: BUDGET,
            },
        )
    }

    /// Opens the cache at `dir` with the given segment size and budget. A cache from before
    /// segments — a file per entry — is moved aside and removed in the background.
    pub fn open_with(dir: PathBuf, opts: Options) -> io::Result<Arc<Self>> {
        fs::create_dir_all(&dir)?;
        let dir = dunce::canonicalize(&dir)?;
        let mut open = OPEN.lock().unwrap();
        if let Some(c) = open.get(&dir).and_then(Weak::upgrade) {
            return Ok(c);
        }
        retire_legacy(&dir);
        let cache = Arc::new(Self {
            segments: Segments::open(dir.join("segments"), opts)?,
            decoded: Mutex::new(Decoded::default()),
            dictionaries: Mutex::new(HashMap::new()),
            dir: dir.clone(),
        });
        open.insert(dir, Arc::downgrade(&cache));
        Ok(cache)
    }

    /// The cache's directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The bytes the cache's segments hold.
    pub fn bytes(&self) -> u64 {
        self.segments.bytes()
    }

    /// How many entries of a kind the cache holds — `Kind::Operation as u8`, say — counting every
    /// entry of every segment: for tests and diagnostics.
    pub fn count(&self, kind: u8) -> usize {
        self.segments.census().get(&kind).map_or(0, |c| c.0)
    }

    /// Bytes this process holds decoded, the read marked in the segments while it counts.
    fn remembered(&self, tag: &(u8, Id), kinds: &[u8]) -> Option<Vec<u8>> {
        let (b, mark) = self
            .decoded
            .lock()
            .unwrap()
            .get(tag, self.segments.epoch())?;
        if mark {
            self.segments.touch(&tag.1, kinds);
        }
        Some(b.to_vec())
    }

    fn remember(&self, tag: (u8, Id), b: &[u8]) {
        self.decoded
            .lock()
            .unwrap()
            .put(tag, b, self.segments.epoch());
    }

    fn append(&self, entries: &[NewEntry]) -> io::Result<()> {
        self.segments.append(entries, &Policy { cache: self })
    }

    /// An object's canonical bytes, if cached and intact.
    pub fn object(&self, key: Key) -> Option<Vec<u8>> {
        let tag = (key.kind as u8, key.id);
        if let Some(b) = self.remembered(&tag, &[key.kind as u8]) {
            return Some(b);
        }
        let mut found = None;
        self.segments.get(&key.id, &mut |e| {
            if e.kind != key.kind as u8 {
                return false;
            }
            match self
                .decode(e)
                .filter(|b| canonical_id(key.kind, b) == Some(key.id))
            {
                Some(b) => {
                    found = Some(b);
                    true
                }
                None => false,
            }
        })?;
        let b = found?;
        self.remember(tag, &b);
        Some(b)
    }

    /// Caches an object after checking its canonical bytes hash to its ID.
    pub fn put_object(&self, key: Key, canonical: &[u8]) -> io::Result<()> {
        if canonical_id(key.kind, canonical) != Some(key.id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} {} does not hash to its ID", key.kind.name(), key.hex()),
            ));
        }
        self.put_objects(&[(key, canonical)])
    }

    /// Caches objects, each checked, in one append.
    pub fn put_objects(&self, objects: &[(Key, &[u8])]) -> io::Result<()> {
        let ids: Vec<[Id; 1]> = objects.iter().map(|(k, _)| [k.id]).collect();
        let entries: Vec<NewEntry> = objects
            .iter()
            .zip(&ids)
            .filter(|((key, canonical), _)| canonical_id(key.kind, canonical) == Some(key.id))
            .map(|((key, canonical), index)| raw_entry(key.kind as u8, canonical, index))
            .collect();
        self.append(&entries)
    }

    /// Caches a tree with its sizes record, after checking both.
    pub fn put_tree_with_sizes(&self, id: &Id, canonical: &[u8], sizes: &[u8]) -> io::Result<()> {
        if canonical_id(Kind::Tree, canonical) != Some(*id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("tree {} does not hash to its ID", hex::encode(id)),
            ));
        }
        sizes_record(id, sizes)?;
        let index = [*id];
        self.append(&[NewEntry {
            trailer: sizes,
            ..raw_entry(Kind::Tree as u8, canonical, &index)
        }])
    }

    /// A chunk's bytes, if cached and intact: a chunk of a large file, or a whole small file's.
    pub fn chunk(&self, id: &Id) -> Option<Vec<u8>> {
        let tag = (CHUNK, *id);
        if let Some(b) = self.remembered(&tag, &[CHUNK, WHOLE]) {
            return Some(b);
        }
        let mut found = None;
        self.segments.get(id, &mut |e| {
            if e.kind != CHUNK && e.kind != WHOLE {
                return false;
            }
            match self.decode(e).filter(|b| chunk_id(b) == *id) {
                Some(b) => {
                    found = Some(b);
                    true
                }
                None => false,
            }
        })?;
        let b = found?;
        self.remember(tag, &b);
        Some(b)
    }

    /// Whether a chunk is cached (not read, nor checked).
    /// Whether the cache holds a chunk, as far as it has seen: it may miss one another process
    /// cached a moment ago. For staging, where a miss costs only staging it again.
    pub fn holds_chunk(&self, id: &Id) -> bool {
        self.decoded.lock().unwrap().contains(&(CHUNK, *id))
            || self
                .segments
                .peek(id, &mut |e| e.kind == CHUNK || e.kind == WHOLE)
                .is_some()
    }

    /// Whether the cache holds a file — whole, or its manifest and every chunk — as holds_chunk
    /// sees.
    pub fn holds_file(&self, id: &Id) -> bool {
        if self.decoded.lock().unwrap().contains(&(WHOLE, *id))
            || self.segments.peek(id, &mut |e| e.kind == WHOLE).is_some()
        {
            return true;
        }
        self.manifest_in(id, true)
            .is_some_and(|m| m.chunks.iter().all(|(c, _)| self.holds_chunk(c)))
    }

    pub fn has_chunk(&self, id: &Id) -> bool {
        self.decoded.lock().unwrap().contains(&(CHUNK, *id))
            || self
                .segments
                .get(id, &mut |e| e.kind == CHUNK || e.kind == WHOLE)
                .is_some()
    }

    /// Caches a chunk after checking it hashes to its ID, compressed.
    pub fn put_chunk(&self, id: &Id, raw: &[u8]) -> io::Result<()> {
        if chunk_id(raw) != *id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk {} does not hash to its ID", hex::encode(id)),
            ));
        }
        let stored = compress(raw);
        let index = [*id];
        self.append(&[stored_entry(CHUNK, &stored, raw.len(), &index)])
    }

    /// Caches a chunk as ok sent it: `stored` is `raw` as ok stores it. The caller checked
    /// `raw` against the ID.
    pub fn put_chunk_stored(&self, id: &Id, raw: &[u8], stored: &Stored) -> io::Result<()> {
        self.remember((CHUNK, *id), raw);
        let index = [*id];
        self.append(&[stored_entry(CHUNK, stored, raw.len(), &index)])
    }

    /// Caches a file of one chunk, whole — found by its file ID and its chunk's — after checking
    /// it hashes to both, compressed.
    pub fn put_whole(&self, file: &Id, raw: &[u8]) -> io::Result<()> {
        if file_id(raw) != *file {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("file {} does not hash to its ID", hex::encode(file)),
            ));
        }
        let stored = compress(raw);
        let index = [*file, chunk_id(raw)];
        self.append(&[stored_entry(WHOLE, &stored, raw.len(), &index)])
    }

    /// Caches a file of one chunk, whole, as ok sent it. The caller checked `raw` against both
    /// IDs.
    pub fn put_whole_stored(
        &self,
        file: &Id,
        chunk: &Id,
        raw: &[u8],
        stored: &Stored,
    ) -> io::Result<()> {
        self.remember((WHOLE, *file), raw);
        let index = [*file, *chunk];
        self.append(&[stored_entry(WHOLE, stored, raw.len(), &index)])
    }

    /// Whether a dictionary is cached (not read, nor checked).
    pub fn has_dictionary(&self, id: u32) -> bool {
        self.dictionaries.lock().unwrap().contains_key(&id)
            || self
                .segments
                .get(&dictionary_key(id), &mut |e| e.kind == DICTIONARY)
                .is_some()
    }

    /// Caches a dictionary, after checking it is one and carries `id`.
    pub fn put_dictionary(&self, id: u32, body: &[u8]) -> io::Result<()> {
        let dict = ruzstd::decoding::Dictionary::decode_dict(body).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("dictionary {id}: {e}"))
        })?;
        if dict.id != id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("dictionary {id} names itself {}", dict.id),
            ));
        }
        let key = dictionary_key(id);
        let index = [key];
        self.append(&[NewEntry {
            key: Some(key),
            ..raw_entry(DICTIONARY, body, &index)
        }])?;
        self.ready(id, dict);
        Ok(())
    }

    /// A dictionary ready to decode with: read before in this process, or from the segments.
    fn dictionary(&self, id: u32) -> Option<Arc<DictionaryDecoder>> {
        if let Some(d) = self.dictionaries.lock().unwrap().get(&id) {
            return Some(d.clone());
        }
        let key = dictionary_key(id);
        let mut found = None;
        self.segments.get(&key, &mut |e| {
            if e.kind != DICTIONARY || e.key != Some(key) {
                return false;
            }
            match ruzstd::decoding::Dictionary::decode_dict(&e.data) {
                Ok(d) if d.id == id => {
                    found = Some(d);
                    true
                }
                _ => false,
            }
        })?;
        Some(self.ready(id, found?))
    }

    fn ready(&self, id: u32, dict: ruzstd::decoding::Dictionary) -> Arc<DictionaryDecoder> {
        let mut decoder = ruzstd::decoding::FrameDecoder::new();
        let _ = decoder.add_dict(dict);
        let d = Arc::new(DictionaryDecoder {
            decoder: Mutex::new(decoder),
            marked: Mutex::new(self.segments.epoch()),
        });
        self.dictionaries
            .lock()
            .unwrap()
            .entry(id)
            .or_insert(d)
            .clone()
    }

    /// Bytes as kept, decoded to `raw_len`: with the cache's dictionaries for a dictionary's.
    pub fn decode_stored(&self, stored: &Stored, raw_len: usize) -> Option<Vec<u8>> {
        self.decode(&Entry::detached(
            stored.codec,
            stored.dict,
            raw_len as u64,
            stored.data.clone(),
        ))
    }

    /// An entry's bytes, decoded; None if they do not decode to their length, or name a
    /// dictionary the cache does not hold.
    fn decode(&self, e: &Entry) -> Option<Vec<u8>> {
        match e.codec {
            Codec::Raw => Some(e.data.clone()),
            Codec::Zstd => {
                if e.raw_len > MAX_DECODED {
                    return None;
                }
                let mut out = Vec::with_capacity(e.raw_len as usize);
                ruzstd::decoding::FrameDecoder::new()
                    .decode_all_to_vec(&e.data, &mut out)
                    .ok()?;
                (out.len() as u64 == e.raw_len).then_some(out)
            }
            Codec::ZstdDict => {
                if e.raw_len > MAX_DECODED {
                    return None;
                }
                let d = self.dictionary(e.dict)?;
                let mut out = Vec::with_capacity(e.raw_len as usize);
                d.decoder
                    .lock()
                    .unwrap()
                    .decode_all_to_vec(&e.data, &mut out)
                    .ok()?;
                // The dictionary lives as long as what it decodes is read: its entry is marked
                // used once each time the segments change.
                let epoch = self.segments.epoch();
                let mut marked = d.marked.lock().unwrap();
                if *marked != epoch {
                    *marked = epoch;
                    self.segments.touch(&dictionary_key(e.dict), &[DICTIONARY]);
                }
                (out.len() as u64 == e.raw_len).then_some(out)
            }
        }
    }

    /// A file of one chunk's contents, if cached whole and intact.
    pub fn whole(&self, file: &Id) -> Option<Vec<u8>> {
        let tag = (WHOLE, *file);
        if let Some(b) = self.remembered(&tag, &[WHOLE]) {
            return Some(b);
        }
        let mut found = None;
        self.segments.get(file, &mut |e| {
            if e.kind != WHOLE {
                return false;
            }
            match self.decode(e).filter(|b| file_id(b) == *file) {
                Some(b) => {
                    found = Some(b);
                    true
                }
                None => false,
            }
        })?;
        let b = found?;
        self.remember(tag, &b);
        Some(b)
    }

    /// A file's manifest, if cached and every chunk it names is cached too: a whole file's is its
    /// one chunk. A manifest stored from a manifest-only read, before its chunks, is not one
    /// until they are ([`Cache::manifest`] reads it).
    pub fn file(&self, id: &Id) -> Option<Manifest> {
        if let Some(raw) = self.whole(id) {
            let m = Manifest::whole(*id, &raw);
            // Its one chunk is read next, by its chunk's ID: already decoded and checked.
            if let [(c, _)] = m.chunks.as_slice() {
                self.remember((CHUNK, *c), &raw);
            }
            return Some(m);
        }
        let m = self.stored_manifest(id)?;
        m.chunks.iter().all(|(c, _)| self.has_chunk(c)).then_some(m)
    }

    /// Whether a file's contents are all cached (not read, nor checked).
    pub fn has_file(&self, id: &Id) -> bool {
        if self.decoded.lock().unwrap().contains(&(WHOLE, *id))
            || self.segments.get(id, &mut |e| e.kind == WHOLE).is_some()
        {
            return true;
        }
        self.stored_manifest(id)
            .is_some_and(|m| m.chunks.iter().all(|(c, _)| self.has_chunk(c)))
    }

    /// A file's manifest, if cached, whether or not its chunks are: what a manifest-only read
    /// (okfs's sizes, and its ranged reads) stores; a whole file's is its one chunk.
    pub fn manifest(&self, id: &Id) -> Option<Manifest> {
        if let Some(m) = self.stored_manifest(id) {
            return Some(m);
        }
        self.whole(id).map(|raw| Manifest::whole(*id, &raw))
    }

    fn stored_manifest(&self, id: &Id) -> Option<Manifest> {
        self.manifest_in(id, false)
    }

    /// A manifest the segments hold: peeked (no look again on a miss) or got.
    fn manifest_in(&self, id: &Id, peek: bool) -> Option<Manifest> {
        let mut found = None;
        let mut accept = |e: &crate::segments::Entry| {
            if e.kind != MANIFEST || e.key != Some(*id) {
                return false;
            }
            match Manifest::decode(&e.data).filter(|m| m.id == *id) {
                Some(m) => {
                    found = Some(m);
                    true
                }
                None => false,
            }
        };
        if peek {
            self.segments.peek(id, &mut accept)?;
        } else {
            self.segments.get(id, &mut accept)?;
        }
        found
    }

    /// Caches a manifest. A file of one chunk whose chunk is cached is kept whole instead.
    pub fn put_file(&self, m: &Manifest) -> io::Result<()> {
        if let [(c, _)] = m.chunks.as_slice()
            && let Some(raw) = self.chunk(c)
            && file_id(&raw) == m.id
        {
            return self.put_whole(&m.id, &raw);
        }
        let data = m.encode();
        let index = [m.id];
        self.append(&[NewEntry {
            key: Some(m.id),
            ..raw_entry(MANIFEST, &data, &index)
        }])
    }

    /// A tree's sizes record (docs/scm.md, "Tree sizes"), if cached and well formed: with
    /// its tree, or alone. The SCM derives it, so it cannot be checked against the tree's ID;
    /// where it is used, its entry count is checked against the tree's (`Shared::tree_sizes`).
    pub fn tree_sizes(&self, tree: &Id) -> Option<TreeSizes> {
        let mut found = None;
        self.segments.get(tree, &mut |e| {
            let record = match e.kind {
                k if k == Kind::Tree as u8 && !e.trailer.is_empty() => {
                    if !self
                        .decode(e)
                        .is_some_and(|b| canonical_id(Kind::Tree, &b) == Some(*tree))
                    {
                        return false;
                    }
                    &e.trailer
                }
                SIZES if e.key == Some(*tree) => &e.data,
                _ => return false,
            };
            match ok_encoding::decode_tree_sizes(record) {
                Ok(s) => {
                    found = Some(s);
                    true
                }
                Err(_) => false,
            }
        })?;
        found
    }

    /// Caches a tree's sizes record, alone, after checking it decodes.
    pub fn put_tree_sizes(&self, tree: &Id, record: &[u8]) -> io::Result<()> {
        sizes_record(tree, record)?;
        let index = [*tree];
        self.append(&[NewEntry {
            key: Some(*tree),
            ..raw_entry(SIZES, record, &index)
        }])
    }

    /// Registers an outbox whose entries — and what they reference — must survive retirement.
    pub fn register_outbox(&self, outbox: &Path) -> io::Result<()> {
        let dir = self.dir.join("outboxes");
        fs::create_dir_all(&dir)?;
        let path = outbox.to_string_lossy().into_owned();
        let name = &blake3::hash(path.as_bytes()).to_hex()[..16];
        let file = dir.join(name);
        if fs::read_to_string(&file).is_ok_and(|p| p == path) {
            return Ok(());
        }
        let mut tmp = tempfile::NamedTempFile::new_in(&dir)?;
        tmp.write_all(path.as_bytes())?;
        tmp.persist(file).map(|_| ()).map_err(|e| e.error)
    }

    /// The IDs the registered outboxes hold or reference. An outbox that no longer exists is
    /// forgotten.
    fn pinned(&self) -> HashSet<Id> {
        let mut out = HashSet::new();
        let Ok(entries) = fs::read_dir(self.dir.join("outboxes")) else {
            return out;
        };
        for e in entries.flatten() {
            let Ok(path) = fs::read_to_string(e.path()) else {
                continue;
            };
            let outbox = PathBuf::from(path);
            if !outbox.exists() {
                let _ = fs::remove_file(e.path());
                continue;
            }
            out.extend(crate::outbox::pinned_at(&outbox));
        }
        out
    }

    /// Operations whose whole ancestry the cache received (`op_frontier`, newest first): a fetch
    /// of missing operations names them as held, so the server sends only what lies beyond them.
    /// Eviction may since have taken some of their ancestors; a read that then misses one reads
    /// it directly.
    pub fn op_frontier(&self) -> Vec<Id> {
        let Ok(b) = fs::read(self.dir.join("op_frontier")) else {
            return Vec::new();
        };
        b.as_chunks::<32>().0.to_vec()
    }

    /// Adds operations whose whole ancestry is now cached to the front of the frontier, keeping
    /// the newest FRONTIER_MAX. Concurrent writers each replace the file whole; one's additions
    /// may be lost, which costs only a larger fetch later.
    pub fn add_op_frontier(&self, ops: &[Id]) -> io::Result<()> {
        let mut out: Vec<Id> = Vec::with_capacity(FRONTIER_MAX);
        for id in ops.iter().chain(self.op_frontier().iter()) {
            if out.len() == FRONTIER_MAX {
                break;
            }
            if !out.contains(id) {
                out.push(*id);
            }
        }
        let mut tmp = tempfile::NamedTempFile::new_in(&self.dir)?;
        tmp.write_all(&out.concat())?;
        tmp.persist(self.dir.join("op_frontier"))
            .map(|_| ())
            .map_err(|e| e.error)
    }
}

/// Entries to cache in one append: what a batched read or a retired publish brings. Each is
/// checked against its ID before it is kept.
#[derive(Default)]
pub struct Puts {
    objects: Vec<(Key, Vec<u8>)>,
    trees: Vec<(Id, Vec<u8>, Vec<u8>)>,
    wholes: Vec<(Id, Id, Stored, usize)>,
    chunks: Vec<(Id, Stored, usize)>,
    manifests: Vec<Manifest>,
    sizes: Vec<(Id, Vec<u8>)>,
}

impl Puts {
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
            && self.trees.is_empty()
            && self.wholes.is_empty()
            && self.chunks.is_empty()
            && self.manifests.is_empty()
            && self.sizes.is_empty()
    }

    /// The bytes held, roughly: a caller flushes past a bound.
    pub fn bytes(&self) -> usize {
        self.objects.iter().map(|o| o.1.len()).sum::<usize>()
            + self
                .trees
                .iter()
                .map(|t| t.1.len() + t.2.len())
                .sum::<usize>()
            + self.wholes.iter().map(|w| w.2.data.len()).sum::<usize>()
            + self.chunks.iter().map(|c| c.1.data.len()).sum::<usize>()
    }

    /// An object, by its canonical bytes; one that does not hash to its ID is dropped.
    pub fn object(&mut self, key: Key, canonical: Vec<u8>) {
        if canonical_id(key.kind, &canonical) == Some(key.id) {
            self.objects.push((key, canonical));
        }
    }

    /// A tree with its sizes record.
    pub fn tree_with_sizes(&mut self, id: Id, canonical: Vec<u8>, sizes: Vec<u8>) {
        if canonical_id(Kind::Tree, &canonical) == Some(id) && sizes_record(&id, &sizes).is_ok() {
            self.trees.push((id, canonical, sizes));
        }
    }

    /// A file of one chunk, whole, as `stored` keeps `raw`; checked against both IDs.
    pub fn whole(&mut self, file: Id, raw: &[u8], stored: Stored) {
        if file_id(raw) == file {
            self.wholes.push((file, chunk_id(raw), stored, raw.len()));
        }
    }

    /// A file of one chunk, whole, written here: compressed before it is kept.
    pub fn whole_raw(&mut self, file: Id, raw: &[u8]) {
        self.whole(file, raw, compress(raw));
    }

    /// A chunk of a larger file, as `stored` keeps `raw`; checked against its ID.
    pub fn chunk(&mut self, id: Id, raw: &[u8], stored: Stored) {
        if chunk_id(raw) == id {
            self.chunks.push((id, stored, raw.len()));
        }
    }

    /// A chunk written here: compressed before it is kept.
    pub fn chunk_raw(&mut self, id: Id, raw: &[u8]) {
        self.chunk(id, raw, compress(raw));
    }

    /// A manifest of a file of more than one chunk, or one read without its contents.
    pub fn manifest(&mut self, m: Manifest) {
        self.manifests.push(m);
    }

    /// A tree's sizes record, alone.
    pub fn sizes(&mut self, tree: Id, record: Vec<u8>) {
        if sizes_record(&tree, &record).is_ok() {
            self.sizes.push((tree, record));
        }
    }
}

impl Cache {
    /// Caches everything in `p`, in one append.
    pub fn put_all(&self, p: &Puts) -> io::Result<()> {
        if p.is_empty() {
            return Ok(());
        }
        let mut index: Vec<Vec<Id>> = Vec::new();
        for (k, _) in &p.objects {
            index.push(vec![k.id]);
        }
        for (id, _, _) in &p.trees {
            index.push(vec![*id]);
        }
        for (file, chunk, ..) in &p.wholes {
            index.push(vec![*file, *chunk]);
        }
        for (id, ..) in &p.chunks {
            index.push(vec![*id]);
        }
        for m in &p.manifests {
            index.push(vec![m.id]);
        }
        for (tree, _) in &p.sizes {
            index.push(vec![*tree]);
        }
        let manifests: Vec<Vec<u8>> = p.manifests.iter().map(Manifest::encode).collect();
        let mut ix = index.iter();
        let mut entries = Vec::with_capacity(index.len());
        for (k, canonical) in &p.objects {
            entries.push(raw_entry(k.kind as u8, canonical, ix.next().unwrap()));
        }
        for (_, canonical, sizes) in &p.trees {
            entries.push(NewEntry {
                trailer: sizes,
                ..raw_entry(Kind::Tree as u8, canonical, ix.next().unwrap())
            });
        }
        for (_, _, stored, raw_len) in &p.wholes {
            entries.push(stored_entry(WHOLE, stored, *raw_len, ix.next().unwrap()));
        }
        for (_, stored, raw_len) in &p.chunks {
            entries.push(stored_entry(CHUNK, stored, *raw_len, ix.next().unwrap()));
        }
        for (m, data) in p.manifests.iter().zip(&manifests) {
            entries.push(NewEntry {
                key: Some(m.id),
                ..raw_entry(MANIFEST, data, ix.next().unwrap())
            });
        }
        for (tree, record) in &p.sizes {
            entries.push(NewEntry {
                key: Some(*tree),
                ..raw_entry(SIZES, record, ix.next().unwrap())
            });
        }
        self.append(&entries)
    }

    /// Whether a file's size is known here without asking ok: it is cached whole, or its
    /// manifest is (not read, nor checked).
    pub fn knows_size(&self, id: &Id) -> bool {
        self.decoded.lock().unwrap().contains(&(WHOLE, *id))
            || self
                .segments
                .get(id, &mut |e| {
                    e.kind == WHOLE || (e.kind == MANIFEST && e.key == Some(*id))
                })
                .is_some()
    }
}

/// What retirement needs from the cache: the outboxes' pins, and each entry's IDs.
struct Policy<'a> {
    cache: &'a Cache,
}

impl Retire for Policy<'_> {
    fn pinned(&self) -> HashSet<Id> {
        self.cache.pinned()
    }

    fn ids(&self, e: &Entry) -> Vec<Id> {
        if e.kind == DICTIONARY {
            return e.key.into_iter().collect();
        }
        let Some(raw) = self.cache.decode(e) else {
            return Vec::new();
        };
        match e.kind {
            WHOLE => vec![file_id(&raw), chunk_id(&raw)],
            CHUNK => vec![chunk_id(&raw)],
            k => Kind::from_byte(k)
                .and_then(|kind| canonical_id(kind, &raw))
                .into_iter()
                .collect(),
        }
    }
}

fn raw_entry<'a>(kind: u8, data: &'a [u8], index: &'a [Id]) -> NewEntry<'a> {
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

fn stored_entry<'a>(kind: u8, stored: &'a Stored, raw_len: usize, index: &'a [Id]) -> NewEntry<'a> {
    NewEntry {
        codec: stored.codec,
        dict: stored.dict,
        raw_len: raw_len as u64,
        ..raw_entry(kind, &stored.data, index)
    }
}

/// The largest entry decoded: no chunk, file, or object is larger than a message carries.
const MAX_DECODED: u64 = crate::client::MAX_MESSAGE as u64;

/// Bytes worth compressing compressed; the rest raw.
fn compress(raw: &[u8]) -> Stored {
    if raw.len() >= 64 {
        let z = ruzstd::encoding::compress_to_vec(raw, ruzstd::encoding::CompressionLevel::Fastest);
        if z.len() < raw.len() - raw.len() / 16 {
            return Stored {
                codec: Codec::Zstd,
                dict: 0,
                data: z,
            };
        }
    }
    Stored::raw(raw.to_vec())
}

fn sizes_record(tree: &Id, record: &[u8]) -> io::Result<()> {
    ok_encoding::decode_tree_sizes(record)
        .map(|_| ())
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("tree {}'s sizes: {e}", hex::encode(tree)),
            )
        })
}

/// A file's ID: BLAKE3 over the file kind byte and its contents.
pub fn file_id(raw: &[u8]) -> Id {
    let mut h = Hasher::new(Kind::File);
    h.update(raw);
    h.finalize()
}

impl Manifest {
    /// The manifest of a file of one chunk, from its contents.
    fn whole(id: Id, raw: &[u8]) -> Self {
        Self {
            id,
            size: raw.len() as u64,
            chunks: if raw.is_empty() {
                Vec::new()
            } else {
                vec![(chunk_id(raw), raw.len() as u32)]
            },
            path_hint: String::new(),
        }
    }
}

/// What the cache keeps decoded, by entry kind and ID: CLOCK over a byte budget. A read answered
/// here still marks the entry used in the segments — until its counter is full, and again once
/// the segments change — so retirement sees what memory serves.
#[derive(Default)]
struct Decoded {
    map: HashMap<(u8, Id), Held>,
    order: VecDeque<(u8, Id)>,
    bytes: usize,
}

struct Held {
    bytes: Arc<[u8]>,
    visited: bool,
    /// Uses marked in the segments since `epoch`.
    marked: u8,
    epoch: u64,
}

/// A use counter's ceiling (`crate::segments`).
const MARKS: u8 = 3;

impl Decoded {
    /// The bytes, and whether the read should be marked in the segments.
    fn get(&mut self, k: &(u8, Id), epoch: u64) -> Option<(Arc<[u8]>, bool)> {
        let e = self.map.get_mut(k)?;
        e.visited = true;
        if e.epoch != epoch {
            e.epoch = epoch;
            e.marked = 0;
        }
        let mark = e.marked < MARKS;
        if mark {
            e.marked += 1;
        }
        Some((e.bytes.clone(), mark))
    }

    fn contains(&self, k: &(u8, Id)) -> bool {
        self.map.contains_key(k)
    }

    /// Keeps bytes just read from the segments (which marked that read) or just written.
    fn put(&mut self, k: (u8, Id), b: &[u8], epoch: u64) {
        if b.len() > DECODED_BYTES / 8 || self.map.contains_key(&k) {
            return;
        }
        while self.bytes + b.len() > DECODED_BYTES {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            match self.map.get_mut(&old) {
                Some(e) if e.visited => {
                    e.visited = false;
                    self.order.push_back(old);
                }
                Some(_) => {
                    let e = self.map.remove(&old).unwrap();
                    self.bytes -= e.bytes.len();
                }
                None => {}
            }
        }
        self.bytes += b.len();
        self.map.insert(
            k,
            Held {
                bytes: Arc::from(b),
                visited: false,
                marked: 1,
                epoch,
            },
        );
        self.order.push_back(k);
    }
}

/// Moves `dir` aside, for `clear_aside` to remove: a cache no longer used, whose hundred thousand
/// files must not hold up the command that found it.
pub fn move_aside(dir: &Path) {
    let Some(parent) = dir.parent() else {
        return;
    };
    let aside = parent.join(format!(
        "{}-legacy-{}-{}",
        dir.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    let _ = fs::rename(dir, aside);
}

/// Removes in the background what `move_aside` set aside from `parent`'s `name`. A command may end
/// before its removal does, so each open resumes what is left.
pub fn clear_aside(parent: &Path, name: &str) {
    let prefix = format!("{name}-legacy-");
    let aside: Vec<PathBuf> = fs::read_dir(parent)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(&prefix))
        .map(|e| e.path())
        .collect();
    if !aside.is_empty() {
        std::thread::spawn(move || {
            for d in aside {
                let _ = fs::remove_dir_all(d);
            }
        });
    }
}

/// The directories of a cache from before segments (a file per entry), moved aside and removed in
/// the background: removing a hundred thousand files must not hold up the command that opened it.
fn retire_legacy(dir: &Path) {
    for sub in ["objects", "chunks", "files", "sizes"] {
        let from = dir.join(sub);
        if from.is_dir() {
            let to = dir.join(format!(
                "legacy-{sub}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos())
            ));
            let _ = fs::rename(&from, &to);
        }
    }
    let legacy: Vec<PathBuf> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("legacy-"))
        .map(|e| e.path())
        .collect();
    if !legacy.is_empty() {
        std::thread::spawn(move || {
            for d in legacy {
                let _ = fs::remove_dir_all(d);
            }
        });
    }
}
