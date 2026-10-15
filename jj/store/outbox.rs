//! The outbox (`.jj/repo/store/outbox`): everything written locally and not yet published. A
//! write is durable before the store call returns — the working-copy state can record a tree ID
//! before the next publish — and an entry leaves only after the server acknowledged the publish
//! that carried it (then it moves into the cache).
//!
//! It is one append-only log, not a file per entry: an entry is its kind, flags, length, key, bytes,
//! and a checksum, and a write appends it and syncs the log once — one fsync where a file each cost
//! a file's and a directory's. Leaving the outbox appends a tombstone; once nothing in the log is
//! live it starts over, and once most of it is dead it is rewritten with what is live. A process
//! reads what others appended by reading on from where it stopped; a torn entry at the end — a
//! crash mid-write — is cut off by the next writer.
//!
//! ```text
//! log      the entries
//! .write   held by a process appending, or rewriting the log
//! .lock    held by a process publishing
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io;
use std::io::Write as _;
use std::os::unix::fs::FileExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;

use jj_lib::lock::FileLock;
use ok_encoding::Kind;

use crate::cache::Cache;
use crate::cache::Manifest;
use crate::cache::Puts;
use crate::cache::chunk_id;
use crate::ids::Id;
use crate::ids::Key;

const MAGIC: &[u8; 8] = b"qobx\x00\x01\x00\x00";
/// A chunk's entry kind; objects are their own kind bytes.
const CHUNK: u8 = Kind::Chunk as u8;
/// A file's manifest.
const MANIFEST: u8 = 0x81;
const TOMBSTONE: u8 = 1;
/// The log is rewritten with what is live once it holds this much and most of it is dead.
const COMPACT_AT: u64 = 16 << 20;

pub struct Outbox {
    dir: PathBuf,
    quarantine: PathBuf,
    state: Mutex<State>,
    /// Held while this process takes the write lock: a snapshot writes files from several threads,
    /// which queue here rather than each racing for the lock file.
    writing: Mutex<()>,
}

/// The log as this process has read it.
#[derive(Default)]
struct State {
    file: Option<File>,
    ino: u64,
    /// Bytes read: the next entry starts here.
    read: u64,
    /// Each live entry's bytes' offset and length, and the order entries were staged in.
    live: HashMap<(u8, Id), (u64, u32)>,
    order: Vec<(u8, Id)>,
    live_bytes: u64,
    dead_bytes: u64,
}

/// What the outbox held when listed.
#[derive(Default)]
pub struct Staged {
    pub objects: Vec<Key>,
    pub files: Vec<Manifest>,
}

impl Staged {
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.files.is_empty()
    }
}

fn checksum(kind: u8, flags: u8, key: &Id, data: &[u8]) -> [u8; 8] {
    let mut h = blake3::Hasher::new();
    h.update(b"ok-outbox-entry\n");
    h.update(&[kind, flags]);
    h.update(key);
    h.update(&(data.len() as u64).to_le_bytes());
    h.update(data);
    h.finalize().as_bytes()[..8].try_into().unwrap()
}

fn encode(kind: u8, flags: u8, key: &Id, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + 5 + 32 + data.len() + 8);
    out.push(kind);
    out.push(flags);
    let mut n = data.len() as u64;
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
    out.extend_from_slice(key);
    out.extend_from_slice(data);
    out.extend_from_slice(&checksum(kind, flags, key, data));
    out
}

/// An entry read from the log: its kind, flags, key, where its bytes are, and its length.
struct Read {
    kind: u8,
    flags: u8,
    key: Id,
    data_at: u64,
    data_len: u32,
    end: u64,
}

/// Reads the entry at `at` of `buf` (which starts at file offset `base`); None if it is not
/// whole or does not check.
fn parse(buf: &[u8], base: u64, at: usize) -> Option<Read> {
    let kind = *buf.get(at)?;
    let flags = *buf.get(at + 1)?;
    let mut i = at + 2;
    let mut len = 0u64;
    for shift in (0..64).step_by(7) {
        let b = *buf.get(i)?;
        i += 1;
        len |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            break;
        }
    }
    let len = usize::try_from(len)
        .ok()
        .filter(|l| *l <= crate::client::MAX_MESSAGE)?;
    let key: Id = buf.get(i..i + 32)?.try_into().ok()?;
    let data = buf.get(i + 32..i + 32 + len)?;
    let sum: [u8; 8] = buf.get(i + 32 + len..i + 40 + len)?.try_into().ok()?;
    if checksum(kind, flags, &key, data) != sum {
        return None;
    }
    Some(Read {
        kind,
        flags,
        key,
        data_at: base + (i + 32) as u64,
        data_len: len as u32,
        end: base + (i + 40 + len) as u64,
    })
}

impl Outbox {
    /// Opens the outbox at `dir`, carrying over what an outbox of a file per entry holds there.
    pub fn open(dir: PathBuf, quarantine: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;
        let this = Self {
            dir,
            quarantine,
            state: Mutex::new(State::default()),
            writing: Mutex::new(()),
        };
        {
            let _w = this.write_lock()?;
            let mut st = this.state.lock().unwrap();
            this.ensure_log(&mut st)?;
            this.read_on(&mut st)?;
            this.cut_torn_tail(&mut st)?;
            this.carry_over(&mut st)?;
        }
        Ok(this)
    }

    /// The outbox at `dir`, for reading only: what another repository sharing a cache pins.
    fn reader(dir: &Path) -> Self {
        Self {
            dir: dir.to_owned(),
            quarantine: PathBuf::new(),
            state: Mutex::new(State::default()),
            writing: Mutex::new(()),
        }
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join("log")
    }

    fn write_lock(&self) -> io::Result<(std::sync::MutexGuard<'_, ()>, FileLock)> {
        let local = self.writing.lock().unwrap();
        let file = FileLock::lock(self.dir.join(".write")).map_err(io::Error::other)?;
        Ok((local, file))
    }

    /// Serializes publishes from this machine's processes.
    pub fn lock(&self) -> io::Result<FileLock> {
        FileLock::lock(self.dir.join(".lock")).map_err(io::Error::other)
    }

    /// Creates the log if there is none, durably. Under the write lock.
    fn ensure_log(&self, st: &mut State) -> io::Result<()> {
        if self.log_path().exists() {
            return Ok(());
        }
        self.replace_log(st, &[])
    }

    /// Replaces the log with one holding `entries` (encoded), durably: written aside, synced,
    /// renamed, the directory synced. Under the write lock.
    fn replace_log(&self, st: &mut State, entries: &[u8]) -> io::Result<()> {
        let mut tmp = tempfile::NamedTempFile::new_in(&self.dir)?;
        tmp.write_all(MAGIC)?;
        tmp.write_all(entries)?;
        tmp.as_file().sync_all()?;
        tmp.persist(self.log_path()).map_err(|e| e.error)?;
        File::open(&self.dir)?.sync_all()?;
        *st = State::default();
        self.read_on(st)
    }

    /// Reads entries appended since this process last read; a log replaced since is read anew.
    /// Stops at an entry that is not whole: another process may be writing it.
    fn read_on(&self, st: &mut State) -> io::Result<()> {
        let meta = match fs::metadata(self.log_path()) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                *st = State::default();
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        if st.file.is_none() || meta.ino() != st.ino || meta.len() < st.read {
            *st = State {
                file: Some(
                    fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(self.log_path())?,
                ),
                ino: meta.ino(),
                read: MAGIC.len() as u64,
                ..State::default()
            };
        }
        let len = meta.len();
        if len <= st.read {
            return Ok(());
        }
        let file = st.file.as_ref().unwrap();
        let mut buf = vec![0; (len - st.read) as usize];
        file.read_exact_at(&mut buf, st.read)?;
        let base = st.read;
        let mut at = 0usize;
        while let Some(r) = parse(&buf, base, at) {
            at = (r.end - base) as usize;
            st.read = r.end;
            let k = (r.kind, r.key);
            if r.flags & TOMBSTONE != 0 {
                if let Some((_, l)) = st.live.remove(&k) {
                    st.live_bytes -= u64::from(l);
                    st.dead_bytes += u64::from(l);
                }
            } else if let std::collections::hash_map::Entry::Vacant(e) = st.live.entry(k) {
                e.insert((r.data_at, r.data_len));
                st.order.push(k);
                st.live_bytes += u64::from(r.data_len);
            } else {
                st.dead_bytes += u64::from(r.data_len);
            }
        }
        Ok(())
    }

    /// Cuts off an entry left torn at the end by a crash. Under the write lock, so no one is
    /// writing it.
    fn cut_torn_tail(&self, st: &mut State) -> io::Result<()> {
        let len = fs::metadata(self.log_path())?.len();
        if len > st.read {
            st.file.as_ref().unwrap().set_len(st.read)?;
        }
        Ok(())
    }

    /// Appends entries, synced if `durable`. Skips an entry already live; a tombstone always goes.
    fn append(&self, entries: &[(u8, u8, Id, &[u8])], durable: bool) -> io::Result<()> {
        let _w = self.write_lock()?;
        let mut st = self.state.lock().unwrap();
        self.ensure_log(&mut st)?;
        self.read_on(&mut st)?;
        self.cut_torn_tail(&mut st)?;
        let mut buf = Vec::new();
        for (kind, flags, key, data) in entries {
            if flags & TOMBSTONE == 0 && st.live.contains_key(&(*kind, *key)) {
                continue;
            }
            buf.extend_from_slice(&encode(*kind, *flags, key, data));
        }
        if buf.is_empty() {
            return Ok(());
        }
        let file = st.file.as_ref().unwrap();
        file.write_all_at(&buf, st.read)?;
        if durable {
            file.sync_data()?;
        }
        self.read_on(&mut st)
    }

    /// Carries over an outbox of a file per entry: each entry that checks is appended and the log
    /// synced, then exactly the files carried — and those that can never check — are removed, and a
    /// directory left empty. An older jj may still be writing there (a daemon of the old build, until
    /// it is replaced): what it writes after this reads is left for the next open, and its files in
    /// progress (a temporary file, renamed into place when written) are not touched. Under the
    /// write lock.
    fn carry_over(&self, st: &mut State) -> io::Result<()> {
        let mut found: Vec<(u8, Id, Vec<u8>)> = Vec::new();
        let mut done: Vec<PathBuf> = Vec::new();
        let mut legacy = Vec::new();
        for (sub, kind_of) in [
            ("objects", None),
            ("chunks", Some(CHUNK)),
            ("files", Some(MANIFEST)),
        ] {
            let Ok(rd) = fs::read_dir(self.dir.join(sub)) else {
                continue;
            };
            legacy.push(self.dir.join(sub));
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') {
                    continue;
                }
                let Ok(data) = fs::read(e.path()) else {
                    continue;
                };
                let parsed = match kind_of {
                    Some(k) => Some((k, name.as_str())),
                    None => name
                        .split_once('-')
                        .and_then(|(k, h)| Some((u8::from_str_radix(k, 16).ok()?, h))),
                };
                let entry = parsed.and_then(|(kind, hex)| {
                    let id = <Id>::try_from(hex::decode(hex).ok()?.as_slice()).ok()?;
                    let ok = match kind {
                        CHUNK => chunk_id(&data) == id,
                        MANIFEST => Manifest::decode(&data).is_some_and(|m| m.id == id),
                        k => {
                            Kind::from_byte(k)
                                .and_then(|kind| crate::cache::canonical_id(kind, &data))
                                == Some(id)
                        }
                    };
                    ok.then_some((kind, id))
                });
                // A file that does not check never will: it is removed with those carried.
                if let Some((kind, id)) = entry {
                    found.push((kind, id, data));
                }
                done.push(e.path());
            }
        }
        if legacy.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for (kind, id, data) in &found {
            if !st.live.contains_key(&(*kind, *id)) {
                buf.extend_from_slice(&encode(*kind, 0, id, data));
            }
        }
        if !buf.is_empty() {
            let file = st.file.as_ref().unwrap();
            file.write_all_at(&buf, st.read)?;
            file.sync_data()?;
            self.read_on(st)?;
        }
        for p in done {
            remove(&p)?;
        }
        for d in legacy {
            // Not empty: an older jj wrote there since; the next open carries it.
            let _ = fs::remove_dir(d);
        }
        Ok(())
    }

    fn bytes(&self, kind: u8, id: &Id) -> Option<Vec<u8>> {
        let mut st = self.state.lock().unwrap();
        self.read_on(&mut st).ok()?;
        let (at, len) = *st.live.get(&(kind, *id))?;
        let mut buf = vec![0; len as usize];
        st.file.as_ref()?.read_exact_at(&mut buf, at).ok()?;
        Some(buf)
    }

    fn has(&self, kind: u8, id: &Id) -> bool {
        let mut st = self.state.lock().unwrap();
        self.read_on(&mut st).is_ok() && st.live.contains_key(&(kind, *id))
    }

    pub fn stage_object(&self, key: Key, canonical: &[u8]) -> io::Result<()> {
        self.append(&[(key.kind as u8, 0, key.id, canonical)], true)
    }

    /// Stages a chunk. It is synced by `sync_chunks`, once for all of a file's chunks, before
    /// the file's manifest is staged.
    pub fn stage_chunk(&self, id: &Id, raw: &[u8]) -> io::Result<()> {
        self.append(&[(CHUNK, 0, *id, raw)], false)
    }

    /// Makes every staged chunk durable.
    pub fn sync_chunks(&self) -> io::Result<()> {
        let st = self.state.lock().unwrap();
        match &st.file {
            Some(f) => f.sync_data(),
            None => Ok(()),
        }
    }

    /// Stages a file's manifest; its chunks must already be staged.
    pub fn stage_file(&self, m: &Manifest) -> io::Result<()> {
        self.append(&[(MANIFEST, 0, m.id, &m.encode())], true)
    }

    /// Stages a file's chunks and then its manifest in one append, unsynced: the log is read in
    /// order and every entry checks, so after a crash a manifest is there only with its chunks
    /// before it; and the next durable append — the tree, commit, or checkpoint that names the
    /// file — makes it durable.
    pub fn stage_file_with(&self, chunks: &[(Id, &[u8])], m: &Manifest) -> io::Result<()> {
        let manifest = m.encode();
        let mut entries: Vec<(u8, u8, Id, &[u8])> = chunks
            .iter()
            .map(|(id, raw)| (CHUNK, 0, *id, *raw))
            .collect();
        entries.push((MANIFEST, 0, m.id, &manifest));
        self.append(&entries, false)
    }

    pub fn object(&self, key: Key) -> Option<Vec<u8>> {
        self.bytes(key.kind as u8, &key.id)
    }

    pub fn chunk(&self, id: &Id) -> Option<Vec<u8>> {
        self.bytes(CHUNK, id)
    }

    pub fn has_chunk(&self, id: &Id) -> bool {
        self.has(CHUNK, id)
    }

    pub fn file(&self, id: &Id) -> Option<Manifest> {
        Manifest::decode(&self.bytes(MANIFEST, id)?)
    }

    /// How many chunks are staged.
    pub fn staged_chunks(&self) -> usize {
        let mut st = self.state.lock().unwrap();
        if self.read_on(&mut st).is_err() {
            return 0;
        }
        st.live.keys().filter(|k| k.0 == CHUNK).count()
    }

    /// Everything staged now.
    pub fn list(&self) -> io::Result<Staged> {
        let mut st = self.state.lock().unwrap();
        self.read_on(&mut st)?;
        let mut staged = Staged::default();
        let file = st.file.as_ref();
        for k in &st.order {
            let Some((at, len)) = st.live.get(k) else {
                continue;
            };
            match k.0 {
                CHUNK => {}
                MANIFEST => {
                    let mut buf = vec![0; *len as usize];
                    if let Some(f) = file
                        && f.read_exact_at(&mut buf, *at).is_ok()
                        && let Some(m) = Manifest::decode(&buf)
                    {
                        staged.files.push(m);
                    }
                }
                kind => {
                    if let Some(kind) = Kind::from_byte(kind) {
                        staged.objects.push(Key::new(kind, k.1));
                    }
                }
            }
        }
        Ok(staged)
    }

    /// The operations staged now (for prefix resolution).
    pub fn operations(&self) -> Vec<Id> {
        self.list()
            .map(|s| {
                s.objects
                    .into_iter()
                    .filter(|k| k.kind == Kind::Operation)
                    .map(|k| k.id)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The IDs the outbox holds or references, which the cache must not evict.
    pub fn pinned(&self) -> HashSet<Id> {
        let mut pinned = HashSet::new();
        if let Ok(staged) = self.list() {
            for key in &staged.objects {
                pinned.insert(key.id);
                if let Some(bytes) = self.object(*key) {
                    for r in crate::refs::references(&bytes) {
                        pinned.insert(r.id);
                    }
                }
            }
            for m in &staged.files {
                pinned.insert(m.id);
                pinned.extend(m.chunks.iter().map(|c| c.0));
            }
        }
        pinned
    }

    /// Retires what a publish carried: each object, file, and chunk moves into the cache
    /// (the server holds it now, and it will likely be read again) and leaves the outbox. A
    /// chunk another still-staged file uses stays.
    pub fn retire(&self, published: &Staged, cache: &Cache) -> io::Result<()> {
        let mut puts = Puts::default();
        for key in &published.objects {
            if let Some(bytes) = self.object(*key) {
                puts.object(*key, bytes);
            }
        }
        for m in &published.files {
            match m.chunks.as_slice() {
                [(cid, _)] => {
                    if let Some(raw) = self.chunk(cid) {
                        puts.whole_raw(m.id, &raw);
                    }
                }
                chunks => {
                    for (cid, _) in chunks {
                        if let Some(raw) = self.chunk(cid) {
                            puts.chunk_raw(*cid, &raw);
                        }
                    }
                    puts.manifest(m.clone());
                }
            }
        }
        // Into the cache first: a crash between the two leaves an entry in both, never in neither.
        cache.put_all(&puts)?;
        let published_files: HashSet<Id> = published.files.iter().map(|m| m.id).collect();
        let still_needed: HashSet<Id> = self
            .list()?
            .files
            .into_iter()
            .filter(|m| !published_files.contains(&m.id))
            .flat_map(|m| m.chunks.into_iter().map(|c| c.0))
            .collect();
        let mut gone: Vec<(u8, u8, Id, &[u8])> = Vec::new();
        for key in &published.objects {
            gone.push((key.kind as u8, TOMBSTONE, key.id, &[]));
        }
        for m in &published.files {
            gone.push((MANIFEST, TOMBSTONE, m.id, &[]));
            for (cid, _) in &m.chunks {
                if !still_needed.contains(cid) {
                    gone.push((CHUNK, TOMBSTONE, *cid, &[]));
                }
            }
        }
        // Not synced: a tombstone a crash loses only publishes its entry again.
        self.append(&gone, false)?;
        self.tidy()
    }

    /// Starts the log over once nothing in it is live, and rewrites it with what is once most of
    /// it is dead.
    fn tidy(&self) -> io::Result<()> {
        let _w = self.write_lock()?;
        let mut st = self.state.lock().unwrap();
        self.read_on(&mut st)?;
        if st.live.is_empty() {
            if st.read > MAGIC.len() as u64 {
                self.replace_log(&mut st, &[])?;
            }
            return Ok(());
        }
        if st.read < COMPACT_AT || st.dead_bytes <= st.live_bytes {
            return Ok(());
        }
        let mut buf = Vec::new();
        let file = st.file.as_ref().unwrap();
        for k in &st.order {
            if let Some((at, len)) = st.live.get(k) {
                let mut data = vec![0; *len as usize];
                file.read_exact_at(&mut data, *at)?;
                buf.extend_from_slice(&encode(k.0, 0, &k.1, &data));
            }
        }
        self.replace_log(&mut st, &buf)
    }

    /// Moves a file's manifest the server can never accept out of the way, as quarantine moves
    /// an object. Returns where it went.
    pub fn quarantine_file(&self, m: &Manifest) -> io::Result<PathBuf> {
        fs::create_dir_all(&self.quarantine)?;
        let to = self
            .quarantine
            .join(format!("file-{}", Key::new(Kind::File, m.id).hex()));
        let mut tmp = tempfile::NamedTempFile::new_in(&self.quarantine)?;
        tmp.write_all(&m.encode())?;
        tmp.as_file().sync_all()?;
        tmp.persist(&to).map_err(|e| e.error)?;
        File::open(&self.quarantine)?.sync_all()?;
        self.append(&[(MANIFEST, TOMBSTONE, m.id, &[])], true)?;
        Ok(to)
    }

    /// Moves an object the server can never accept out of the way, keeping it for
    /// inspection. Returns where it went.
    pub fn quarantine(&self, key: Key) -> io::Result<PathBuf> {
        let bytes = self.object(key).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} {} is not staged", key.kind.name(), key.hex()),
            )
        })?;
        fs::create_dir_all(&self.quarantine)?;
        let to = self
            .quarantine
            .join(format!("{:02x}-{}", key.kind as u8, key.hex()));
        let mut tmp = tempfile::NamedTempFile::new_in(&self.quarantine)?;
        tmp.write_all(&bytes)?;
        tmp.as_file().sync_all()?;
        tmp.persist(&to).map_err(|e| e.error)?;
        File::open(&self.quarantine)?.sync_all()?;
        self.append(&[(key.kind as u8, TOMBSTONE, key.id, &[])], true)?;
        Ok(to)
    }
}

fn remove(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// The IDs the outbox at `dir` holds or references: what a cache it shares with others must keep
/// when another process retires its segments.
pub fn pinned_at(dir: &Path) -> HashSet<Id> {
    Outbox::reader(dir).pinned()
}
