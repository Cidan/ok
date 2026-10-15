//! The state a repository's three stores share, and the runtime the SCM client runs on.
//!
//! jj drives its stores from its own executor (`block_on`), with no tokio runtime; tonic
//! needs one. Every RPC therefore runs as a task on one process-wide runtime, and the
//! stores await its result through a channel, which works from any executor.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::Weak;

use ok_encoding::Hasher;
use ok_encoding::Kind;
use ok_encoding::TreeSizes;
use ok_proto::ok::scm::v1 as pb;
use tokio::runtime::Runtime;
use tonic::Status;

use crate::batch::Batcher;
use crate::cache::Cache;
use crate::cache::Manifest;
use crate::cache::Puts;
use crate::cache::Stored;
use crate::cache::canonical_id;
use crate::cache::chunk_id;
use crate::client::Client;
use crate::config::RepoConfig;
use crate::ids::Id;
use crate::ids::Key;
use crate::ids::id_from;
use crate::ids::kind_from_wire;
use crate::ids::kind_to_wire;
use crate::outbox::Outbox;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("ok-store")
        .enable_all()
        .build()
        .expect("start the ok store runtime")
});

/// Runs `fut` on the store runtime and awaits its result from whatever executor calls.
pub async fn run<F, T>(fut: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    RUNTIME.spawn(async move {
        let _ = tx.send(fut.await);
    });
    rx.await.expect("the ok store runtime dropped a task")
}

/// Runs `fut` on the store runtime and blocks this thread until its result: for synchronous
/// callers (the okfs daemon's file-system threads, and its snapshots), from inside an executor
/// or not — a nested executor's `block_on` would refuse to run. Never call it on the store
/// runtime's own threads.
pub fn block<F, T>(fut: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    RUNTIME.spawn(async move {
        let _ = tx.send(fut.await);
    });
    rx.recv().expect("the ok store runtime dropped a task")
}

/// Spawns `fut` on the store runtime without waiting for it.
pub fn spawn<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    RUNTIME.spawn(fut);
}

/// One repository's shared state.
pub struct Shared {
    pub repo_dir: PathBuf,
    pub config: RepoConfig,
    pub cache: Arc<Cache>,
    pub outbox: Outbox,
    pub client: Client,
    objects: Arc<Batcher<Key, Vec<u8>>>,
    commits: Arc<Batcher<Key, Vec<u8>>>,
    operations: Arc<Batcher<Key, Vec<u8>>>,
    files: Arc<Batcher<Id, Manifest>>,
    manifests: Arc<Batcher<Id, Manifest>>,
    chunks: Arc<Batcher<ChunkRead, Vec<u8>>>,
    tree_sizes: Arc<Batcher<Id, TreeSizes>>,
    /// Reads of files' contents and manifests that waited on ok.
    waits: Mutex<crate::calls::WaitStats>,
}

/// A chunk a ranged read is missing, with where it lies in the file it is read from: what the
/// chunk batcher asks the SCM for. Two reads of one chunk are one read, from whichever file.
#[derive(Clone, Copy, Debug)]
struct ChunkRead {
    chunk: Id,
    file: Id,
    offset: u64,
    len: u64,
}

impl PartialEq for ChunkRead {
    fn eq(&self, other: &Self) -> bool {
        self.chunk == other.chunk
    }
}

impl Eq for ChunkRead {}

impl std::hash::Hash for ChunkRead {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.chunk.hash(state);
    }
}

/// How many objects one batched read asks for.
const OBJECTS_PER_READ: usize = 1000;
/// How many files one batched file read streams.
const FILES_PER_READ: usize = 256;
/// How many manifests one manifest-only read asks for.
pub(crate) const MANIFESTS_PER_READ: usize = 1000;
/// How many trees one read of trees' sizes asks for.
const TREES_PER_SIZES_READ: usize = 1000;

static REGISTRY: LazyLock<Mutex<HashMap<PathBuf, Weak<Shared>>>> = LazyLock::new(Default::default);

/// Every repository's shared state this process still has open.
pub(crate) fn registered() -> Vec<Arc<Shared>> {
    REGISTRY
        .lock()
        .unwrap()
        .values()
        .filter_map(Weak::upgrade)
        .collect()
}

impl Shared {
    /// The shared state of the repository whose directory (`.jj/repo`) contains
    /// `store_path` — the backend's, the op store's, or the op heads store's directory.
    pub fn for_store_path(store_path: &Path) -> io::Result<Arc<Self>> {
        let repo_dir = dunce::canonicalize(store_path.parent().unwrap_or(store_path))?;
        let mut registry = REGISTRY.lock().unwrap();
        if let Some(shared) = registry.get(&repo_dir).and_then(Weak::upgrade) {
            return Ok(shared);
        }
        let store_dir = repo_dir.join("store");
        let config = RepoConfig::read(&store_dir)?;
        let cache_dir = config.cache_dir()?;
        // A cache from before the machine's: the repository's own, moved aside and removed.
        let own = store_dir.join("cache");
        if own.exists() && dunce::canonicalize(&own).ok() != dunce::canonicalize(&cache_dir).ok() {
            crate::cache::move_aside(&own);
        }
        crate::cache::clear_aside(&store_dir, "cache");
        let cache = Cache::open(cache_dir)?;
        let client = Client::new(
            &config.endpoint,
            config.repository_id,
            config.dev_principal.clone(),
            config.workspace.clone(),
        )?;
        let shared = Arc::new(Self {
            objects: object_batcher(client.clone(), cache.clone()),
            commits: commit_batcher(client.clone(), cache.clone()),
            operations: operation_batcher(client.clone(), cache.clone()),
            files: file_batcher(client.clone(), cache.clone()),
            manifests: manifest_batcher(client.clone(), cache.clone()),
            chunks: chunk_batcher(client.clone(), cache.clone()),
            tree_sizes: tree_sizes_batcher(client.clone(), cache.clone()),
            waits: Mutex::default(),
            outbox: {
                let outbox = Outbox::open(store_dir.join("outbox"), store_dir.join("quarantine"))?;
                cache.register_outbox(&dunce::canonicalize(store_dir.join("outbox"))?)?;
                outbox
            },
            cache,
            client,
            config,
            repo_dir: repo_dir.clone(),
        });
        registry.insert(repo_dir, Arc::downgrade(&shared));
        Ok(shared)
    }

    /// The reads of files' contents and manifests that waited on ok, since this repository was
    /// opened: those the cache and the outbox could not answer.
    pub fn waits(&self) -> crate::calls::WaitStats {
        self.waits.lock().unwrap().clone()
    }

    fn waited(&self, since: std::time::Instant) {
        self.waits.lock().unwrap().add(since.elapsed());
    }

    /// An object's canonical bytes: from the cache, the outbox, or the server (batched with
    /// concurrent reads). None if the server does not have it. Operations and views come
    /// from the caller's own operation log.
    pub async fn object(self: &Arc<Self>, key: Key) -> Result<Option<Vec<u8>>, Status> {
        if let Some(bytes) = self.cache.object(key) {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.outbox.object(key) {
            return Ok(Some(bytes));
        }
        let batcher = match key.kind {
            Kind::Operation | Kind::View => &self.operations,
            Kind::Commit => &self.commits,
            _ => &self.objects,
        };
        batcher.get(key).await
    }

    /// A file's manifest, with every chunk it names readable locally: from the cache, the
    /// outbox, or the server (streamed into the cache, batched with concurrent reads).
    pub async fn file(self: &Arc<Self>, id: Id) -> Result<Option<Manifest>, Status> {
        if let Some(m) = self.cache.file(&id) {
            return Ok(Some(m));
        }
        if let Some(m) = self.outbox.file(&id) {
            return Ok(Some(m));
        }
        let since = std::time::Instant::now();
        let got = self.files.get(id).await;
        self.waited(since);
        got
    }

    /// A chunk's bytes, from the outbox or the cache.
    pub fn chunk(&self, id: &Id) -> Option<Vec<u8>> {
        self.outbox.chunk(id).or_else(|| self.cache.chunk(id))
    }

    /// A file's manifest — its size and its chunks — whether or not its chunks are local: from
    /// the cache, the outbox, or the server's manifest-only read, batched with concurrent ones.
    pub async fn manifest(self: &Arc<Self>, id: Id) -> Result<Option<Manifest>, Status> {
        if let Some(m) = self.cache.manifest(&id) {
            return Ok(Some(m));
        }
        if let Some(m) = self.outbox.file(&id) {
            return Ok(Some(m));
        }
        let since = std::time::Instant::now();
        let got = self.manifests.get(id).await;
        self.waited(since);
        got
    }

    /// What a tree's entries weigh, in its order (docs/scm.md, "Tree sizes"): from the
    /// cache, or the SCM, batched with concurrent reads. None if the SCM does not hold the tree —
    /// one only this machine has written, not yet published: read its files' manifests instead.
    pub async fn tree_sizes(self: &Arc<Self>, tree: Id) -> Result<Option<TreeSizes>, Status> {
        if tree == ok_encoding::empty_tree_id() {
            return Ok(Some(TreeSizes::default()));
        }
        if let Some(s) = self.cache.tree_sizes(&tree) {
            return Ok(Some(s));
        }
        let since = std::time::Instant::now();
        let got = self.tree_sizes.get(tree).await;
        self.waited(since);
        got
    }

    /// Bytes [offset, offset + len) of a file (fewer at its end), reading from the server only
    /// the chunks that are not local, each verified against its ID — through the chunk batcher,
    /// so concurrent reads share a request and a chunk being fetched is not fetched again.
    pub async fn read_range(
        self: &Arc<Self>,
        id: Id,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, Status> {
        let manifest = self
            .manifest(id)
            .await?
            .ok_or_else(|| Status::not_found(format!("file {}", hex::encode(id))))?;
        let end = (offset + len).min(manifest.size);
        if offset >= end {
            return Ok(Vec::new());
        }
        // The chunks the range covers, with their offsets.
        let mut covering = Vec::new();
        let mut at = 0u64;
        for (cid, clen) in &manifest.chunks {
            let next = at + u64::from(*clen);
            if next > offset && at < end {
                covering.push((*cid, at, u64::from(*clen)));
            }
            at = next;
        }
        let mut have: HashMap<Id, Vec<u8>> = HashMap::new();
        let mut missing = Vec::new();
        for (cid, at, clen) in &covering {
            match self.chunk(cid) {
                Some(b) => {
                    have.insert(*cid, b);
                }
                None => missing.push((*cid, *at, *clen)),
            }
        }
        let since = std::time::Instant::now();
        let fetching = !missing.is_empty();
        let reads = missing.into_iter().map(|(chunk, at, clen)| {
            let read = ChunkRead {
                chunk,
                file: id,
                offset: at,
                len: clen,
            };
            async move { Ok::<_, Status>((chunk, self.chunks.get(read).await?)) }
        });
        for (chunk, data) in futures::future::try_join_all(reads).await? {
            if let Some(data) = data {
                have.insert(chunk, data);
            }
        }
        if fetching {
            self.waited(since);
        }
        let mut out = Vec::with_capacity((end - offset) as usize);
        for (cid, at, clen) in covering {
            let data = have.get(&cid).ok_or_else(|| {
                Status::data_loss(format!(
                    "the SCM did not send chunk {} of file {}",
                    hex::encode(cid),
                    hex::encode(id)
                ))
            })?;
            let from = offset.saturating_sub(at) as usize;
            let to = (end.min(at + clen) - at) as usize;
            out.extend_from_slice(&data[from..to]);
        }
        Ok(out)
    }
}

/// Decodes an `EncodedObject` into its key and canonical bytes, checking its ID.
pub fn from_wire(o: pb::EncodedObject) -> Result<(Key, Vec<u8>), Status> {
    let kind = kind_from_wire(o.kind)
        .ok_or_else(|| Status::data_loss(format!("object kind {}", o.kind)))?;
    let id = id_from(&o.id).ok_or_else(|| Status::data_loss("an object ID is not 32 bytes"))?;
    let mut canonical = Vec::with_capacity(o.body.len() + 1);
    canonical.push(kind as u8);
    canonical.extend_from_slice(&o.body);
    if canonical_id(kind, &canonical) != Some(id) {
        return Err(Status::data_loss(format!(
            "{} {} from the SCM does not hash to its ID",
            kind.name(),
            hex::encode(id)
        )));
    }
    Ok((Key::new(kind, id), canonical))
}

fn io_status(e: io::Error) -> Status {
    Status::internal(format!("local cache: {e}"))
}

/// The raw bytes of the chunk a frame carries, decompressed as its codec says: the SCM sends
/// chunks as their packs store them (`ReadFilesRequest.stored`, docs/scm.md, "Reads"). The
/// caller verifies them against the chunk's ID.
#[cfg(test)]
pub(crate) fn chunk_bytes(frame: pb::FileData, cache: &Cache) -> Result<Vec<u8>, Status> {
    stored_chunk(frame, cache).map(|(raw, _)| raw)
}

/// The dictionary a frame's chunk is compressed with, if it is: what must be cached before the
/// chunk can be decoded (`ensure_dictionary`).
pub(crate) fn frame_dictionary(frame: &pb::FileData) -> Option<u32> {
    (frame.codec == pb::ChunkCodec::ZstdDict as i32)
        .then(|| zstd_dictionary_id(&frame.data))
        .flatten()
}

/// The dictionary a zstd frame's header names (RFC 8878 §3.1.1.1): None if it names none.
pub(crate) fn zstd_dictionary_id(frame: &[u8]) -> Option<u32> {
    if frame.get(..4)? != [0x28, 0xb5, 0x2f, 0xfd] {
        return None;
    }
    let descriptor = *frame.get(4)?;
    let size = [0, 1, 2, 4][usize::from(descriptor & 3)];
    // A window descriptor follows unless the frame is a single segment.
    let at = if descriptor & 0x20 != 0 { 5 } else { 6 };
    let id = frame
        .get(at..at + size)?
        .iter()
        .enumerate()
        .fold(0u32, |id, (i, b)| id | u32::from(*b) << (8 * i));
    (id != 0).then_some(id)
}

/// Caches the repository's dictionary `id` unless it is: read from the SCM, checked to be one
/// that carries its ID. A dictionary is never wrong in a way that matters — what decodes with it
/// is checked against its ID — so this checks only that it is a dictionary.
pub(crate) async fn ensure_dictionary(
    client: &Client,
    cache: &Cache,
    id: u32,
) -> Result<(), Status> {
    if cache.has_dictionary(id) {
        return Ok(());
    }
    let d = client
        .store()
        .await?
        .read_dictionary(pb::ReadDictionaryRequest {
            repository_id: client.repository_id(),
            id,
        })
        .await?
        .into_inner();
    cache
        .put_dictionary(id, &d.body)
        .map_err(|e| Status::data_loss(format!("dictionary {id} from the SCM: {e}")))
}

/// A frame's chunk, decoded, with how it came — what the cache keeps. A chunk compressed with a
/// dictionary decodes with the cache's copy of it, which `ensure_dictionary` puts there first.
pub(crate) fn stored_chunk(
    frame: pb::FileData,
    cache: &Cache,
) -> Result<(Vec<u8>, Stored), Status> {
    use crate::segments::Codec;
    let pb::FileData {
        chunk_id,
        data,
        codec,
        raw_length,
        ..
    } = frame;
    let lost = |what: String| {
        Status::data_loss(format!(
            "chunk {} from the SCM: {what}",
            hex::encode(&chunk_id)
        ))
    };
    let n = raw_length as usize;
    // No chunk is larger than a message could carry raw.
    if n > crate::client::MAX_MESSAGE {
        return Err(lost(format!(
            "{n} bytes, over {}",
            crate::client::MAX_MESSAGE
        )));
    }
    let (raw, stored) = match pb::ChunkCodec::try_from(codec) {
        // An SCM that does not send chunks stored sends them raw, and names no codec.
        Ok(pb::ChunkCodec::Unspecified) => return Ok((data.clone(), Stored::raw(data))),
        Ok(pb::ChunkCodec::Raw) => (data.clone(), Stored::raw(data)),
        Ok(pb::ChunkCodec::Zstd) => {
            let mut raw = Vec::with_capacity(n);
            ruzstd::decoding::FrameDecoder::new()
                .decode_all_to_vec(&data, &mut raw)
                .map_err(|e| lost(format!("zstd: {e}")))?;
            let stored = Stored {
                codec: Codec::Zstd,
                dict: 0,
                data,
            };
            (raw, stored)
        }
        Ok(pb::ChunkCodec::ZstdDict) => {
            let dict = zstd_dictionary_id(&data)
                .ok_or_else(|| lost("a dictionary's frame names no dictionary".into()))?;
            let stored = Stored {
                codec: Codec::ZstdDict,
                dict,
                data,
            };
            let raw = cache
                .decode_stored(&stored, n)
                .ok_or_else(|| lost(format!("does not decode with dictionary {dict}")))?;
            (raw, stored)
        }
        Err(_) => return Err(lost(format!("unknown codec {codec}"))),
    };
    if raw.len() != n {
        return Err(lost(format!(
            "{} bytes, the SCM says {raw_length}",
            raw.len()
        )));
    }
    Ok((raw, stored))
}

/// Reads `keys` from the SCM into the cache (`ReadOperations` for operations and views,
/// `ReadObjects` otherwise), returning those it has.
async fn read_objects(
    client: &Client,
    cache: &Cache,
    keys: &[Key],
    operations: bool,
) -> Result<HashMap<Key, Vec<u8>>, Status> {
    let mut store = client.store().await?;
    let refs = keys
        .iter()
        .map(|k| pb::ObjectRef {
            kind: kind_to_wire(k.kind),
            id: k.id.to_vec(),
        })
        .collect();
    let repository_id = client.repository_id();
    let mut stream = if operations {
        store
            .read_operations(pb::ReadOperationsRequest {
                repository_id,
                refs,
                workspace: client.workspace(),
            })
            .await?
            .into_inner()
    } else {
        store
            .read_objects(pb::ReadObjectsRequest {
                repository_id,
                refs,
            })
            .await?
            .into_inner()
    };
    let mut found = HashMap::new();
    let mut puts = Puts::default();
    while let Some(result) = stream.message().await? {
        if let Some(pb::object_result::Result::Object(o)) = result.result {
            let (key, canonical) = from_wire(o)?;
            puts.object(key, canonical.clone());
            found.insert(key, canonical);
        }
    }
    cache.put_all(&puts).map_err(io_status)?;
    Ok(found)
}

fn object_batcher(client: Client, cache: Arc<Cache>) -> Arc<Batcher<Key, Vec<u8>>> {
    Batcher::new(
        "objects",
        OBJECTS_PER_READ,
        Box::new(move |keys: Vec<Key>| {
            let client = client.clone();
            let cache = cache.clone();
            Box::pin(async move { read_objects(&client, &cache, &keys, false).await })
        }),
    )
}

/// Operation and view misses. jj walks the operation log one operation at a time, each one's
/// view after it, so a missing operation comes with its uncached ancestors and every one's view
/// (`PrefetchOperations`), up to 1,000, naming the operation frontier as held. A prefetch that
/// ends before its limit leaves the wanted operations' whole ancestry cached: they join the
/// frontier. What it did not bring — a view alone, an ancestor eviction took — is read directly.
fn operation_batcher(client: Client, cache: Arc<Cache>) -> Arc<Batcher<Key, Vec<u8>>> {
    Batcher::new(
        "operations",
        OBJECTS_PER_READ,
        Box::new(move |keys: Vec<Key>| {
            let client = client.clone();
            let cache = cache.clone();
            Box::pin(async move {
                let want: Vec<Id> = keys
                    .iter()
                    .filter(|k| k.kind == Kind::Operation)
                    .map(|k| k.id)
                    .collect();
                let mut found = HashMap::new();
                if !want.is_empty() {
                    let limit = crate::prefetch::ANCESTORS_PER_MISS;
                    let (got, ops) = crate::prefetch::operations(
                        &client,
                        &cache,
                        &want,
                        &cache.op_frontier(),
                        limit,
                    )
                    .await?;
                    if (ops as i32) < limit {
                        cache.add_op_frontier(&want).map_err(io_status)?;
                    }
                    found = got;
                }
                let unreached: Vec<Key> = keys
                    .iter()
                    .copied()
                    .filter(|k| !found.contains_key(k))
                    .collect();
                if !unreached.is_empty() {
                    found.extend(read_objects(&client, &cache, &unreached, true).await?);
                }
                Ok(found)
            })
        }),
    )
}

/// Commit misses: each missing commit comes with up to 1,000 uncached ancestors
/// (`PrefetchCommits`). The prefetch's limit is shared by the whole batch and streams newest
/// first, so a requested commit it did not reach is read directly.
fn commit_batcher(client: Client, cache: Arc<Cache>) -> Arc<Batcher<Key, Vec<u8>>> {
    Batcher::new(
        "commits",
        OBJECTS_PER_READ,
        Box::new(move |keys: Vec<Key>| {
            let client = client.clone();
            let cache = cache.clone();
            Box::pin(async move {
                let want: Vec<Id> = keys.iter().map(|k| k.id).collect();
                let mut found = crate::prefetch::commits(
                    &client,
                    &cache,
                    &want,
                    &[],
                    false,
                    crate::prefetch::ANCESTORS_PER_MISS,
                )
                .await?;
                let unreached: Vec<Key> = keys
                    .iter()
                    .copied()
                    .filter(|k| !found.contains_key(k))
                    .collect();
                if !unreached.is_empty() {
                    found.extend(read_objects(&client, &cache, &unreached, false).await?);
                }
                Ok(found)
            })
        }),
    )
}

/// Chunks ranged reads miss: one `ReadFiles` per batch, a range per chunk, the chunks arriving
/// as stored, each verified against its ID and cached.
fn chunk_batcher(client: Client, cache: Arc<Cache>) -> Arc<Batcher<ChunkRead, Vec<u8>>> {
    Batcher::new(
        "chunks",
        FILES_PER_READ,
        Box::new(move |reads: Vec<ChunkRead>| {
            let client = client.clone();
            let cache = cache.clone();
            Box::pin(async move {
                let mut store = client.store().await?;
                let req = pb::ReadFilesRequest {
                    repository_id: client.repository_id(),
                    files: reads
                        .iter()
                        .map(|r| pb::FileRange {
                            file_id: r.file.to_vec(),
                            offset: r.offset,
                            length: r.len,
                        })
                        .collect(),
                    manifests_only: false,
                    stored: true,
                    dictionaries: true,
                };
                let wanted: HashMap<Id, ChunkRead> = reads.iter().map(|r| (r.chunk, *r)).collect();
                let mut found = HashMap::new();
                let mut puts = Puts::default();
                let mut stream = store.read_files(req).await?.into_inner();
                while let Some(frame) = stream.message().await? {
                    if frame.not_found || frame.data.is_empty() {
                        continue;
                    }
                    let cid = id_from(&frame.chunk_id)
                        .ok_or_else(|| Status::data_loss("a chunk ID is not 32 bytes"))?;
                    if let Some(dict) = frame_dictionary(&frame) {
                        ensure_dictionary(&client, &cache, dict).await?;
                    }
                    let (data, stored) = stored_chunk(frame, &cache)?;
                    if chunk_id(&data) != cid {
                        return Err(Status::data_loss(format!(
                            "chunk {} from the SCM does not hash to its ID",
                            hex::encode(cid)
                        )));
                    }
                    if let Some(read) = wanted.get(&cid) {
                        puts.chunk(cid, &data, stored);
                        found.insert(*read, data);
                    }
                }
                cache.put_all(&puts).map_err(io_status)?;
                Ok(found)
            })
        }),
    )
}

/// Manifests read without contents (`manifests_only`): each file's size and chunks, cached for
/// ranged reads.
fn tree_sizes_batcher(client: Client, cache: Arc<Cache>) -> Arc<Batcher<Id, TreeSizes>> {
    Batcher::new(
        "tree_sizes",
        TREES_PER_SIZES_READ,
        Box::new(move |ids: Vec<Id>| {
            let client = client.clone();
            let cache = cache.clone();
            Box::pin(async move {
                let mut store = client.store().await?;
                let req = pb::ReadTreeSizesRequest {
                    repository_id: client.repository_id(),
                    tree_ids: ids.iter().map(|id| id.to_vec()).collect(),
                };
                let mut stream = store.read_tree_sizes(req).await?.into_inner();
                let mut found = HashMap::new();
                let mut puts = Puts::default();
                while let Some(m) = stream.message().await? {
                    let id = id_from(&m.tree_id)
                        .ok_or_else(|| Status::data_loss("a tree ID is not 32 bytes"))?;
                    let sizes = ok_encoding::decode_tree_sizes(&m.record).map_err(|e| {
                        Status::data_loss(format!("tree {}'s sizes: {e}", hex::encode(id)))
                    })?;
                    puts.sizes(id, m.record);
                    found.insert(id, sizes);
                }
                cache.put_all(&puts).map_err(io_status)?;
                Ok(found)
            })
        }),
    )
}

fn manifest_batcher(client: Client, cache: Arc<Cache>) -> Arc<Batcher<Id, Manifest>> {
    Batcher::new(
        "manifests",
        MANIFESTS_PER_READ,
        Box::new(move |ids: Vec<Id>| {
            let client = client.clone();
            let cache = cache.clone();
            Box::pin(async move {
                let mut store = client.store().await?;
                let req = pb::ReadFilesRequest {
                    repository_id: client.repository_id(),
                    files: ids
                        .iter()
                        .map(|id| pb::FileRange {
                            file_id: id.to_vec(),
                            offset: 0,
                            length: 0,
                        })
                        .collect(),
                    manifests_only: true,
                    stored: false,
                    dictionaries: false,
                };
                let mut stream = store.read_files(req).await?.into_inner();
                let mut found = HashMap::new();
                let mut puts = Puts::default();
                let mut current: Option<(Manifest, Vec<u64>)> = None;
                while let Some(frame) = stream.message().await? {
                    let id = id_from(&frame.file_id)
                        .ok_or_else(|| Status::data_loss("a file ID is not 32 bytes"))?;
                    if frame.not_found {
                        continue;
                    }
                    let (manifest, offsets) = current.get_or_insert_with(|| {
                        (
                            Manifest {
                                id,
                                size: frame.size,
                                chunks: Vec::new(),
                                path_hint: String::new(),
                            },
                            Vec::new(),
                        )
                    });
                    if manifest.id != id {
                        return Err(Status::data_loss(
                            "the SCM interleaved two files' manifests",
                        ));
                    }
                    if !frame.chunk_id.is_empty() {
                        let cid = id_from(&frame.chunk_id)
                            .ok_or_else(|| Status::data_loss("a chunk ID is not 32 bytes"))?;
                        manifest.chunks.push((cid, 0));
                        offsets.push(frame.offset);
                    }
                    if frame.last {
                        let (mut manifest, offsets) = current.take().unwrap();
                        // Each chunk's length: to the next chunk's offset, the last to the end.
                        for i in 0..manifest.chunks.len() {
                            let next = offsets.get(i + 1).copied().unwrap_or(manifest.size);
                            let len = next
                                .checked_sub(offsets[i])
                                .filter(|l| *l > 0 && *l <= u64::from(u32::MAX));
                            manifest.chunks[i].1 = len.ok_or_else(|| {
                                Status::data_loss(format!(
                                    "the SCM sent file {}'s chunks out of order",
                                    hex::encode(id)
                                ))
                            })? as u32;
                        }
                        if offsets.first().is_some_and(|o| *o != 0) {
                            return Err(Status::data_loss(format!(
                                "file {}'s manifest does not start at its first byte",
                                hex::encode(id)
                            )));
                        }
                        puts.manifest(manifest.clone());
                        found.insert(manifest.id, manifest);
                    }
                }
                cache.put_all(&puts).map_err(io_status)?;
                Ok(found)
            })
        }),
    )
}

fn file_batcher(client: Client, cache: Arc<Cache>) -> Arc<Batcher<Id, Manifest>> {
    Batcher::new(
        "files",
        FILES_PER_READ,
        Box::new(move |ids: Vec<Id>| {
            let client = client.clone();
            let cache = cache.clone();
            Box::pin(async move {
                let mut store = client.store().await?;
                let req = pb::ReadFilesRequest {
                    manifests_only: false,
                    stored: true,
                    dictionaries: true,
                    repository_id: client.repository_id(),
                    files: ids
                        .iter()
                        .map(|id| pb::FileRange {
                            file_id: id.to_vec(),
                            offset: 0,
                            length: 0,
                        })
                        .collect(),
                };
                let mut stream = store.read_files(req).await?.into_inner();
                let mut found = HashMap::new();
                let mut puts = Puts::default();
                // The file being received: its manifest, its hash so far, and its chunks as sent.
                let mut current: Option<(Manifest, Hasher)> = None;
                let mut chunks: Vec<(Id, Vec<u8>, Stored)> = Vec::new();
                while let Some(frame) = stream.message().await? {
                    let id = id_from(&frame.file_id)
                        .ok_or_else(|| Status::data_loss("a file ID is not 32 bytes"))?;
                    if frame.not_found {
                        continue;
                    }
                    let (manifest, hasher) = current.get_or_insert_with(|| {
                        (
                            Manifest {
                                id,
                                size: frame.size,
                                chunks: Vec::new(),
                                path_hint: String::new(),
                            },
                            Hasher::new(Kind::File),
                        )
                    });
                    if manifest.id != id {
                        return Err(Status::data_loss("the SCM interleaved two files' chunks"));
                    }
                    // A chunk is never empty: a frame without data carries none (the empty
                    // file's one frame).
                    let last = frame.last;
                    if !frame.data.is_empty() {
                        let cid = id_from(&frame.chunk_id)
                            .ok_or_else(|| Status::data_loss("a chunk ID is not 32 bytes"))?;
                        if let Some(dict) = frame_dictionary(&frame) {
                            ensure_dictionary(&client, &cache, dict).await?;
                        }
                        let (data, stored) = stored_chunk(frame, &cache)?;
                        if chunk_id(&data) != cid {
                            return Err(Status::data_loss(format!(
                                "chunk {} from the SCM does not hash to its ID",
                                hex::encode(cid)
                            )));
                        }
                        hasher.update(&data);
                        manifest.chunks.push((cid, data.len() as u32));
                        chunks.push((cid, data, stored));
                    }
                    if last {
                        let (manifest, hasher) = current.take().unwrap();
                        let total: u64 = manifest.chunks.iter().map(|c| u64::from(c.1)).sum();
                        if hasher.finalize() != manifest.id || total != manifest.size {
                            return Err(Status::data_loss(format!(
                                "file {} from the SCM does not hash to its ID",
                                hex::encode(manifest.id)
                            )));
                        }
                        // A file of one chunk is kept whole; a larger one as its chunks and manifest.
                        if chunks.len() == 1 {
                            let (_, raw, stored) = chunks.pop().unwrap();
                            puts.whole(manifest.id, &raw, stored);
                        } else {
                            for (cid, raw, stored) in chunks.drain(..) {
                                puts.chunk(cid, &raw, stored);
                            }
                            puts.manifest(manifest.clone());
                        }
                        found.insert(manifest.id, manifest);
                        if puts.bytes() > 8 << 20 {
                            cache
                                .put_all(&std::mem::take(&mut puts))
                                .map_err(io_status)?;
                        }
                    }
                }
                cache.put_all(&puts).map_err(io_status)?;
                if let Some((manifest, _)) = current {
                    return Err(Status::data_loss(format!(
                        "the SCM ended file {} before its last chunk",
                        hex::encode(manifest.id)
                    )));
                }
                Ok(found)
            })
        }),
    )
}
