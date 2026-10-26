//! Checkouts and moves (docs/scm.md, "Checkouts", "Deltas"): a commit's checkout read as layers in
//! one request, and a working copy's move from one tree to another reading only what changed — the
//! trees the old one does not hold at the same path, and each changed file as a delta against the
//! file it replaces, where the SCM finds that smaller.

use std::collections::HashMap;
use std::io::Read as _;
use std::sync::Arc;

use futures::StreamExt as _;
use futures::TryStreamExt as _;
use jj_lib::backend::TreeValue;
use jj_lib::object_id::ObjectId as _;
use ok_encoding::Hasher;
use ok_encoding::KIND_TREE_SIZES;
use ok_encoding::Kind;
use ok_encoding::Object;
use ok_proto::ok::scm::v1 as pb;
use tonic::Status;

use crate::cache::Manifest;
use crate::cache::Puts;
use crate::cache::Stored;
use crate::cache::chunk_id;
use crate::cache::file_id;
use crate::chunk::SINGLE_CHUNK;
use crate::ids::Id;
use crate::ids::Key;
use crate::ids::id_from;
use crate::prefetch::ReadAhead;
use crate::shared;
use crate::shared::Shared;
use crate::shared::ensure_dictionary;
use crate::shared::frame_dictionary;
use crate::shared::stored_chunk;

/// What a layer is: its magic and version before its records' zstd frame
/// (internal/scm/layer).
const LAYER_MAGIC: &[u8] = b"OKCHKLAY";
const LAYER_VERSION: u8 = 1;

/// The most a layer's records decompress to, as the SCM bounds them.
const LAYER_MAX_RECORDS: u64 = 1 << 30;

/// The largest file sent as a delta, and the largest base named: the SCM's `DeltaMaxBytes`.
pub const DELTA_MAX_BYTES: usize = 4 << 20;

/// How many files' reads a move keeps in flight, each a `ReadFiles` of up to `FILES_PER_READ`.
const MOVE_READS_IN_FLIGHT: usize = 4;

/// How much a stream gathers before it writes to the cache.
const FLUSH_BYTES: usize = 4 << 20;

fn io_status(e: std::io::Error) -> Status {
    Status::internal(format!("local cache: {e}"))
}

/// Has a commit's checkout local: the layers of it the cache did not take in before — one request,
/// after the first usually one small layer — each record taken in as if it had been read: trees
/// with their sizes records, symlinks, copy histories, and the files planning reads, whole. Moving
/// from a tree whose checkout the cache took in (`have`), it is one layer of what the commit changes
/// from that tree (`ReadCheckoutRequest.have_tree`); a record eviction took since is read as any
/// miss is. Returns the commit's tree.
pub async fn checkout_layers(
    shared: Arc<Shared>,
    commit: &[u8],
    have: Option<Id>,
) -> Result<Id, Status> {
    let commit = commit.to_vec();
    shared::run(async move {
        let held = shared.cache.checkout_layers();
        let mut store = shared.client.store().await?;
        let mut stream = store
            .read_checkout(pb::ReadCheckoutRequest {
                repository_id: shared.client.repository_id(),
                commit_id: commit,
                held_layers: held.iter().map(|h| h.to_vec()).collect(),
                have_tree: have.map(|h| h.to_vec()).unwrap_or_default(),
            })
            .await?
            .into_inner();
        let mut tree = None;
        let mut pieces: HashMap<Id, Vec<u8>> = HashMap::new();
        let mut taken = Vec::new();
        while let Some(f) = stream.message().await? {
            match f.frame {
                Some(pb::checkout_frame::Frame::Header(h)) => {
                    tree = Some(
                        id_from(&h.tree_id)
                            .ok_or_else(|| Status::data_loss("a tree ID is not 32 bytes"))?,
                    );
                }
                Some(pb::checkout_frame::Frame::Layer(l)) => {
                    let id = id_from(&l.layer_id)
                        .ok_or_else(|| Status::data_loss("a layer ID is not 32 bytes"))?;
                    let b = pieces.entry(id).or_default();
                    b.extend_from_slice(&l.data);
                    if !l.last {
                        continue;
                    }
                    let b = pieces.remove(&id).unwrap_or_default();
                    if ok_encoding::hash(&b) != id {
                        return Err(Status::data_loss(format!(
                            "checkout layer {} does not hash to its ID",
                            hex::encode(id)
                        )));
                    }
                    take_layer(&shared, &b)?;
                    taken.push(id);
                }
                None => {}
            }
        }
        if !pieces.is_empty() {
            return Err(Status::data_loss(
                "the SCM ended a checkout layer before its last piece",
            ));
        }
        let tree =
            tree.ok_or_else(|| Status::data_loss("the SCM sent a checkout without its header"))?;
        if !taken.is_empty() {
            shared
                .cache
                .add_checkout_layers(&taken)
                .map_err(io_status)?;
        }
        Ok(tree)
    })
    .await
}

/// Takes a layer's records into the cache.
fn take_layer(shared: &Shared, b: &[u8]) -> Result<(), Status> {
    let lost = |what: String| Status::data_loss(format!("a checkout layer: {what}"));
    let Some(rest) = b.strip_prefix(LAYER_MAGIC) else {
        return Err(lost("not a checkout layer".into()));
    };
    if rest.first() != Some(&LAYER_VERSION) {
        return Err(lost(format!("version {:?}", rest.first())));
    }
    let mut raw = Vec::new();
    ruzstd::decoding::StreamingDecoder::new(&rest[1..])
        .map_err(|e| lost(format!("zstd: {e}")))?
        .take(LAYER_MAX_RECORDS + 1)
        .read_to_end(&mut raw)
        .map_err(|e| lost(format!("zstd: {e}")))?;
    if raw.len() as u64 > LAYER_MAX_RECORDS {
        return Err(lost("its records are over a GiB".into()));
    }
    let records = ok_encoding::decode_layer_records(&raw).map_err(lost)?;
    let mut sizes: HashMap<[u8; 32], Vec<u8>> = HashMap::new();
    let mut rest = Vec::with_capacity(records.len());
    for r in records {
        if r.kind == KIND_TREE_SIZES {
            sizes.insert(r.id, r.body);
        } else {
            rest.push(r);
        }
    }
    let mut puts = Puts::default();
    for r in rest {
        let mut canonical = Vec::with_capacity(r.body.len() + 1);
        canonical.push(r.kind);
        canonical.extend_from_slice(&r.body);
        match r.kind {
            k if k == Kind::Tree as u8 => match sizes.remove(&r.id) {
                Some(s) => puts.tree_with_sizes(r.id, canonical, s),
                None => puts.object(Key::new(Kind::Tree, r.id), canonical),
            },
            k if k == Kind::Symlink as u8 => puts.object(Key::new(Kind::Symlink, r.id), canonical),
            k if k == Kind::CopyHistory as u8 => {
                puts.object(Key::new(Kind::CopyHistory, r.id), canonical)
            }
            k if k == Kind::File as u8 => put_file(&mut puts, r.id, &r.body)?,
            _ => {}
        }
        if puts.bytes() > FLUSH_BYTES {
            shared
                .cache
                .put_all(&std::mem::take(&mut puts))
                .map_err(io_status)?;
        }
    }
    for (tree, s) in sizes {
        puts.sizes(tree, s);
    }
    shared.cache.put_all(&puts).map_err(io_status)
}

/// Caches a file's whole contents as a read of it would: a file of one chunk whole, a larger one as
/// its chunks — cut as the client cuts every file — and its manifest, the empty file as a manifest
/// alone.
fn put_file(puts: &mut Puts, id: Id, data: &[u8]) -> Result<(), Status> {
    if file_id(data) != id {
        return Err(Status::data_loss(format!(
            "file {} does not hash to its ID",
            hex::encode(id)
        )));
    }
    if data.is_empty() {
        puts.manifest(Manifest {
            id,
            size: 0,
            chunks: Vec::new(),
            path_hint: String::new(),
        });
        return Ok(());
    }
    if data.len() <= SINGLE_CHUNK {
        puts.whole_raw(id, data);
        return Ok(());
    }
    let mut chunks = Vec::new();
    for c in fastcdc::v2020::FastCDC::new(
        data,
        crate::chunk::MIN,
        crate::chunk::AVG,
        crate::chunk::MAX,
    ) {
        let bytes = &data[c.offset..c.offset + c.length];
        let cid = chunk_id(bytes);
        puts.chunk_raw(cid, bytes);
        chunks.push((cid, c.length as u32));
    }
    puts.manifest(Manifest {
        id,
        size: data.len() as u64,
        chunks,
        path_hint: String::new(),
    });
    Ok(())
}

/// Reads files into the cache, each with a base — the file it replaces, at its path — which, held
/// locally, the SCM may send it as a delta against. A batch of `FILES_PER_READ` files to a request,
/// a few in flight. Returns how many files and bytes it read.
pub async fn files_with_bases(
    shared: Arc<Shared>,
    reads: Vec<(Id, Option<Id>)>,
) -> Result<ReadAhead, Status> {
    let batches: Vec<Vec<(Id, Option<Id>)>> = reads
        .chunks(crate::shared::FILES_PER_READ)
        .map(|c| c.to_vec())
        .collect();
    let got: Vec<ReadAhead> = futures::stream::iter(batches)
        .map(|batch| {
            let shared = shared.clone();
            async move { read_batch(&shared, batch).await }
        })
        .buffer_unordered(MOVE_READS_IN_FLIGHT)
        .try_collect()
        .await?;
    Ok(got.iter().fold(ReadAhead::default(), |a, b| ReadAhead {
        files: a.files + b.files,
        bytes: a.bytes + b.bytes,
        deltas: a.deltas + b.deltas,
    }))
}

async fn read_batch(
    shared: &Arc<Shared>,
    batch: Vec<(Id, Option<Id>)>,
) -> Result<ReadAhead, Status> {
    let mut bases: HashMap<Id, Vec<u8>> = HashMap::new();
    let mut files = Vec::with_capacity(batch.len());
    for (id, base) in &batch {
        let mut fr = pb::FileRange {
            file_id: id.to_vec(),
            offset: 0,
            length: 0,
            base_file_id: Vec::new(),
        };
        if let Some(b) = base
            && let Some(bytes) = bases.get(b).cloned().or_else(|| shared.local_bytes(b))
            && bytes.len() <= DELTA_MAX_BYTES
        {
            fr.base_file_id = b.to_vec();
            bases.insert(*b, bytes);
        }
        files.push(fr);
    }
    let mut store = shared.client.store().await?;
    let req = pb::ReadFilesRequest {
        repository_id: shared.client.repository_id(),
        files,
        manifests_only: false,
        stored: true,
        dictionaries: true,
        deltas: true,
    };
    let mut stream = store.read_files(req).await?.into_inner();
    let mut out = ReadAhead::default();
    let mut puts = Puts::default();
    // The file being received as chunks: its manifest, its hash so far, and its chunks as sent.
    let mut current: Option<(Manifest, Hasher)> = None;
    let mut chunks: Vec<(Id, Vec<u8>, Stored)> = Vec::new();
    while let Some(frame) = stream.message().await? {
        let id = id_from(&frame.file_id)
            .ok_or_else(|| Status::data_loss("a file ID is not 32 bytes"))?;
        if frame.not_found {
            return Err(Status::not_found(format!(
                "the SCM does not have file {}",
                hex::encode(id)
            )));
        }
        if frame.codec == pb::ChunkCodec::Delta as i32 {
            if current.is_some() {
                return Err(Status::data_loss(
                    "the SCM sent a delta inside another file's chunks",
                ));
            }
            let data = apply(&frame, &bases)?;
            out.files += 1;
            out.deltas += 1;
            out.bytes += data.len() as u64;
            put_file(&mut puts, id, &data)?;
        } else {
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
            let last = frame.last;
            if !frame.data.is_empty() {
                let cid = id_from(&frame.chunk_id)
                    .ok_or_else(|| Status::data_loss("a chunk ID is not 32 bytes"))?;
                if let Some(dict) = frame_dictionary(&frame) {
                    ensure_dictionary(&shared.client, &shared.cache, dict).await?;
                }
                let (data, stored) = stored_chunk(frame, &shared.cache)?;
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
                let (manifest, hasher) = current.take().expect("a file being received");
                let total: u64 = manifest.chunks.iter().map(|c| u64::from(c.1)).sum();
                if hasher.finalize() != manifest.id || total != manifest.size {
                    return Err(Status::data_loss(format!(
                        "file {} from the SCM does not hash to its ID",
                        hex::encode(manifest.id)
                    )));
                }
                out.files += 1;
                out.bytes += manifest.size;
                if chunks.len() == 1 {
                    let (_, raw, stored) = chunks.pop().expect("one chunk");
                    puts.whole(manifest.id, &raw, stored);
                } else {
                    for (cid, raw, stored) in chunks.drain(..) {
                        puts.chunk(cid, &raw, stored);
                    }
                    puts.manifest(manifest);
                }
            }
        }
        if puts.bytes() > FLUSH_BYTES {
            shared
                .cache
                .put_all(&std::mem::take(&mut puts))
                .map_err(io_status)?;
        }
    }
    if let Some((manifest, _)) = current {
        return Err(Status::data_loss(format!(
            "the SCM ended file {} before its last chunk",
            hex::encode(manifest.id)
        )));
    }
    shared.cache.put_all(&puts).map_err(io_status)?;
    Ok(out)
}

/// The file a delta frame makes from the base it names, checked against the file's ID and size.
fn apply(frame: &pb::FileData, bases: &HashMap<Id, Vec<u8>>) -> Result<Vec<u8>, Status> {
    let lost = |what: String| {
        Status::data_loss(format!(
            "file {}'s delta: {what}",
            hex::encode(&frame.file_id)
        ))
    };
    let base =
        id_from(&frame.base_file_id).ok_or_else(|| lost("its base's ID is not 32 bytes".into()))?;
    let base = bases
        .get(&base)
        .ok_or_else(|| lost("against a base not asked for".into()))?;
    let n = frame.raw_length as usize;
    if n > DELTA_MAX_BYTES || frame.size != frame.raw_length as u64 {
        return Err(lost(format!("{n} bytes, of a file of {}", frame.size)));
    }
    // A delta's instructions are at most a few times what they make.
    let mut delta = Vec::new();
    let bound = (3 * n + 1024) as u64;
    ruzstd::decoding::StreamingDecoder::new(&frame.data[..])
        .map_err(|e| lost(format!("zstd: {e}")))?
        .take(bound + 1)
        .read_to_end(&mut delta)
        .map_err(|e| lost(format!("zstd: {e}")))?;
    if delta.len() as u64 > bound {
        return Err(lost(
            "its instructions are larger than what they make".into(),
        ));
    }
    let data = ok_encoding::apply_delta(base, &delta, n as u64).map_err(lost)?;
    if file_id(&data) != id_from(&frame.file_id).expect("checked") {
        return Err(lost("what it makes does not hash to the file's ID".into()));
    }
    Ok(data)
}

/// Has local what a working copy's move from `old` to `new` reads: `new`'s trees that `old` does
/// not hold at the same path (one closure, with `old` as its base, when `new` is not cached), then
/// every file under `new` the cache lacks, each with the file at its path in `old` as a base.
/// Returns the files it read.
pub async fn move_tree(
    shared: Arc<Shared>,
    old: &jj_lib::backend::TreeId,
    new: &jj_lib::backend::TreeId,
) -> Result<ReadAhead, Status> {
    let (Ok(old), Ok(new)) = (
        <Id>::try_from(old.as_bytes()),
        <Id>::try_from(new.as_bytes()),
    ) else {
        return Err(Status::invalid_argument("a tree ID is not 32 bytes"));
    };
    if old == new || new == ok_encoding::empty_tree_id() {
        return Ok(ReadAhead::default());
    }
    shared::run(async move {
        if shared.cache.object(Key::new(Kind::Tree, new)).is_none() {
            let base = (old != ok_encoding::empty_tree_id()
                && shared.cache.object(Key::new(Kind::Tree, old)).is_some())
            .then_some(old);
            crate::prefetch::tree_closure(shared.clone(), new, base, false).await?;
        }
        let empty = ok_encoding::empty_tree_id();
        let mut reads: Vec<(Id, Option<Id>)> = Vec::new();
        let mut seen: std::collections::HashSet<Id> = std::collections::HashSet::new();
        let mut level = vec![(new, (old != empty).then_some(old))];
        while !level.is_empty() {
            let read: Vec<(Vec<u8>, Option<Vec<u8>>)> = futures::stream::iter(level)
                .map(|(n, o)| {
                    let shared = shared.clone();
                    async move {
                        let nb =
                            shared
                                .object(Key::new(Kind::Tree, n))
                                .await?
                                .ok_or_else(|| {
                                    Status::not_found(format!(
                                        "the SCM has no tree {}",
                                        hex::encode(n)
                                    ))
                                })?;
                        let ob = match o {
                            Some(o) => shared.object(Key::new(Kind::Tree, o)).await?,
                            None => None,
                        };
                        Ok::<_, Status>((nb, ob))
                    }
                })
                .buffer_unordered(512)
                .try_collect()
                .await?;
            let mut next = Vec::new();
            for (nb, ob) in read {
                let Ok(Object::Tree(nt)) = ok_encoding::decode(&nb) else {
                    return Err(Status::data_loss("a tree does not decode"));
                };
                let mut was: HashMap<String, TreeValue> = HashMap::new();
                if let Some(ob) = ob
                    && let Ok(Object::Tree(ot)) = ok_encoding::decode(&ob)
                {
                    for e in ot.entries() {
                        was.insert(e.name().as_internal_str().to_owned(), e.value().clone());
                    }
                }
                for e in nt.entries() {
                    let old = was.get(e.name().as_internal_str());
                    match e.value() {
                        TreeValue::Tree(id) => {
                            let Ok(id) = <Id>::try_from(id.as_bytes()) else {
                                continue;
                            };
                            let base = match old {
                                Some(TreeValue::Tree(o)) => {
                                    if o.as_bytes() == id.as_slice() {
                                        continue;
                                    }
                                    <Id>::try_from(o.as_bytes()).ok()
                                }
                                _ => None,
                            };
                            if id != empty {
                                next.push((id, base));
                            }
                        }
                        TreeValue::File { id, .. } => {
                            let Ok(id) = <Id>::try_from(id.as_bytes()) else {
                                continue;
                            };
                            if shared.cache.has_file(&id)
                                || shared.outbox.file(&id).is_some()
                                || !seen.insert(id)
                            {
                                continue;
                            }
                            let base = match old {
                                Some(TreeValue::File { id: o, .. }) => {
                                    <Id>::try_from(o.as_bytes()).ok()
                                }
                                _ => None,
                            };
                            reads.push((id, base));
                        }
                        _ => {}
                    }
                }
            }
            level = next;
        }
        files_with_bases(shared, reads).await
    })
    .await
}
