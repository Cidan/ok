//! Publishing the outbox (docs/scm.md, "Cache, outbox, and prefetch").
//!
//! A publish carries the whole outbox — the transaction's objects and anything an earlier
//! command staged — and, last, swaps the operation heads. When the outbox is large it goes
//! as several publishes: files first, then objects in dependency order (each publish's
//! references already stored by an earlier one), with only the last one moving the heads.
//! Nothing is visible until the heads move, so a transaction still lands atomically.

use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use ok_encoding::Kind;
use ok_proto::ok::scm::v1 as pb;
use prost::Message as _;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::Status;

use crate::cache::Manifest;
use crate::client;
use crate::ids::Id;
use crate::ids::Key;
use crate::ids::id_from;
use crate::ids::kind_from_wire;
use crate::ids::kind_to_wire;
use crate::outbox::Staged;
use crate::refs::references;
use crate::shared::Shared;

/// A publish's Begin stays under this, well inside the server's message limit.
const BATCH_BYTES: usize = 3 << 20;

/// An operation-heads swap: remove `old`, add `new`.
pub struct Heads {
    pub old: Vec<Id>,
    pub new: Id,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("the SCM refused the publish: {0}")]
    Rpc(Status),
    #[error(
        "operation {op} can never be published: {why}. It was moved to {path}; run the command \
         again, and jj reloads from the SCM's operation heads"
    )]
    Quarantined {
        op: String,
        why: String,
        path: String,
    },
}

impl From<Status> for PublishError {
    fn from(s: Status) -> Self {
        Self::Rpc(s)
    }
}

/// Publishes everything staged and, if `heads` is set, swaps the caller's operation heads;
/// returns the heads after the publish. Runs on the store runtime; the caller holds the
/// outbox lock.
pub async fn publish(shared: Arc<Shared>, heads: Option<Heads>) -> Result<Vec<Id>, PublishError> {
    let staged = set_aside_oversized(
        &shared,
        shared.outbox.list()?,
        heads.as_ref().map(|h| h.new),
    )?;
    let batches = plan(&shared, &staged);
    let last = batches.len() - 1;
    let mut heads = heads;
    let mut result = Vec::new();
    for (i, batch) in batches.iter().enumerate() {
        let swap = if i == last { heads.take() } else { None };
        result = publish_batch(&shared, batch, swap).await?;
    }
    shared.outbox.retire(&staged, &shared.cache)?;
    Ok(result)
}

/// Sets aside what the SCM can never accept from what is staged: every file larger than it holds
/// (one an older jj staged; this one refuses them as they are written), and every object that
/// reaches one — the trees, commits, views, and operations of a snapshot that failed for it. They
/// are quarantined, and what is left publishes. An operation being recorded that reaches one is
/// refused, saying which file.
pub(crate) fn set_aside_oversized(
    shared: &Shared,
    mut staged: Staged,
    recording: Option<Id>,
) -> Result<Staged, PublishError> {
    let oversized: Vec<Manifest> = staged
        .files
        .iter()
        .filter(|m| m.size > crate::chunk::MAX_FILE)
        .cloned()
        .collect();
    if oversized.is_empty() {
        return Ok(staged);
    }
    let mut bad: HashSet<Key> = oversized
        .iter()
        .map(|m| Key::new(Kind::File, m.id))
        .collect();
    let refs: Vec<(Key, Vec<Key>)> = staged
        .objects
        .iter()
        .map(|k| {
            (
                *k,
                shared
                    .outbox
                    .object(*k)
                    .map(|b| references(&b))
                    .unwrap_or_default(),
            )
        })
        .collect();
    loop {
        let before = bad.len();
        for (k, r) in &refs {
            if !bad.contains(k) && r.iter().any(|x| bad.contains(x)) {
                bad.insert(*k);
            }
        }
        if bad.len() == before {
            break;
        }
    }
    if let Some(op) = recording
        && bad.contains(&Key::new(Kind::Operation, op))
    {
        let m = &oversized[0];
        return Err(PublishError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} is {} bytes, larger than the 50 MiB ok holds: remove it, or leave it untracked",
                m.path_hint, m.size
            ),
        )));
    }
    for m in &oversized {
        shared.outbox.quarantine_file(m)?;
    }
    for k in staged.objects.iter().filter(|k| bad.contains(k)) {
        shared.outbox.quarantine(*k)?;
    }
    staged.files.retain(|m| m.size <= crate::chunk::MAX_FILE);
    staged.objects.retain(|k| !bad.contains(k));
    Ok(staged)
}

#[derive(Default)]
struct Batch {
    files: Vec<Manifest>,
    objects: Vec<Key>,
}

/// Orders the staged files and objects into publishes: files, then objects with every
/// staged reference before its referrer, split by size. Always at least one publish.
fn plan(shared: &Shared, staged: &Staged) -> Vec<Batch> {
    let mut batches = vec![Batch::default()];
    let mut size = 0usize;
    let next = |batches: &mut Vec<Batch>, size: &mut usize, cost: usize| {
        if *size + cost > BATCH_BYTES && *size > 0 {
            batches.push(Batch::default());
            *size = 0;
        }
        *size += cost;
    };
    for m in &staged.files {
        next(
            &mut batches,
            &mut size,
            64 + m.path_hint.len() + 40 * m.chunks.len(),
        );
        batches.last_mut().unwrap().files.push(m.clone());
    }
    for key in dependency_order(shared, &staged.objects) {
        next(&mut batches, &mut size, 40);
        batches.last_mut().unwrap().objects.push(key);
    }
    batches
}

/// `keys` with each one's references (among `keys`) before it: an iterative post-order,
/// so a long commit chain cannot overflow the stack.
pub(crate) fn dependency_order(shared: &Shared, keys: &[Key]) -> Vec<Key> {
    let staged: HashSet<Key> = keys.iter().copied().collect();
    let deps = |k: Key| -> Vec<Key> {
        shared
            .outbox
            .object(k)
            .map(|b| references(&b))
            .unwrap_or_default()
            .into_iter()
            .filter(|r| staged.contains(r))
            .collect()
    };
    let mut order = Vec::with_capacity(keys.len());
    let mut done: HashSet<Key> = HashSet::new();
    for &root in keys {
        if done.contains(&root) {
            continue;
        }
        let mut stack = vec![(root, false)];
        while let Some((k, expanded)) = stack.pop() {
            if done.contains(&k) {
                continue;
            }
            if expanded {
                done.insert(k);
                order.push(k);
                continue;
            }
            stack.push((k, true));
            for d in deps(k) {
                if !done.contains(&d) {
                    stack.push((d, false));
                }
            }
        }
    }
    order
}

/// Publishes one batch, retrying once if the server refuses it for references the client
/// can re-send from its cache, and quarantining an operation it can never accept.
async fn publish_batch(
    shared: &Arc<Shared>,
    batch: &Batch,
    heads: Option<Heads>,
) -> Result<Vec<Id>, PublishError> {
    let heads_wire = heads.map(|h| pb::OpHeadsUpdate {
        old_ids: h.old.iter().map(|i| i.to_vec()).collect(),
        new_id: h.new.to_vec(),
    });
    let first = client::while_frozen(|| {
        publish_once(shared, &batch.objects, &batch.files, heads_wire.clone())
    })
    .await;
    let missing = match first {
        Ok(heads) => return Ok(heads),
        Err(status) => match rejection(&status) {
            Some(missing) => missing,
            None => return Err(status.into()),
        },
    };
    // Re-send what the server lacks from the cache, once. A chunk or file it lacks was
    // swept by garbage collection during the exchange: the retry's exchange asks for it
    // again, and the batch's files hold it.
    let mut missing = missing;
    let resendable: Vec<Key> = missing
        .iter()
        .copied()
        .filter(|k| shared.cache.object(*k).is_some())
        .collect();
    let restage = missing
        .iter()
        .any(|k| matches!(k.kind, Kind::Chunk | Kind::File));
    if !resendable.is_empty() || restage {
        let mut objects = resendable;
        objects.extend(batch.objects.iter().copied());
        match publish_once(shared, &objects, &batch.files, heads_wire).await {
            Ok(heads) => return Ok(heads),
            Err(status) => match rejection(&status) {
                Some(still) => missing = still,
                None => return Err(status.into()),
            },
        }
    }
    // Still refused. An operation whose parent operation was collected (a `jj op abandon`
    // elsewhere) can never be published: quarantine it so it never blocks another command.
    let lost: HashSet<Key> = missing.into_iter().collect();
    for key in batch.objects.iter().filter(|k| k.kind == Kind::Operation) {
        let refs = shared
            .outbox
            .object(*key)
            .map(|b| references(&b))
            .unwrap_or_default();
        if let Some(gone) = refs.iter().find(|r| lost.contains(r)) {
            let path = shared.outbox.quarantine(*key)?;
            return Err(PublishError::Quarantined {
                op: key.hex(),
                why: format!(
                    "the SCM no longer has its {} {}",
                    gone.kind.name(),
                    gone.hex()
                ),
                path: path.display().to_string(),
            });
        }
    }
    let mut named: Vec<String> = lost
        .iter()
        .map(|k| format!("{} {}", k.kind.name(), k.hex()))
        .collect();
    named.sort();
    Err(PublishError::Rpc(Status::failed_precondition(format!(
        "the SCM lacks referenced objects the client cannot re-send: {}",
        named.join(", ")
    ))))
}

/// The references a FAILED_PRECONDITION publish names as missing, if it is a rejection.
pub(crate) fn rejection(status: &Status) -> Option<Vec<Key>> {
    if status.code() != Code::FailedPrecondition {
        return None;
    }
    let rpc = RpcStatus::decode(status.details()).ok()?;
    let any = rpc
        .details
        .into_iter()
        .find(|a| a.type_url.ends_with("/ok.scm.v1.PublishRejection"))?;
    let rej = pb::PublishRejection::decode(any.value.as_slice()).ok()?;
    Some(
        rej.missing
            .iter()
            .filter_map(|r| Some(Key::new(kind_from_wire(r.kind)?, id_from(&r.id)?)))
            .collect(),
    )
}

/// `google.rpc.Status`, the encoding of a status's details.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<prost_types::Any>,
}

async fn publish_once(
    shared: &Arc<Shared>,
    objects: &[Key],
    files: &[Manifest],
    heads: Option<pb::OpHeadsUpdate>,
) -> Result<Vec<Id>, Status> {
    use pb::publish_request::Step;
    let mut store = shared.client.store().await?;
    let (tx, rx) = mpsc::channel(64);
    let begin = pb::publish_request::Begin {
        repository_id: shared.client.repository_id(),
        workspace: shared.client.workspace(),
        // OkBackend::write_file refuses to stage a secret's value (docs/secrets.md).
        guards_secrets: true,
        objects: objects
            .iter()
            .map(|k| pb::ObjectRef {
                kind: kind_to_wire(k.kind),
                id: k.id.to_vec(),
            })
            .collect(),
        files: files
            .iter()
            .map(|m| pb::FileManifest {
                file_id: m.id.to_vec(),
                size: m.size,
                chunks: m
                    .chunks
                    .iter()
                    .map(|(id, len)| pb::ChunkRef {
                        id: id.to_vec(),
                        length: *len,
                    })
                    .collect(),
                path_hint: m.path_hint.clone(),
            })
            .collect(),
    };
    let send = |tx: &mpsc::Sender<pb::PublishRequest>, step: Step| {
        let tx = tx.clone();
        async move {
            tx.send(pb::PublishRequest { step: Some(step) })
                .await
                .map_err(|_| Status::aborted("the publish stream closed"))
        }
    };
    send(&tx, Step::Begin(begin)).await?;
    let mut responses = store.publish(ReceiverStream::new(rx)).await?.into_inner();
    let missing = match responses.message().await? {
        Some(pb::PublishResponse {
            step: Some(pb::publish_response::Step::Missing(m)),
        }) => m,
        _ => {
            return Err(Status::internal(
                "the SCM did not answer the publish's begin",
            ));
        }
    };
    for r in &missing.objects {
        let key = Key::new(
            kind_from_wire(r.kind)
                .ok_or_else(|| Status::internal("the SCM asked for an unknown kind"))?,
            id_from(&r.id).ok_or_else(|| Status::internal("the SCM asked for a malformed ID"))?,
        );
        let canonical = shared
            .outbox
            .object(key)
            .or_else(|| shared.cache.object(key))
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "{} {} is no longer available locally",
                    key.kind.name(),
                    key.hex()
                ))
            })?;
        send(
            &tx,
            Step::Object(pb::EncodedObject {
                kind: r.kind,
                id: r.id.clone(),
                body: canonical[1..].to_vec(),
            }),
        )
        .await?;
    }
    for c in &missing.chunks {
        let id =
            id_from(c).ok_or_else(|| Status::internal("the SCM asked for a malformed chunk ID"))?;
        let data = shared.chunk(&id).ok_or_else(|| {
            Status::failed_precondition(format!(
                "chunk {} is no longer available locally",
                hex::encode(id)
            ))
        })?;
        send(
            &tx,
            Step::Chunk(pb::ChunkData {
                id: c.clone(),
                data,
            }),
        )
        .await?;
    }
    send(
        &tx,
        Step::Commit(pb::publish_request::Commit { op_heads: heads }),
    )
    .await?;
    drop(tx);
    match responses.message().await? {
        Some(pb::PublishResponse {
            step: Some(pb::publish_response::Step::Done(done)),
        }) => Ok(done.op_heads.iter().filter_map(|h| id_from(h)).collect()),
        _ => Err(Status::internal("the SCM did not finish the publish")),
    }
}
