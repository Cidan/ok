//! The commit graph (docs/scm.md, "The commit graph"): jj's index of a repository's commits built
//! from what the SCM sends of each — its ID, change ID, generation, committer's time, tree, and
//! parents — not from every commit's body, which jj reads only for the commits a command looks at.
//! A clone builds it so, before jj loads the repository; the index is jj's own, written by jj's
//! default index store, so jj finds it there and nothing else about it differs. An operation made
//! since jj indexes as it makes it, reading only the commits that operation adds.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::io::Read as _;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt as _;
use jj_lib::backend;
use jj_lib::backend::ChangeId;
use jj_lib::backend::CommitId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::Signature;
use jj_lib::backend::Timestamp;
use jj_lib::backend::TreeId;
use jj_lib::commit::Commit;
use jj_lib::default_index::DefaultIndexStore;
use jj_lib::index::IndexStore as _;
use jj_lib::index::MutableIndex;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_walk;
use jj_lib::operation::Operation;
use jj_lib::repo::RepoLoader;
use jj_lib::store::Store;
use ok_encoding::GraphRecord;
use ok_proto::ok::scm::v1 as pb;
use tonic::Status;

use crate::ids::Id;
use crate::ids::id_from;
use crate::shared;
use crate::shared::Shared;

/// What a main graph layer is: its magic and version before its records' zstd frame
/// (internal/scm/graph).
const LAYER_MAGIC: &[u8] = b"OKGRAPHL";
const LAYER_VERSION: u8 = 1;

/// The most a layer's records decompress to, as the SCM bounds them.
const LAYER_MAX_RECORDS: u64 = 8 << 30;

/// How many haves a read names: the indexed operation's commits, which reach everything below.
const MAX_HAVES: usize = 10_000;

/// How many of main's layers an index store remembers having indexed.
const HELD_MAX: usize = 64;

/// The file beside jj's own, in the index's directory, naming the layers of main's graph this index
/// took in: the next read names them as held.
const HELD_FILE: &str = "ok_graph_layers";

/// What a read of the graph brought: the layers of main the SCM named, and every record, layers'
/// first, in topological order.
pub struct GraphRead {
    pub layers: Vec<Id>,
    pub records: Vec<GraphRecord>,
}

/// Reads what an index of the commits reachable from `heads` needs, the index holding everything
/// reachable from `have` and the layers of main's graph `held` names.
pub async fn read_graph(
    shared: Arc<Shared>,
    heads: Vec<Id>,
    have: Vec<Id>,
    held: Vec<Id>,
) -> Result<GraphRead, Status> {
    shared::run(async move {
        let mut store = shared.client.store().await?;
        let mut stream = store
            .read_graph(pb::ReadGraphRequest {
                repository_id: shared.client.repository_id(),
                heads: heads.iter().map(|h| h.to_vec()).collect(),
                have: have.iter().map(|h| h.to_vec()).collect(),
                held_layers: held.iter().map(|h| h.to_vec()).collect(),
            })
            .await?
            .into_inner();
        let mut named: Option<Vec<Id>> = None;
        let mut layers: HashMap<Id, Vec<GraphRecord>> = HashMap::new();
        let mut pieces: HashMap<Id, Vec<u8>> = HashMap::new();
        let mut loose = Vec::new();
        while let Some(f) = stream.message().await? {
            match f.frame {
                Some(pb::graph_frame::Frame::Header(h)) => {
                    let ids: Option<Vec<Id>> = h.layers.iter().map(|l| id_from(l)).collect();
                    named =
                        Some(ids.ok_or_else(|| Status::data_loss("a layer ID is not 32 bytes"))?);
                }
                Some(pb::graph_frame::Frame::Layer(l)) => {
                    let id = id_from(&l.layer_id)
                        .ok_or_else(|| Status::data_loss("a layer ID is not 32 bytes"))?;
                    pieces.entry(id).or_default().extend_from_slice(&l.data);
                    if !l.last {
                        continue;
                    }
                    let b = pieces.remove(&id).unwrap_or_default();
                    if ok_encoding::hash(&b) != id {
                        return Err(Status::data_loss(format!(
                            "graph layer {} does not hash to its ID",
                            hex::encode(id)
                        )));
                    }
                    layers.insert(id, decode_layer(&b)?);
                }
                Some(pb::graph_frame::Frame::Records(r)) => {
                    loose.extend(
                        ok_encoding::decode_graph_records(&r.data).map_err(Status::data_loss)?,
                    );
                }
                None => {}
            }
        }
        if !pieces.is_empty() {
            return Err(Status::data_loss(
                "the SCM ended a graph layer before its last piece",
            ));
        }
        let named =
            named.ok_or_else(|| Status::data_loss("the SCM sent a graph without its header"))?;
        let mut records = Vec::new();
        for id in &named {
            if let Some(recs) = layers.remove(id) {
                records.extend(recs);
            } else if !held.contains(id) {
                return Err(Status::data_loss(format!(
                    "the SCM named graph layer {}, which it neither sent nor was held",
                    hex::encode(id)
                )));
            }
        }
        records.extend(loose);
        Ok(GraphRead {
            layers: named,
            records,
        })
    })
    .await
}

fn decode_layer(b: &[u8]) -> Result<Vec<GraphRecord>, Status> {
    let lost = |what: String| Status::data_loss(format!("a graph layer: {what}"));
    let Some(rest) = b.strip_prefix(LAYER_MAGIC) else {
        return Err(lost("not a graph layer".into()));
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
        return Err(lost("its records are over its bound".into()));
    }
    ok_encoding::decode_graph_records(&raw).map_err(lost)
}

/// Indexes the repository's operation heads from the commit graph, each that jj's default index
/// store has no index of: what a clone does before jj loads the repository at them. A repository
/// not on ok, or another index store, is left to jj. Should the graph not do — the SCM unreachable,
/// or one that serves no graph — jj indexes them as it loads them, reading their commits.
pub async fn index_heads(
    loader: &RepoLoader,
    repo_dir: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(backend) = loader.store().backend_impl::<crate::OkBackend>() else {
        return Ok(());
    };
    let Some(inner) = loader.index_store().downcast_ref::<DefaultIndexStore>() else {
        return Ok(());
    };
    let index = GraphIndex {
        inner,
        dir: repo_dir.join("index"),
    };
    for id in loader.op_heads_store().get_op_heads().await? {
        let op = loader.load_operation(&id).await?;
        if !index.indexed(&op) {
            index
                .build(&op, loader.store(), backend.shared().clone())
                .await?;
        }
    }
    Ok(())
}

/// jj's default index store, and its directory: an index built from the commit graph is written
/// through it, as jj writes its own.
struct GraphIndex<'a> {
    inner: &'a DefaultIndexStore,
    dir: PathBuf,
}

impl GraphIndex<'_> {
    fn indexed(&self, op: &Operation) -> bool {
        self.dir.join("op_links").join(op.id().hex()).is_file()
    }

    fn held(&self) -> Vec<Id> {
        let Ok(b) = fs::read(self.dir.join(HELD_FILE)) else {
            return Vec::new();
        };
        b.as_chunks::<32>().0.to_vec()
    }

    fn add_held(&self, layers: &[Id]) -> std::io::Result<()> {
        let mut out: Vec<Id> = Vec::with_capacity(HELD_MAX);
        for id in layers.iter().chain(self.held().iter()) {
            if out.len() == HELD_MAX {
                break;
            }
            if !out.contains(id) {
                out.push(*id);
            }
        }
        let mut tmp = tempfile::NamedTempFile::new_in(&self.dir)?;
        tmp.write_all(&out.concat())?;
        tmp.persist(self.dir.join(HELD_FILE))
            .map(|_| ())
            .map_err(|e| e.error)
    }

    /// Builds the index at `op` from the commit graph: from its nearest indexed ancestor's, with
    /// every commit the operations since reference and their ancestors added — from the graph's
    /// records, then, for what the SCM does not hold (what this machine has not published yet),
    /// from the commits themselves.
    async fn build(
        &self,
        op: &Operation,
        store: &Arc<Store>,
        shared: Arc<Shared>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut unindexed = Vec::new();
        let mut parent_op = None;
        let mut ancestors = std::pin::pin!(op_walk::walk_ancestors(std::slice::from_ref(op)));
        while let Some(o) = ancestors.next().await {
            let o = o?;
            if self.indexed(&o) {
                parent_op = Some(o);
                break;
            }
            unindexed.push(o);
        }
        let parent_op = parent_op.ok_or("no ancestor operation is indexed")?;
        let parent = self.inner.get_index_at_op(&parent_op, store).await?;
        let mut index = parent.start_modification();
        let mut heads: Vec<CommitId> = Vec::new();
        let mut seen = HashSet::new();
        for o in &unindexed {
            let view = o.view().await?;
            for id in o
                .all_referenced_commit_ids()
                .chain(view.all_referenced_commit_ids())
            {
                if seen.insert(id.clone()) && !index.as_index().has_id(id).await? {
                    heads.push(id.clone());
                }
            }
        }
        if !heads.is_empty() {
            let root = store.root_commit_id().clone();
            let mut have: Vec<Id> = Vec::new();
            let parent_view = parent_op.view().await?;
            for id in parent_view.all_referenced_commit_ids() {
                if *id != root
                    && have.len() < MAX_HAVES
                    && let Ok(h) = <Id>::try_from(id.as_bytes())
                    && !have.contains(&h)
                {
                    have.push(h);
                }
            }
            let wanted: Vec<Id> = heads
                .iter()
                .filter_map(|h| <Id>::try_from(h.as_bytes()).ok())
                .collect();
            let mut held = self.held();
            loop {
                let read =
                    read_graph(shared.clone(), wanted.clone(), have.clone(), held.clone()).await?;
                add_records(index.as_mut(), store, &read.records).await?;
                let mut missing = false;
                for h in &heads {
                    missing = missing || !index.as_index().has_id(h).await?;
                }
                if !missing || held.is_empty() {
                    if !read.layers.is_empty() {
                        self.add_held(&read.layers)?;
                    }
                    break;
                }
                // A layer named as held is not in the index built on: asked again, all are sent.
                held.clear();
            }
            add_commits(index.as_mut(), store, &heads).await?;
        }
        self.inner.write_index(index, op)?;
        Ok(())
    }
}

/// Adds records to the index in their order, each whose parents it holds and it does not.
async fn add_records(
    index: &mut dyn MutableIndex,
    store: &Arc<Store>,
    records: &[GraphRecord],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    'records: for r in records {
        let id = CommitId::new(r.id.to_vec());
        if index.as_index().has_id(&id).await? {
            continue;
        }
        for p in &r.parents {
            if !index.as_index().has_id(&CommitId::new(p.to_vec())).await? {
                continue 'records;
            }
        }
        index.add_commit(&commit_of(store, id, r)).await?;
    }
    Ok(())
}

/// The commit a record describes, as jj's index reads it: its ID, change ID, parents, tree, and
/// committer's time — never its description or author, which a command reads from the commit itself.
fn commit_of(store: &Arc<Store>, id: CommitId, r: &GraphRecord) -> Commit {
    let terms = r.tree.len();
    let signature = Signature {
        name: String::new(),
        email: String::new(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(r.committed_millis),
            tz_offset: r.tz_offset,
        },
    };
    let data = backend::Commit {
        parents: r
            .parents
            .iter()
            .map(|p| CommitId::new(p.to_vec()))
            .collect(),
        predecessors: Vec::new(),
        root_tree: Merge::from_vec(
            r.tree
                .iter()
                .map(|t| TreeId::new(t.to_vec()))
                .collect::<Vec<_>>(),
        ),
        conflict_labels: if terms == 1 {
            Merge::resolved(String::new())
        } else {
            Merge::from_vec(vec![String::new(); terms])
        },
        change_id: ChangeId::new(r.change_id.clone()),
        description: String::new(),
        author: signature.clone(),
        committer: signature,
        secure_sig: None,
    };
    Commit::new(store.clone(), id, Arc::new(data))
}

/// Adds `heads` and their ancestors the index lacks, read as commits, parents first: what the
/// graph did not hold.
async fn add_commits(
    index: &mut dyn MutableIndex,
    store: &Arc<Store>,
    heads: &[CommitId],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut order: Vec<Commit> = Vec::new();
    let mut visited: HashSet<CommitId> = HashSet::new();
    // Depth first, each commit after its parents: (commit, whether its parents are done).
    let mut stack: Vec<(CommitId, bool)> = heads.iter().rev().map(|h| (h.clone(), false)).collect();
    let mut loaded: HashMap<CommitId, Commit> = HashMap::new();
    while let Some((id, expanded)) = stack.pop() {
        if expanded {
            order.push(loaded.remove(&id).expect("loaded"));
            continue;
        }
        if visited.contains(&id) || index.as_index().has_id(&id).await? {
            continue;
        }
        visited.insert(id.clone());
        let commit = store.get_commit_async(&id).await?;
        stack.push((id.clone(), true));
        for p in commit.parent_ids().iter().rev() {
            if !visited.contains(p) {
                stack.push((p.clone(), false));
            }
        }
        loaded.insert(id, commit);
    }
    for c in &order {
        index.add_commit(c).await?;
    }
    Ok(())
}
