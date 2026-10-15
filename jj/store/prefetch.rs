//! Prefetch: streaming what jj is about to read into the cache in a few calls instead of one
//! round trip per object.
//!
//! - `jj clone` streams the whole operation log — operations, views, and the commits they
//!   reference (`operation_log`) — and the working-copy tree with its files (`tree`) before jj
//!   builds its index and checks out.
//! - A cache miss on a commit fetches it with up to 1,000 of its ancestors
//!   (`Shared::object`, through the commit batcher), and one on an operation fetches it with up
//!   to 1,000 of its uncached ancestors and their views (the operation batcher).

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use futures::StreamExt as _;
use futures::TryStreamExt as _;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::object_id::ObjectId as _;
use ok_encoding::Kind;
use ok_encoding::Object;
use ok_proto::ok::scm::v1 as pb;
use tonic::Status;

use crate::cache::Cache;
use crate::cache::Puts;
use crate::client::Client;
use crate::ids::Id;
use crate::ids::Key;
use crate::ids::id_from;
use crate::ids::id_of;
use crate::shared;
use crate::shared::Shared;
use crate::shared::from_wire;

/// How many uncached ancestors a commit miss brings along.
pub const ANCESTORS_PER_MISS: i32 = 1000;

/// How many files are fetched concurrently while prefetching a tree: enough to fill every batch
/// the store keeps in flight. Fewer form one small batch at a time, a round trip each — a real
/// clone of 30,000 small files took 470 of them, one after another.
const CONCURRENT_FILES: usize = crate::shared::FILES_PER_READ * crate::batch::MAX_IN_FLIGHT;

/// Streams commits into the cache: those reachable from `want` (or, with `op_log`, from
/// everything the caller's operation log references) and not from `have`, at most `limit`
/// (0: all). Returns the commits streamed.
pub async fn commits(
    client: &Client,
    cache: &Cache,
    want: &[Id],
    have: &[Id],
    op_log: bool,
    limit: i32,
) -> Result<HashMap<Key, Vec<u8>>, Status> {
    let mut store = client.store().await?;
    let mut stream = store
        .prefetch_commits(pb::PrefetchCommitsRequest {
            repository_id: client.repository_id(),
            want: want.iter().map(|w| w.to_vec()).collect(),
            have: have.iter().map(|h| h.to_vec()).collect(),
            op_log,
            limit,
            workspace: client.workspace(),
        })
        .await?
        .into_inner();
    let mut out = HashMap::new();
    let mut puts = Puts::default();
    while let Some(o) = stream.message().await? {
        let (key, canonical) = from_wire(o)?;
        if key.kind != Kind::Commit {
            return Err(Status::data_loss(
                "the SCM prefetched something other than a commit",
            ));
        }
        puts.object(key, canonical.clone());
        if puts.bytes() > FLUSH_BYTES {
            flush(cache, &mut puts)?;
        }
        out.insert(key, canonical);
    }
    flush(cache, &mut puts)?;
    Ok(out)
}

/// Streams operations into the cache, each with its view: those of the caller's log reachable
/// from `want` (its heads when empty) and not from `have`, at most `limit` (0: all). Returns
/// what it streamed and how many operations.
pub async fn operations(
    client: &Client,
    cache: &Cache,
    want: &[Id],
    have: &[Id],
    limit: i32,
) -> Result<(HashMap<Key, Vec<u8>>, usize), Status> {
    let mut store = client.store().await?;
    let mut stream = store
        .prefetch_operations(pb::PrefetchOperationsRequest {
            repository_id: client.repository_id(),
            want: want.iter().map(|w| w.to_vec()).collect(),
            have: have.iter().map(|h| h.to_vec()).collect(),
            limit,
            workspace: client.workspace(),
        })
        .await?
        .into_inner();
    let mut out = HashMap::new();
    let mut puts = Puts::default();
    let mut ops = 0;
    while let Some(o) = stream.message().await? {
        let (key, canonical) = from_wire(o)?;
        match key.kind {
            Kind::Operation => ops += 1,
            Kind::View => {}
            _ => {
                return Err(Status::data_loss(
                    "the SCM prefetched something other than an operation or a view",
                ));
            }
        }
        puts.object(key, canonical.clone());
        if puts.bytes() > FLUSH_BYTES {
            flush(cache, &mut puts)?;
        }
        out.insert(key, canonical);
    }
    flush(cache, &mut puts)?;
    Ok((out, ops))
}

/// How much a stream gathers before it writes to the cache: a write is one append.
const FLUSH_BYTES: usize = 4 << 20;

fn flush(cache: &Cache, puts: &mut Puts) -> Result<(), Status> {
    cache
        .put_all(&std::mem::take(puts))
        .map_err(|e| Status::internal(format!("local cache: {e}")))
}

/// Streams the caller's whole operation log — its operations and views, then the commits they
/// reference — into the cache of the repository whose store directory is `store_path`: what jj
/// reads to load the repository and build its index, in a few calls rather than one per
/// operation. The heads then head the operation frontier. Returns the commits streamed.
pub async fn operation_log(store_path: &Path) -> Result<usize, Status> {
    let shared = Shared::for_store_path(store_path).map_err(|e| Status::internal(e.to_string()))?;
    shared::run(async move {
        let heads: Vec<Id> = {
            let mut store = shared.client.store().await?;
            store
                .get_op_heads(pb::GetOpHeadsRequest {
                    repository_id: shared.client.repository_id(),
                    workspace: shared.client.workspace(),
                })
                .await?
                .into_inner()
                .op_ids
                .iter()
                .filter_map(|h| <Id>::try_from(h.as_slice()).ok())
                // The root operation is virtual: the server holds no such operation.
                .filter(|h| *h != crate::ids::ROOT_ID)
                .collect()
        };
        let (logged, commits) = futures::future::try_join(
            operations(&shared.client, &shared.cache, &heads, &[], 0),
            commits(&shared.client, &shared.cache, &[], &[], true, 0),
        )
        .await?;
        if logged.1 > 0 {
            shared
                .cache
                .add_op_frontier(&heads)
                .map_err(|e| Status::internal(format!("local cache: {e}")))?;
        }
        Ok(commits.len())
    })
    .await
}

/// Streams a tree's whole closure — every tree, symlink, copy history, and file under it —
/// into the cache of the repository whose store directory is `store_path`.
pub async fn tree(store_path: &Path, tree_id: &TreeId) -> Result<(), Status> {
    let shared = Shared::for_store_path(store_path).map_err(|e| Status::internal(e.to_string()))?;
    let Some(root) = id_of(tree_id) else {
        return Err(Status::invalid_argument("a tree ID is not 32 bytes"));
    };
    if root == ok_encoding::empty_tree_id() {
        return Ok(());
    }
    shared::run(async move { tree_closure(shared, root, true).await }).await
}

/// Streams a tree's closure without its files' contents — every tree, symlink, and copy history
/// under it — into the cache: what a virtual workspace lists and looks up, so that both are
/// local soon after a clone (docs/okfs.md, "Metadata").
pub async fn trees(shared: Arc<Shared>, tree_id: &TreeId) -> Result<(), Status> {
    let Some(root) = id_of(tree_id) else {
        return Err(Status::invalid_argument("a tree ID is not 32 bytes"));
    };
    if root == ok_encoding::empty_tree_id() {
        return Ok(());
    }
    shared::run(async move { tree_closure(shared, root, false).await }).await
}

/// Which repository-relative paths a read-ahead wants.
pub type Wanted = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// What a read-ahead fetched.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadAhead {
    pub files: u64,
    pub bytes: u64,
}

/// Has a tree, every file's size under it, and the files whose repository-relative paths `wanted`
/// accepts, local before they are read (a presentation's read-ahead, docs/okfs.md, "ok's
/// services"): what reads a build graph walks every directory, and a listing shows each file's
/// size. The trees come as one closure when the root is not cached (a repository's first), and
/// otherwise only those not cached, a level at a time: a later commit shares most of its trees
/// with an earlier one. The sizes come as each tree's sizes record, one per directory; as
/// manifests only for a tree the SCM does not hold. Returns the wanted files it fetched.
pub async fn tree_and_files(
    shared: Arc<Shared>,
    tree_id: &TreeId,
    wanted: Wanted,
) -> Result<ReadAhead, Status> {
    let Some(root) = id_of(tree_id) else {
        return Err(Status::invalid_argument("a tree ID is not 32 bytes"));
    };
    if root == ok_encoding::empty_tree_id() {
        return Ok(ReadAhead::default());
    }
    shared::run(async move {
        if shared.cache.object(Key::new(Kind::Tree, root)).is_none() {
            tree_closure(shared.clone(), root, false).await?;
        }
        let mut files: HashMap<Id, ()> = HashMap::new();
        // Each tree, and its files whose sizes are not otherwise local: its record answers them.
        let mut trees: Vec<(Id, Vec<Id>)> = Vec::new();
        let mut level = vec![(String::new(), root)];
        while !level.is_empty() {
            let read: Vec<(String, Id, Option<Vec<u8>>)> = futures::stream::iter(level)
                .map(|(dir, id)| {
                    let shared = shared.clone();
                    async move {
                        let bytes = shared.object(Key::new(Kind::Tree, id)).await?;
                        Ok::<_, Status>((dir, id, bytes))
                    }
                })
                .buffer_unordered(CONCURRENT_TREES)
                .try_collect()
                .await?;
            let mut next = Vec::new();
            for (dir, tree_id, bytes) in read {
                let mut unsized_files = Vec::new();
                let bytes = bytes
                    .ok_or_else(|| Status::not_found(format!("the SCM has no tree for {dir:?}")))?;
                let Ok(Object::Tree(tree)) = ok_encoding::decode(&bytes) else {
                    return Err(Status::data_loss(format!("{dir:?} is not a tree")));
                };
                for entry in tree.entries() {
                    let name = entry.name().as_internal_str();
                    let path = if dir.is_empty() {
                        name.to_owned()
                    } else {
                        format!("{dir}/{name}")
                    };
                    match entry.value() {
                        TreeValue::Tree(id) => {
                            if let Ok(id) = <Id>::try_from(id.as_bytes()) {
                                next.push((path, id));
                            }
                        }
                        TreeValue::File { id, .. } => {
                            let Ok(id) = <Id>::try_from(id.as_bytes()) else {
                                continue;
                            };
                            if wanted(&path) && !shared.cache.has_file(&id) {
                                files.insert(id, ());
                            } else if !shared.cache.knows_size(&id) {
                                unsized_files.push(id);
                            }
                        }
                        _ => {}
                    }
                }
                trees.push((tree_id, unsized_files));
            }
            level = next;
        }
        let unheld: Vec<Vec<Id>> = futures::stream::iter(trees)
            .map(|(tree, files)| {
                let shared = shared.clone();
                async move {
                    if files.is_empty() || shared.tree_sizes(tree).await?.is_some() {
                        return Ok::<_, Status>(Vec::new());
                    }
                    Ok(files)
                }
            })
            .buffer_unordered(CONCURRENT_TREES)
            .try_collect()
            .await?;
        let sizes: HashSet<Id> = unheld.into_iter().flatten().collect();
        futures::stream::iter(sizes)
            .map(|id| {
                let shared = shared.clone();
                async move { shared.manifest(id).await.map(|_| ()) }
            })
            .buffer_unordered(CONCURRENT_MANIFESTS)
            .try_collect::<Vec<()>>()
            .await?;
        let sizes: Vec<u64> = futures::stream::iter(files.into_keys())
            .map(|id| {
                let shared = shared.clone();
                async move { Ok::<_, Status>(shared.file(id).await?.map_or(0, |m| m.size)) }
            })
            .buffer_unordered(CONCURRENT_FILES)
            .try_collect()
            .await?;
        Ok(ReadAhead {
            files: sizes.len() as u64,
            bytes: sizes.iter().sum(),
        })
    })
    .await
}

/// How many trees a read-ahead asks for at once (the store batches them into few requests).
const CONCURRENT_TREES: usize = 512;

/// How many sizes a read-ahead asks for at once: enough to fill the store's largest manifest
/// batches.
const CONCURRENT_MANIFESTS: usize = 4 * crate::shared::MANIFESTS_PER_READ;

/// Streams a tree's closure into the cache: with its files' contents (a real checkout), or with
/// each tree's sizes instead (a virtual one, which stats files without reading them).
async fn tree_closure(shared: Arc<Shared>, root: Id, with_files: bool) -> Result<(), Status> {
    let mut store = shared.client.store().await?;
    let mut stream = store
        .read_tree_closure(pb::ReadTreeClosureRequest {
            repository_id: shared.client.repository_id(),
            tree_id: root.to_vec(),
            path_prefixes: Vec::new(),
            with_sizes: !with_files,
        })
        .await?
        .into_inner();
    let mut files: HashSet<Id> = HashSet::new();
    let mut puts = Puts::default();
    // A level's trees wait for their sizes, which follow them, so each is kept with its record.
    let mut waiting: HashMap<Id, Vec<u8>> = HashMap::new();
    while let Some(o) = stream.message().await? {
        if o.kind == pb::ObjectKind::TreeSizes as i32 {
            let tree =
                id_from(&o.id).ok_or_else(|| Status::data_loss("a tree ID is not 32 bytes"))?;
            ok_encoding::decode_tree_sizes(&o.body).map_err(|e| {
                Status::data_loss(format!("tree {}'s sizes: {e}", hex::encode(tree)))
            })?;
            match waiting.remove(&tree) {
                Some(canonical) => puts.tree_with_sizes(tree, canonical, o.body),
                None => puts.sizes(tree, o.body),
            }
            continue;
        }
        let (key, canonical) = from_wire(o)?;
        if let Ok(Object::Tree(tree)) = ok_encoding::decode(&canonical) {
            for entry in tree.entries() {
                if let TreeValue::File { id, .. } = entry.value()
                    && let Ok(id) = <Id>::try_from(id.as_bytes())
                {
                    files.insert(id);
                }
            }
        }
        if key.kind == Kind::Tree && !with_files {
            waiting.insert(key.id, canonical);
        } else {
            // A level's leaves come after its trees' sizes: what still waits has none.
            for (id, canonical) in waiting.drain() {
                puts.object(Key::new(Kind::Tree, id), canonical);
            }
            puts.object(key, canonical);
        }
        if puts.bytes() > FLUSH_BYTES {
            flush(&shared.cache, &mut puts)?;
        }
    }
    for (id, canonical) in waiting.drain() {
        puts.object(Key::new(Kind::Tree, id), canonical);
    }
    flush(&shared.cache, &mut puts)?;
    if !with_files {
        return Ok(());
    }
    futures::stream::iter(files)
        .map(|id| {
            let shared = shared.clone();
            async move { shared.file(id).await.map(|_| ()) }
        })
        .buffer_unordered(CONCURRENT_FILES)
        .try_collect::<Vec<()>>()
        .await?;
    Ok(())
}
